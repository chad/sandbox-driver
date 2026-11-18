//! An in-process boxd for unit tests: the token exchange over HTTP and the
//! subset of `BoxdApi` the provider calls over gRPC, with machines held in
//! memory. Every call is recorded by method name.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::{Stream, stream};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::api::*;
use crate::api_server::boxd_api_server::{BoxdApi, BoxdApiServer};

type Stream_<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// How the fake misbehaves, when a test asks it to.
#[derive(Clone, Copy, Default)]
pub(crate) struct Faults {
    /// `SetVmEgressAllow` refuses its entries.
    pub(crate) refuse_egress: bool,
    /// `DestroyVm` fails, leaving the machine in place.
    pub(crate) fail_destroy:  bool,
}

#[derive(Default)]
struct State {
    machines: HashMap<String, GetVmResponse>,
    calls:    Vec<&'static str>,
    created:  u32,
}

#[derive(Clone)]
struct Service {
    state:  Arc<Mutex<State>>,
    faults: Faults,
}

pub(crate) struct FakeBoxd {
    state: Arc<Mutex<State>>,
    grpc:  SocketAddr,
    auth:  SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for FakeBoxd {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl FakeBoxd {
    pub(crate) async fn start(faults: Faults) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let service = Service {
            state: Arc::clone(&state),
            faults,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind gRPC");
        let grpc = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(BoxdApiServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("fake boxd serves");
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind auth");
        let auth = listener.local_addr().expect("address");
        let exchange = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = vec![0; 4096];
                let read = socket.read(&mut request).await.unwrap_or(0);
                let body = if String::from_utf8_lossy(&request[..read]).contains("bxd_good") {
                    (
                        "200 OK",
                        r#"{"token":"jwt","expires_at":4102444800,"user_id":"u"}"#,
                    )
                } else {
                    ("401 Unauthorized", r#"{"error":"bad key"}"#)
                };
                let response = format!(
                    "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.0,
                    body.1.len(),
                    body.1
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        Self {
            state,
            grpc,
            auth,
            tasks: vec![server, exchange],
        }
    }

    pub(crate) fn api_url(&self) -> String {
        format!("http://{}", self.grpc)
    }

    pub(crate) fn auth_url(&self) -> String {
        format!("http://{}/api/v1/auth/token", self.auth)
    }

    pub(crate) fn calls(&self) -> Vec<&'static str> {
        self.state.lock().expect("state").calls.clone()
    }

    /// Adds a machine as another tool would have made it.
    pub(crate) fn insert(&self, vm: GetVmResponse) {
        self.state
            .lock()
            .expect("state")
            .machines
            .insert(vm.vm_id.clone(), vm);
    }

    pub(crate) fn machine(&self, id: &str) -> Option<GetVmResponse> {
        self.state.lock().expect("state").machines.get(id).cloned()
    }
}

impl Service {
    fn record(&self, call: &'static str) -> MutexGuard<'_, State> {
        let mut state = self.state.lock().expect("state");
        state.calls.push(call);
        state
    }

    fn note(&self, call: &'static str) {
        drop(self.record(call));
    }

    fn with_machine<T>(
        &self,
        call: &'static str,
        id: &str,
        f: impl FnOnce(&mut GetVmResponse) -> T,
    ) -> Result<T, Status> {
        let mut state = self.record(call);
        state
            .machines
            .values_mut()
            .find(|vm| vm.vm_id == id || vm.name == id)
            .map(f)
            .ok_or_else(|| Status::not_found("VM not found (region fra)"))
    }

    fn create(
        &self,
        call: &'static str,
        name: String,
        labels: HashMap<String, String>,
    ) -> CreateVmResponse {
        let mut state = self.record(call);
        state.created += 1;
        let vm_id = format!("vm-{}", state.created);
        let name = if name.is_empty() { vm_id.clone() } else { name };
        let vm = GetVmResponse {
            vm_id: vm_id.clone(),
            name: name.clone(),
            status: "running".to_owned(),
            labels,
            vcpu: 1,
            memory_bytes: 4 << 30,
            access_domain: "boxd.sh".to_owned(),
            ..GetVmResponse::default()
        };
        state.machines.insert(vm_id.clone(), vm.clone());
        CreateVmResponse {
            vm_id,
            name,
            status: "running".to_owned(),
            machine: Some(vm),
            ..CreateVmResponse::default()
        }
    }
}

#[tonic::async_trait]
impl BoxdApi for Service {
    type ExecStream = Stream_<ExecChunk>;
    type FsReadStream = Stream_<FsReadChunk>;
    type FsListStream = Stream_<FsListChunk>;

    async fn create_vm(
        &self,
        request: Request<CreateVmRequest>,
    ) -> Result<Response<CreateVmResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(self.create(
            "CreateVm",
            request.name,
            request.labels,
        )))
    }

    async fn create_vm_from_snapshot(
        &self,
        request: Request<CreateVmFromSnapshotRequest>,
    ) -> Result<Response<CreateVmResponse>, Status> {
        let request = request.into_inner();
        Ok(Response::new(self.create(
            "CreateVmFromSnapshot",
            request.name,
            request.labels,
        )))
    }

    async fn destroy_vm(
        &self,
        request: Request<DestroyVmRequest>,
    ) -> Result<Response<DestroyVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        let mut state = self.record("DestroyVm");
        if self.faults.fail_destroy {
            return Err(Status::internal("the worker is unreachable"));
        }
        let key = state
            .machines
            .values()
            .find(|vm| vm.vm_id == id || vm.name == id)
            .map(|vm| vm.vm_id.clone())
            .ok_or_else(|| Status::not_found("VM not found"))?;
        state.machines.remove(&key);
        Ok(Response::new(DestroyVmResponse {}))
    }

    async fn get_vm(
        &self,
        request: Request<GetVmRequest>,
    ) -> Result<Response<GetVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("GetVm", &id, |vm| vm.clone())
            .map(Response::new)
    }

    async fn list_vms(
        &self,
        request: Request<ListVmsRequest>,
    ) -> Result<Response<ListVmsResponse>, Status> {
        let selectors = request.into_inner().label_selector;
        let state = self.record("ListVms");
        let vms = state
            .machines
            .values()
            .filter(|vm| {
                selectors.iter().all(|selector| {
                    let (key, value) = selector.split_once('=').unwrap_or((selector, ""));
                    vm.labels.get(key).is_some_and(|v| v == value)
                })
            })
            .cloned()
            .collect();
        Ok(Response::new(ListVmsResponse {
            vms,
            ..ListVmsResponse::default()
        }))
    }

    async fn start_vm(
        &self,
        request: Request<StartVmRequest>,
    ) -> Result<Response<StartVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("StartVm", &id, |vm| vm.status = "running".to_owned())?;
        Ok(Response::new(StartVmResponse {}))
    }

    async fn stop_vm(
        &self,
        request: Request<StopVmRequest>,
    ) -> Result<Response<StopVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("StopVm", &id, |vm| vm.status = "stopped".to_owned())?;
        Ok(Response::new(StopVmResponse {}))
    }

    async fn resume_vm(
        &self,
        request: Request<ResumeVmRequest>,
    ) -> Result<Response<ResumeVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("ResumeVm", &id, |vm| vm.status = "running".to_owned())?;
        Ok(Response::new(ResumeVmResponse::default()))
    }

    async fn wake_vm(
        &self,
        request: Request<WakeVmRequest>,
    ) -> Result<Response<WakeVmResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("WakeVm", &id, |vm| vm.status = "running".to_owned())?;
        Ok(Response::new(WakeVmResponse {}))
    }

    async fn set_vm_egress_allow(
        &self,
        request: Request<SetVmEgressAllowRequest>,
    ) -> Result<Response<SetVmEgressAllowResponse>, Status> {
        let request = request.into_inner();
        if self.faults.refuse_egress {
            self.note("SetVmEgressAllow");
            return Err(Status::invalid_argument(
                "10.0.0.0/8 is not a public address",
            ));
        }
        let entries = request.entries.clone();
        self.with_machine("SetVmEgressAllow", &request.vm, |vm| {
            vm.egress_allow = entries;
        })?;
        Ok(Response::new(SetVmEgressAllowResponse {
            entries: request.entries,
        }))
    }

    async fn update_vm_labels(
        &self,
        request: Request<UpdateVmLabelsRequest>,
    ) -> Result<Response<UpdateVmLabelsResponse>, Status> {
        let request = request.into_inner();
        let set = request.set.clone();
        let labels = self.with_machine("UpdateVmLabels", &request.vm, |vm| {
            if request.replace_all {
                vm.labels = set;
            } else {
                vm.labels.extend(set);
            }
            vm.labels.clone()
        })?;
        Ok(Response::new(UpdateVmLabelsResponse { labels }))
    }

    async fn exec(
        &self,
        request: Request<Streaming<ExecChunk>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("no command"))?;
        self.with_machine("Exec", &first.vm_id, |_| ())?;
        let mut stdin = Vec::new();
        while let Some(chunk) = inbound.message().await? {
            stdin.extend(chunk.data);
        }
        // Echoes the command line on stdout and stdin on stderr, then exits 3.
        let chunks = vec![
            Ok(ExecChunk {
                data: first.command.into_bytes(),
                ..ExecChunk::default()
            }),
            Ok(ExecChunk {
                data: stdin,
                is_stderr: true,
                ..ExecChunk::default()
            }),
            Ok(ExecChunk {
                exit_code: 3,
                ..ExecChunk::default()
            }),
        ];
        Ok(Response::new(Box::pin(stream::iter(chunks))))
    }

    async fn fs_read(
        &self,
        request: Request<FsReadRequest>,
    ) -> Result<Response<Self::FsReadStream>, Status> {
        self.note("FsRead");
        let _ = request;
        Err(Status::unimplemented("FsRead"))
    }

    async fn fs_write(
        &self,
        request: Request<Streaming<FsWriteChunk>>,
    ) -> Result<Response<FsWriteResponse>, Status> {
        self.note("FsWrite");
        let _ = request;
        Err(Status::unimplemented("FsWrite"))
    }

    async fn fs_list(
        &self,
        request: Request<FsListRequest>,
    ) -> Result<Response<Self::FsListStream>, Status> {
        self.note("FsList");
        let _ = request;
        Err(Status::unimplemented("FsList"))
    }

    async fn fs_mkdir(
        &self,
        request: Request<FsMkdirRequest>,
    ) -> Result<Response<FsMkdirResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("FsMkdir", &id, |_| ())?;
        Ok(Response::new(FsMkdirResponse::default()))
    }

    async fn fs_realpath(
        &self,
        request: Request<FsRealpathRequest>,
    ) -> Result<Response<FsRealpathResponse>, Status> {
        let id = request.into_inner().vm_id;
        self.with_machine("FsRealpath", &id, |_| ())?;
        Ok(Response::new(FsRealpathResponse {
            path: b"/home/boxd".to_vec(),
        }))
    }

    async fn create_proxy(
        &self,
        request: Request<CreateProxyRequest>,
    ) -> Result<Response<CreateProxyResponse>, Status> {
        let request = request.into_inner();
        let name = self.with_machine("CreateProxy", &request.vm, |vm| vm.name.clone())?;
        Ok(Response::new(CreateProxyResponse {
            domain: format!("{}.{name}.boxd.sh", request.name),
            name: request.name,
            port: request.port,
            ..CreateProxyResponse::default()
        }))
    }

    async fn delete_proxy(
        &self,
        request: Request<DeleteProxyRequest>,
    ) -> Result<Response<DeleteProxyResponse>, Status> {
        self.note("DeleteProxy");
        let _ = request;
        Err(Status::not_found("no such proxy"))
    }

    async fn list_proxies(
        &self,
        request: Request<ListProxiesRequest>,
    ) -> Result<Response<ListProxiesResponse>, Status> {
        self.note("ListProxies");
        let _ = request;
        Ok(Response::new(ListProxiesResponse::default()))
    }

    async fn list_orgs(
        &self,
        _request: Request<ListOrgsRequest>,
    ) -> Result<Response<ListOrgsResponse>, Status> {
        self.note("ListOrgs");
        Ok(Response::new(ListOrgsResponse {
            orgs:           vec![OrgSummary {
                id: "org-1".to_owned(),
                name: "Acme".to_owned(),
                slug: "acme".to_owned(),
                ..OrgSummary::default()
            }],
            default_org_id: "org-1".to_owned(),
        }))
    }

    async fn list_regions(
        &self,
        _request: Request<ListRegionsRequest>,
    ) -> Result<Response<ListRegionsResponse>, Status> {
        self.note("ListRegions");
        Ok(Response::new(ListRegionsResponse {
            regions:     Vec::new(),
            org_id:      "org-1".to_owned(),
            this_region: "fra".to_owned(),
        }))
    }

    async fn whoami(
        &self,
        _request: Request<WhoamiRequest>,
    ) -> Result<Response<WhoamiResponse>, Status> {
        self.note("Whoami");
        Ok(Response::new(WhoamiResponse::default()))
    }
}
