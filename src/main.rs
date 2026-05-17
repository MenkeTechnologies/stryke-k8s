//! `stryke-k8s-helper` — Kubernetes bridge binary.
//!
//! Wraps `kube-rs` + `k8s-openapi`. Output is NDJSON for list / watch /
//! log streams, single JSON otherwise. Accepts a `kind` shortcut
//! (`pods`, `svc`, `deploy`, …) or a strict `group/version/Kind` GVK.
//! Resolves unknown kinds through the cluster's discovery API.

use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::{AsyncBufReadExt, StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api, AttachParams, DeleteParams, DynamicObject, GroupVersionKind, ListParams, LogParams,
    Patch, PatchParams, PostParams, ResourceExt,
};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::core::params::WatchParams;
use kube::core::{ApiResource, GroupVersion};
use kube::discovery::{verbs, Discovery, Scope};
use kube::runtime::watcher;
use kube::{Client, Config};
use serde_json::{json, Value as JsonValue};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-k8s-helper",
    version,
    about = "Kubernetes client for the stryke `k8s` package"
)]
struct Cli {
    #[command(flatten)]
    conn: Conn,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Debug, Clone)]
struct Conn {
    /// kubeconfig context (defaults to current-context).
    #[arg(long, env = "KUBE_CONTEXT", global = true)]
    context: Option<String>,
    /// Override default namespace (most commands also accept --namespace).
    #[arg(long, env = "KUBE_NAMESPACE", global = true)]
    default_namespace: Option<String>,
    /// Explicit kubeconfig path (else $KUBECONFIG / in-cluster SA).
    #[arg(long, env = "KUBECONFIG", global = true)]
    kubeconfig: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// List resources of a kind. `kind` is `pods` / `svc` / `apps/v1/Deployment`.
    Get {
        kind: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
        /// All namespaces (cluster-scoped or every namespace for namespaced kinds).
        #[arg(long, short = 'A')]
        all_namespaces: bool,
        #[arg(long)]
        label_selector: Option<String>,
        #[arg(long)]
        field_selector: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    /// Get one named resource.
    GetOne {
        kind: String,
        name: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
    },
    /// Server-side apply from JSON on stdin (or --doc=…).
    Apply {
        #[arg(long)]
        doc: Option<String>,
        #[arg(long, default_value = "stryke-k8s")]
        field_manager: String,
        #[arg(long)]
        force: bool,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
    },
    /// Create from JSON (POST). Fails on conflict.
    Create {
        #[arg(long)]
        doc: Option<String>,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
    },
    /// Replace (PUT) from JSON. The doc must include `metadata.resourceVersion`.
    Replace {
        #[arg(long)]
        doc: Option<String>,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
    },
    /// Delete a resource by name.
    Delete {
        kind: String,
        name: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
        #[arg(long)]
        grace_period: Option<i64>,
        #[arg(long)]
        force: bool,
    },
    /// Pod logs. NDJSON when `--follow`, otherwise raw text.
    Logs {
        pod: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
        #[arg(long, short = 'c')]
        container: Option<String>,
        #[arg(long)]
        tail: Option<i64>,
        #[arg(long)]
        since_seconds: Option<i64>,
        #[arg(long)]
        previous: bool,
        #[arg(long, short = 'f')]
        follow: bool,
        #[arg(long)]
        timestamps: bool,
    },
    /// Scale a Deployment / StatefulSet / ReplicaSet to `--replicas`.
    Scale {
        kind: String,
        name: String,
        #[arg(long)]
        replicas: i32,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
    },
    /// Watch a resource stream as NDJSON events: `{type, object}`.
    Watch {
        kind: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
        #[arg(long)]
        label_selector: Option<String>,
        #[arg(long)]
        field_selector: Option<String>,
    },
    /// Exec a command in a pod and stream stdout as NDJSON `{stream:"stdout"|"stderr", data}`.
    Exec {
        pod: String,
        #[arg(long, short = 'n')]
        namespace: Option<String>,
        #[arg(long, short = 'c')]
        container: Option<String>,
        /// Command + args, e.g. `--cmd sh -- -c "echo hi"`.
        #[arg(long, num_args = 1.., required = true)]
        cmd: Vec<String>,
    },
    /// Cluster /version.
    Version,
    /// Healthz round-trip → `{ok: true|false}`.
    Ping,
    /// List kubeconfig contexts.
    Contexts,
    /// Print current kubeconfig context.
    CurrentContext,
    /// All discoverable api-resources as NDJSON.
    ApiResources,
    /// Convenience: list namespaces (NDJSON of `{name, status}`).
    Namespaces,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-k8s-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match &cli.cmd {
        Cmd::Contexts => return cmd_contexts(&cli.conn).await,
        Cmd::CurrentContext => return cmd_current_context(&cli.conn).await,
        _ => {}
    }

    let client = make_client(&cli.conn).await?;
    let default_ns = cli
        .conn
        .default_namespace
        .clone()
        .unwrap_or_else(|| client.default_namespace().to_string());

    match cli.cmd {
        Cmd::Get {
            kind,
            namespace,
            all_namespaces,
            label_selector,
            field_selector,
            limit,
        } => {
            cmd_get(
                &client,
                &kind,
                namespace.as_deref().unwrap_or(&default_ns),
                all_namespaces,
                label_selector.as_deref(),
                field_selector.as_deref(),
                limit,
            )
            .await
        }
        Cmd::GetOne { kind, name, namespace } => {
            cmd_get_one(
                &client,
                &kind,
                &name,
                namespace.as_deref().unwrap_or(&default_ns),
            )
            .await
        }
        Cmd::Apply { doc, field_manager, force, namespace } => {
            cmd_apply(
                &client,
                doc.as_deref(),
                &field_manager,
                force,
                namespace.as_deref().unwrap_or(&default_ns),
            )
            .await
        }
        Cmd::Create { doc, namespace } => {
            cmd_create(&client, doc.as_deref(), namespace.as_deref().unwrap_or(&default_ns)).await
        }
        Cmd::Replace { doc, namespace } => {
            cmd_replace(&client, doc.as_deref(), namespace.as_deref().unwrap_or(&default_ns))
                .await
        }
        Cmd::Delete { kind, name, namespace, grace_period, force } => {
            cmd_delete(
                &client,
                &kind,
                &name,
                namespace.as_deref().unwrap_or(&default_ns),
                grace_period,
                force,
            )
            .await
        }
        Cmd::Logs {
            pod,
            namespace,
            container,
            tail,
            since_seconds,
            previous,
            follow,
            timestamps,
        } => {
            cmd_logs(
                &client,
                &pod,
                namespace.as_deref().unwrap_or(&default_ns),
                container.as_deref(),
                tail,
                since_seconds,
                previous,
                follow,
                timestamps,
            )
            .await
        }
        Cmd::Scale { kind, name, replicas, namespace } => {
            cmd_scale(
                &client,
                &kind,
                &name,
                replicas,
                namespace.as_deref().unwrap_or(&default_ns),
            )
            .await
        }
        Cmd::Watch { kind, namespace, label_selector, field_selector } => {
            cmd_watch(
                &client,
                &kind,
                namespace.as_deref().unwrap_or(&default_ns),
                label_selector.as_deref(),
                field_selector.as_deref(),
            )
            .await
        }
        Cmd::Exec { pod, namespace, container, cmd } => {
            cmd_exec(
                &client,
                &pod,
                namespace.as_deref().unwrap_or(&default_ns),
                container.as_deref(),
                &cmd,
            )
            .await
        }
        Cmd::Version => cmd_version(&client).await,
        Cmd::Ping => cmd_ping(&client).await,
        Cmd::ApiResources => cmd_api_resources(&client).await,
        Cmd::Namespaces => cmd_namespaces(&client).await,
        Cmd::Contexts | Cmd::CurrentContext => unreachable!(),
    }
}

/* ------------------------------------------------------------------------- */
/* connection                                                                */
/* ------------------------------------------------------------------------- */

async fn make_client(c: &Conn) -> Result<Client> {
    let opts = KubeConfigOptions {
        context: c.context.clone(),
        cluster: None,
        user: None,
    };
    let cfg = if let Some(path) = &c.kubeconfig {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading kubeconfig {path}"))?;
        let kc: Kubeconfig = serde_yaml::from_str(&raw).context("parsing kubeconfig")?;
        Config::from_custom_kubeconfig(kc, &opts)
            .await
            .context("building config from custom kubeconfig")?
    } else {
        match Config::from_kubeconfig(&opts).await {
            Ok(cfg) => cfg,
            Err(_) => Config::incluster().context("no kubeconfig and not in-cluster")?,
        }
    };
    Client::try_from(cfg).context("creating kube client")
}

/* ------------------------------------------------------------------------- */
/* GVK resolution                                                            */
/* ------------------------------------------------------------------------- */

/// Resolve a user-facing `kind` string into (ApiResource, capabilities).
/// Accepts `pods`, `po`, `service`, `apps/v1/Deployment`, etc.
async fn resolve_kind(client: &Client, raw: &str) -> Result<(ApiResource, Scope)> {
    // strict GVK form: group/version/Kind   or   /v1/Kind   or   v1/Kind
    if raw.contains('/') {
        let parts: Vec<&str> = raw.splitn(3, '/').collect();
        let (group, version, kind) = match parts.as_slice() {
            [g, v, k] => (*g, *v, *k),
            [v, k] => ("", *v, *k),
            _ => bail!("kind `{raw}` must be `kind`, `version/Kind`, or `group/version/Kind`"),
        };
        let gvk = GroupVersionKind::gvk(group, version, kind);
        let discovery = Discovery::new(client.clone()).run().await?;
        if let Some((ar, caps)) = discovery.resolve_gvk(&gvk) {
            return Ok((ar, caps.scope));
        }
        // last resort: synthesise an ApiResource (plural = lowercase kind+"s")
        let ar = ApiResource::from_gvk(&gvk);
        return Ok((ar, Scope::Namespaced));
    }

    let lower = raw.to_lowercase();
    let discovery = Discovery::new(client.clone()).run().await?;
    for group in discovery.groups() {
        for (ar, caps) in group.recommended_resources() {
            if matches(&ar, &lower) {
                return Ok((ar, caps.scope));
            }
        }
    }
    bail!("could not resolve kind `{raw}` against cluster api-resources")
}

fn matches(ar: &ApiResource, lower: &str) -> bool {
    if ar.kind.to_lowercase() == lower {
        return true;
    }
    if ar.plural.to_lowercase() == lower {
        return true;
    }
    // singular: kube doesn't carry it on ApiResource; treat kind-lowercase as singular alias
    if format!("{}s", ar.kind.to_lowercase()) == lower {
        return true;
    }
    false
}

/* ------------------------------------------------------------------------- */
/* helpers                                                                   */
/* ------------------------------------------------------------------------- */

fn api(client: &Client, ar: &ApiResource, scope: &Scope, ns: &str, all_ns: bool) -> Api<DynamicObject> {
    match scope {
        Scope::Cluster => Api::all_with(client.clone(), ar),
        Scope::Namespaced => {
            if all_ns {
                Api::all_with(client.clone(), ar)
            } else {
                Api::namespaced_with(client.clone(), ns, ar)
            }
        }
    }
}

fn read_doc(s: Option<&str>) -> Result<JsonValue> {
    let text = match s {
        Some(s) => s.to_string(),
        None => {
            let mut buf = String::new();
            io::stdin().read_to_string(&mut buf)?;
            buf
        }
    };
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        Ok(serde_json::from_str(&text).context("parsing JSON document")?)
    } else {
        Ok(serde_yaml::from_str(&text).context("parsing YAML document")?)
    }
}

use std::io::Read;

fn extract_gvk(doc: &JsonValue) -> Result<GroupVersionKind> {
    let api_version = doc
        .get("apiVersion")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc missing `apiVersion`"))?;
    let kind = doc
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc missing `kind`"))?;
    let gv: GroupVersion = api_version
        .parse()
        .with_context(|| format!("invalid apiVersion `{api_version}`"))?;
    Ok(GroupVersionKind::gvk(&gv.group, &gv.version, kind))
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_ndjson<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

#[allow(clippy::too_many_arguments)]
async fn cmd_get(
    client: &Client,
    kind: &str,
    namespace: &str,
    all_ns: bool,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<u32>,
) -> Result<()> {
    let (ar, scope) = resolve_kind(client, kind).await?;
    let api = api(client, &ar, &scope, namespace, all_ns);
    let mut lp = ListParams::default();
    if let Some(ls) = label_selector {
        lp = lp.labels(ls);
    }
    if let Some(fs) = field_selector {
        lp = lp.fields(fs);
    }
    if let Some(l) = limit {
        lp = lp.limit(l);
    }
    let list = api.list(&lp).await.context("list")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for obj in list.items {
        emit_ndjson(&mut out, &obj)?;
    }
    Ok(())
}

async fn cmd_get_one(
    client: &Client,
    kind: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let (ar, scope) = resolve_kind(client, kind).await?;
    let api = api(client, &ar, &scope, namespace, false);
    match api.get_opt(name).await.context("get")? {
        Some(obj) => emit_json(&obj),
        None => emit_json(&JsonValue::Null),
    }
}

async fn cmd_apply(
    client: &Client,
    doc: Option<&str>,
    field_manager: &str,
    force: bool,
    default_ns: &str,
) -> Result<()> {
    let body = read_doc(doc)?;
    let gvk = extract_gvk(&body)?;
    let discovery = Discovery::new(client.clone()).run().await?;
    let (ar, caps) = discovery
        .resolve_gvk(&gvk)
        .ok_or_else(|| anyhow!("apiVersion/kind `{}/{}` not found in cluster", gvk.api_version(), gvk.kind))?;
    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.metadata.name is required for apply"))?;
    let ns = body
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or(default_ns)
        .to_string();
    let api = api(client, &ar, &caps.scope, &ns, false);
    let mut pp = PatchParams::apply(field_manager);
    if force {
        pp = pp.force();
    }
    let patch: DynamicObject = serde_json::from_value(body).context("doc → DynamicObject")?;
    let res = api.patch(name, &pp, &Patch::Apply(&patch)).await.context("apply")?;
    emit_json(&res)
}

async fn cmd_create(client: &Client, doc: Option<&str>, default_ns: &str) -> Result<()> {
    let body = read_doc(doc)?;
    let gvk = extract_gvk(&body)?;
    let discovery = Discovery::new(client.clone()).run().await?;
    let (ar, caps) = discovery
        .resolve_gvk(&gvk)
        .ok_or_else(|| anyhow!("apiVersion/kind not found in cluster"))?;
    let ns = body
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or(default_ns)
        .to_string();
    let api = api(client, &ar, &caps.scope, &ns, false);
    let obj: DynamicObject = serde_json::from_value(body).context("doc → DynamicObject")?;
    let res = api.create(&PostParams::default(), &obj).await.context("create")?;
    emit_json(&res)
}

async fn cmd_replace(client: &Client, doc: Option<&str>, default_ns: &str) -> Result<()> {
    let body = read_doc(doc)?;
    let gvk = extract_gvk(&body)?;
    let discovery = Discovery::new(client.clone()).run().await?;
    let (ar, caps) = discovery
        .resolve_gvk(&gvk)
        .ok_or_else(|| anyhow!("apiVersion/kind not found in cluster"))?;
    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.metadata.name is required for replace"))?
        .to_string();
    let ns = body
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str())
        .unwrap_or(default_ns)
        .to_string();
    let api = api(client, &ar, &caps.scope, &ns, false);
    let obj: DynamicObject = serde_json::from_value(body).context("doc → DynamicObject")?;
    let res = api.replace(&name, &PostParams::default(), &obj).await.context("replace")?;
    emit_json(&res)
}

async fn cmd_delete(
    client: &Client,
    kind: &str,
    name: &str,
    namespace: &str,
    grace_period: Option<i64>,
    force: bool,
) -> Result<()> {
    let (ar, scope) = resolve_kind(client, kind).await?;
    let api = api(client, &ar, &scope, namespace, false);
    let mut dp = DeleteParams::default();
    if let Some(g) = grace_period {
        dp = dp.grace_period(g as u32);
    }
    if force {
        dp = dp.grace_period(0);
    }
    let res = api.delete(name, &dp).await.context("delete")?;
    let out = match res {
        either::Either::Left(obj) => json!({"status": "Deleting", "object": obj}),
        either::Either::Right(status) => json!({"status": "Done", "details": status}),
    };
    emit_json(&out)
}

#[allow(clippy::too_many_arguments)]
async fn cmd_logs(
    client: &Client,
    pod: &str,
    namespace: &str,
    container: Option<&str>,
    tail: Option<i64>,
    since_seconds: Option<i64>,
    previous: bool,
    follow: bool,
    timestamps: bool,
) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let mut lp = LogParams {
        previous,
        follow,
        timestamps,
        ..Default::default()
    };
    if let Some(c) = container {
        lp.container = Some(c.to_string());
    }
    lp.tail_lines = tail;
    lp.since_seconds = since_seconds;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    if follow {
        let mut stream = pods.log_stream(pod, &lp).await.context("log_stream")?.lines();
        while let Some(line) = stream.try_next().await.context("log line")? {
            emit_ndjson(&mut out, &json!({ "line": line }))?;
            out.flush().ok();
        }
    } else {
        let text = pods.logs(pod, &lp).await.context("logs")?;
        out.write_all(text.as_bytes())?;
        if !text.ends_with('\n') {
            out.write_all(b"\n")?;
        }
    }
    Ok(())
}

async fn cmd_scale(
    client: &Client,
    kind: &str,
    name: &str,
    replicas: i32,
    namespace: &str,
) -> Result<()> {
    let (ar, scope) = resolve_kind(client, kind).await?;
    let api = api(client, &ar, &scope, namespace, false);
    let patch = json!({ "spec": { "replicas": replicas } });
    let pp = PatchParams::default();
    let res = api
        .patch_scale(name, &pp, &Patch::Merge(&patch))
        .await
        .context("patch_scale")?;
    emit_json(&res)
}

async fn cmd_watch(
    client: &Client,
    kind: &str,
    namespace: &str,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<()> {
    let (ar, scope) = resolve_kind(client, kind).await?;
    let api = api(client, &ar, &scope, namespace, false);
    let mut wp = WatchParams::default();
    if let Some(ls) = label_selector {
        wp = wp.labels(ls);
    }
    if let Some(fs) = field_selector {
        wp = wp.fields(fs);
    }
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut stream = watcher(api, watcher::Config::default()).boxed();
    while let Some(ev) = stream.try_next().await.context("watcher")? {
        match ev {
            watcher::Event::Apply(obj) => {
                emit_ndjson(&mut out, &json!({ "type": "APPLY", "object": obj }))?
            }
            watcher::Event::Delete(obj) => {
                emit_ndjson(&mut out, &json!({ "type": "DELETE", "object": obj }))?
            }
            watcher::Event::Init => emit_ndjson(&mut out, &json!({ "type": "INIT" }))?,
            watcher::Event::InitApply(obj) => {
                emit_ndjson(&mut out, &json!({ "type": "INIT_APPLY", "object": obj }))?
            }
            watcher::Event::InitDone => emit_ndjson(&mut out, &json!({ "type": "INIT_DONE" }))?,
        }
        out.flush().ok();
    }
    Ok(())
}

async fn cmd_exec(
    client: &Client,
    pod: &str,
    namespace: &str,
    container: Option<&str>,
    cmd: &[String],
) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let mut ap = AttachParams::default().stdout(true).stderr(true);
    if let Some(c) = container {
        ap = ap.container(c.to_string());
    }
    let mut attached = pods.exec(pod, cmd.iter(), &ap).await.context("exec")?;
    let mut stdout_stream = attached
        .stdout()
        .ok_or_else(|| anyhow!("no stdout on exec"))?;
    let mut stderr_stream = attached.stderr();

    use tokio::io::AsyncReadExt;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut buf = [0u8; 8192];

    loop {
        tokio::select! {
            n = stdout_stream.read(&mut buf) => {
                let n = n.context("exec stdout")?;
                if n == 0 { break; }
                emit_ndjson(&mut out, &json!({ "stream": "stdout", "data": String::from_utf8_lossy(&buf[..n]) }))?;
                out.flush().ok();
            }
            n = async {
                if let Some(s) = stderr_stream.as_mut() {
                    s.read(&mut buf).await
                } else {
                    futures_util::future::pending().await
                }
            } => {
                let n = n.context("exec stderr")?;
                if n == 0 { continue; }
                emit_ndjson(&mut out, &json!({ "stream": "stderr", "data": String::from_utf8_lossy(&buf[..n]) }))?;
                out.flush().ok();
            }
        }
    }

    attached.join().await.context("exec join")?;
    Ok(())
}

async fn cmd_version(client: &Client) -> Result<()> {
    let v = client.apiserver_version().await.context("apiserver_version")?;
    emit_json(&v)
}

async fn cmd_ping(client: &Client) -> Result<()> {
    let ok = client.apiserver_version().await.is_ok();
    emit_json(&json!({ "ok": ok }))
}

async fn cmd_contexts(c: &Conn) -> Result<()> {
    let kc = load_kubeconfig(c)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for ctx in &kc.contexts {
        let ctx_ctx = &ctx.context;
        emit_ndjson(
            &mut out,
            &json!({
                "name": ctx.name,
                "cluster": ctx_ctx.as_ref().map(|c| c.cluster.clone()),
                "user": ctx_ctx.as_ref().map(|c| c.user.clone()),
                "namespace": ctx_ctx.as_ref().and_then(|c| c.namespace.clone()),
                "current": ctx.name == kc.current_context.as_deref().unwrap_or(""),
            }),
        )?;
    }
    Ok(())
}

async fn cmd_current_context(c: &Conn) -> Result<()> {
    let kc = load_kubeconfig(c)?;
    emit_json(&json!({ "current_context": kc.current_context }))
}

fn load_kubeconfig(c: &Conn) -> Result<Kubeconfig> {
    if let Some(path) = &c.kubeconfig {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading kubeconfig {path}"))?;
        return serde_yaml::from_str(&raw).context("parsing kubeconfig");
    }
    Kubeconfig::read().context("reading kubeconfig from default search path")
}

async fn cmd_api_resources(client: &Client) -> Result<()> {
    let discovery = Discovery::new(client.clone()).run().await?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for group in discovery.groups() {
        for (ar, caps) in group.recommended_resources() {
            emit_ndjson(
                &mut out,
                &json!({
                    "group": ar.group,
                    "version": ar.version,
                    "kind": ar.kind,
                    "plural": ar.plural,
                    "namespaced": matches!(caps.scope, Scope::Namespaced),
                    "verbs": caps.operations.iter().map(|v| v.as_str()).collect::<Vec<_>>(),
                }),
            )?;
        }
    }
    Ok(())
}

async fn cmd_namespaces(client: &Client) -> Result<()> {
    use k8s_openapi::api::core::v1::Namespace;
    let api: Api<Namespace> = Api::all(client.clone());
    let list = api.list(&ListParams::default()).await.context("list ns")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for ns in list.items {
        let status = ns
            .status
            .as_ref()
            .and_then(|s| s.phase.clone())
            .unwrap_or_default();
        let labels: BTreeMap<String, String> = ns.metadata.labels.clone().unwrap_or_default();
        emit_ndjson(
            &mut out,
            &json!({
                "name": ns.name_any(),
                "status": status,
                "labels": labels,
            }),
        )?;
    }
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* verbs helper (silence unused-import warning under feature toggles)         */
/* ------------------------------------------------------------------------- */
#[allow(dead_code)]
fn _verbs_marker() -> &'static [&'static str] {
    &[
        verbs::LIST,
        verbs::GET,
        verbs::CREATE,
        verbs::DELETE,
        verbs::PATCH,
        verbs::WATCH,
    ]
}
