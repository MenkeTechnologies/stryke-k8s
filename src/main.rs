//! `stryke-k8s-helper` — Kubernetes bridge binary.
//!
//! Wraps `kube-rs` + `k8s-openapi`. Output is NDJSON for list / watch /
//! log streams, single JSON otherwise. Accepts a `kind` shortcut
//! (`pods`, `svc`, `deploy`, …) or a strict `group/version/Kind` GVK.
//! Resolves unknown kinds through the cluster's discovery API.

use std::collections::BTreeMap;
use std::io::{self, BufWriter, Write};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use futures_util::{AsyncBufReadExt, StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api, AttachParams, DeleteParams, DynamicObject, GroupVersionKind, ListParams, LogParams,
    Patch, PatchParams, PostParams, ResourceExt,
};
use kube::config::{KubeConfigOptions, Kubeconfig};
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
    let normalised = expand_shortname(&lower).unwrap_or(&lower);
    let discovery = Discovery::new(client.clone()).run().await?;
    for group in discovery.groups() {
        for (ar, caps) in group.recommended_resources() {
            if matches(&ar, normalised) {
                return Ok((ar, caps.scope));
            }
        }
    }
    bail!("could not resolve kind `{raw}` against cluster api-resources")
}

/// Standard kubectl short names → canonical plural form.
fn expand_shortname(s: &str) -> Option<&'static str> {
    match s {
        "po"      => Some("pods"),
        "svc"     => Some("services"),
        "deploy"  => Some("deployments"),
        "rs"      => Some("replicasets"),
        "ds"      => Some("daemonsets"),
        "sts"     => Some("statefulsets"),
        "cm"      => Some("configmaps"),
        "ns"      => Some("namespaces"),
        "no"      => Some("nodes"),
        "ing"     => Some("ingresses"),
        "ep"      => Some("endpoints"),
        "ev"      => Some("events"),
        "pv"      => Some("persistentvolumes"),
        "pvc"     => Some("persistentvolumeclaims"),
        "sa"      => Some("serviceaccounts"),
        "pdb"     => Some("poddisruptionbudgets"),
        "hpa"     => Some("horizontalpodautoscalers"),
        "crd"     => Some("customresourcedefinitions"),
        "cs"      => Some("componentstatuses"),
        "limits"  => Some("limitranges"),
        "quota"   => Some("resourcequotas"),
        "netpol"  => Some("networkpolicies"),
        "pc"      => Some("priorityclasses"),
        "sc"      => Some("storageclasses"),
        "cj"      => Some("cronjobs"),
        _         => None,
    }
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
        .ok_or_else(|| anyhow!("doc.metadata.name is required for apply"))?
        .to_string();
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
    let res = api.patch(&name, &pp, &Patch::Apply(&patch)).await.context("apply")?;
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
    let mut wc = watcher::Config::default();
    if let Some(ls) = label_selector {
        wc = wc.labels(ls);
    }
    if let Some(fs) = field_selector {
        wc = wc.fields(fs);
    }
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let mut stream = watcher(api, wc).boxed();
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
    let mut obuf = [0u8; 8192];
    let mut ebuf = [0u8; 8192];

    loop {
        tokio::select! {
            n = stdout_stream.read(&mut obuf) => {
                let n = n.context("exec stdout")?;
                if n == 0 { break; }
                emit_ndjson(&mut out, &json!({ "stream": "stdout", "data": String::from_utf8_lossy(&obuf[..n]) }))?;
                out.flush().ok();
            }
            n = async {
                if let Some(s) = stderr_stream.as_mut() {
                    s.read(&mut ebuf).await
                } else {
                    futures_util::future::pending().await
                }
            } => {
                let n = n.context("exec stderr")?;
                if n == 0 { continue; }
                emit_ndjson(&mut out, &json!({ "stream": "stderr", "data": String::from_utf8_lossy(&ebuf[..n]) }))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ─── expand_shortname ────────────────────────────────────────────

    #[test]
    fn expand_shortname_common_kubectl_aliases() {
        assert_eq!(expand_shortname("po"), Some("pods"));
        assert_eq!(expand_shortname("svc"), Some("services"));
        assert_eq!(expand_shortname("deploy"), Some("deployments"));
        assert_eq!(expand_shortname("rs"), Some("replicasets"));
        assert_eq!(expand_shortname("ds"), Some("daemonsets"));
        assert_eq!(expand_shortname("sts"), Some("statefulsets"));
        assert_eq!(expand_shortname("cm"), Some("configmaps"));
        assert_eq!(expand_shortname("ns"), Some("namespaces"));
        assert_eq!(expand_shortname("no"), Some("nodes"));
        assert_eq!(expand_shortname("pvc"), Some("persistentvolumeclaims"));
        assert_eq!(expand_shortname("pv"), Some("persistentvolumes"));
        assert_eq!(expand_shortname("sa"), Some("serviceaccounts"));
        assert_eq!(expand_shortname("crd"), Some("customresourcedefinitions"));
        assert_eq!(expand_shortname("cj"), Some("cronjobs"));
    }

    #[test]
    fn expand_shortname_unknown_returns_none() {
        assert_eq!(expand_shortname("pods"), None); // already canonical
        assert_eq!(expand_shortname("Pod"), None);  // case-sensitive
        assert_eq!(expand_shortname(""), None);
        assert_eq!(expand_shortname("xyz"), None);
    }

    #[test]
    fn expand_shortname_case_sensitive() {
        // kubectl shortnames are lowercase-only; mixed case must not match.
        assert_eq!(expand_shortname("PO"), None);
        assert_eq!(expand_shortname("Po"), None);
        assert_eq!(expand_shortname("PVC"), None);
    }

    // ─── matches ─────────────────────────────────────────────────────

    fn ar(group: &str, version: &str, kind: &str, plural: &str) -> ApiResource {
        ApiResource {
            group: group.into(),
            version: version.into(),
            api_version: if group.is_empty() {
                version.into()
            } else {
                format!("{group}/{version}")
            },
            kind: kind.into(),
            plural: plural.into(),
        }
    }

    #[test]
    fn matches_on_kind_lowercase() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(matches(&r, "pod"));
    }

    #[test]
    fn matches_on_plural() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(matches(&r, "pods"));
    }

    #[test]
    fn matches_kind_plus_s_singular_alias() {
        // The fallback rule: if user asks for "configmaps" and the kind is
        // "ConfigMap" but plural is unknown, we still match. Pin the rule.
        let r = ar("", "v1", "Service", "services");
        // services is plural; "service" matches via kind-lowercase rule.
        assert!(matches(&r, "service"));
    }

    #[test]
    fn matches_rejects_unrelated_string() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(!matches(&r, "podx"));
        assert!(!matches(&r, ""));
        assert!(!matches(&r, "node"));
    }

    // ─── read_doc (explicit-string branch — stdin branch skipped) ────

    #[test]
    fn read_doc_json_object() {
        let v = read_doc(Some(r#"{"apiVersion":"v1","kind":"Pod"}"#)).unwrap();
        assert_eq!(v["apiVersion"], json!("v1"));
        assert_eq!(v["kind"], json!("Pod"));
    }

    #[test]
    fn read_doc_json_array() {
        let v = read_doc(Some("[1,2,3]")).unwrap();
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 3);
    }

    #[test]
    fn read_doc_yaml_when_not_starting_with_brace_or_bracket() {
        let yaml = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: foo\n";
        let v = read_doc(Some(yaml)).unwrap();
        assert_eq!(v["apiVersion"], json!("v1"));
        assert_eq!(v["kind"], json!("Pod"));
        assert_eq!(v["metadata"]["name"], json!("foo"));
    }

    #[test]
    fn read_doc_json_with_leading_whitespace() {
        // trim_start checks the first non-ws char, so leading newline/spaces
        // before '{' still routes to the JSON path.
        let v = read_doc(Some("\n  \n{\"k\":1}")).unwrap();
        assert_eq!(v["k"], json!(1));
    }

    #[test]
    fn read_doc_invalid_input_errors() {
        let err = read_doc(Some("{not json")).unwrap_err();
        assert!(format!("{err:#}").to_lowercase().contains("parsing"));
    }

    // ─── extract_gvk ─────────────────────────────────────────────────

    #[test]
    fn extract_gvk_core_v1() {
        let doc = json!({"apiVersion":"v1","kind":"Pod"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "");
        assert_eq!(g.version, "v1");
        assert_eq!(g.kind, "Pod");
    }

    #[test]
    fn extract_gvk_apps_v1() {
        let doc = json!({"apiVersion":"apps/v1","kind":"Deployment"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "apps");
        assert_eq!(g.version, "v1");
        assert_eq!(g.kind, "Deployment");
    }

    #[test]
    fn extract_gvk_missing_apiversion_errors() {
        let doc = json!({"kind":"Pod"});
        let err = extract_gvk(&doc).unwrap_err();
        assert!(format!("{err}").contains("apiVersion"));
    }

    #[test]
    fn extract_gvk_missing_kind_errors() {
        let doc = json!({"apiVersion":"v1"});
        let err = extract_gvk(&doc).unwrap_err();
        assert!(format!("{err}").contains("kind"));
    }

    #[test]
    fn extract_gvk_apiversion_not_string_errors() {
        let doc = json!({"apiVersion":123,"kind":"Pod"});
        let err = extract_gvk(&doc).unwrap_err();
        // as_str returns None for non-string → same "missing" message.
        assert!(format!("{err}").contains("apiVersion"));
    }

    // ─── emit_ndjson ─────────────────────────────────────────────────

    #[test]
    fn emit_ndjson_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_multi_call_line_count() {
        let mut buf = Vec::new();
        for i in 0..3 {
            emit_ndjson(&mut buf, &json!({"i": i})).unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.lines().count(), 3);
    }

    #[test]
    fn expand_shortname_ingress_and_endpoints() {
        assert_eq!(expand_shortname("ing"), Some("ingresses"));
        assert_eq!(expand_shortname("ep"), Some("endpoints"));
    }

    #[test]
    fn expand_shortname_hpa_and_cronjob() {
        assert_eq!(expand_shortname("hpa"), Some("horizontalpodautoscalers"));
        assert_eq!(expand_shortname("cj"), Some("cronjobs"));
    }

    #[test]
    fn matches_on_api_version_group_form() {
        let r = ar("apps", "v1", "Deployment", "deployments");
        assert!(matches(&r, "deployments"));
        assert!(matches(&r, "deployment"));
    }

    #[test]
    fn matches_rejects_wrong_kind() {
        let r = ar("batch", "v1", "Job", "jobs");
        assert!(!matches(&r, "cronjob"));
    }

    #[test]
    fn extract_gvk_beta_api_version() {
        let doc = json!({"apiVersion":"batch/v1beta1","kind":"CronJob"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "batch");
        assert_eq!(g.version, "v1beta1");
        assert_eq!(g.kind, "CronJob");
    }

    #[test]
    fn read_doc_yaml_list_still_parses() {
        let yaml = "- item: 1\n";
        let v = read_doc(Some(yaml)).unwrap();
        assert!(v.is_array());
    }

    #[test]
    fn extract_gvk_empty_kind_string_accepted() {
        // Present but empty — not treated as missing; pins liberal behavior.
        let doc = json!({"apiVersion":"v1","kind":""});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.kind, "");
    }

    #[test]
    fn expand_shortname_netpol_and_sc() {
        assert_eq!(expand_shortname("netpol"), Some("networkpolicies"));
        assert_eq!(expand_shortname("sc"), Some("storageclasses"));
    }

    #[test]
    fn expand_shortname_limits_and_quota() {
        assert_eq!(expand_shortname("limits"), Some("limitranges"));
        assert_eq!(expand_shortname("quota"), Some("resourcequotas"));
    }

    #[test]
    fn matches_singular_alias_adds_s_to_kind() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(matches(&r, "pods"));
        assert!(matches(&r, "pod"));
    }

    #[test]
    fn matches_kind_case_insensitive() {
        let r = ar("", "v1", "ConfigMap", "configmaps");
        assert!(matches(&r, "configmap"));
    }

    #[test]
    fn extract_gvk_core_group_empty() {
        let doc = json!({"apiVersion":"v1","kind":"Service"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "");
        assert_eq!(g.version, "v1");
        assert_eq!(g.kind, "Service");
    }

    #[test]
    fn read_doc_json_array_root() {
        let v = read_doc(Some("[{\"a\":1}]")).unwrap();
        assert!(v.is_array());
    }

    #[test]
    fn matches_rejects_plural_with_extra_suffix() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(!matches(&r, "podss"));
    }

    #[test]
    fn expand_shortname_pdb() {
        assert_eq!(expand_shortname("pdb"), Some("poddisruptionbudgets"));
    }

    #[test]
    fn expand_shortname_ev_and_cs() {
        assert_eq!(expand_shortname("ev"), Some("events"));
        assert_eq!(expand_shortname("cs"), Some("componentstatuses"));
    }

    #[test]
    fn expand_shortname_pc_priorityclass() {
        assert_eq!(expand_shortname("pc"), Some("priorityclasses"));
    }

    #[test]
    fn matches_on_plural_exact() {
        let r = ar("apps", "v1", "Deployment", "deployments");
        assert!(matches(&r, "deployments"));
    }

    #[test]
    fn extract_gvk_apiextensions_group() {
        let doc = json!({"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "apiextensions.k8s.io");
        assert_eq!(g.kind, "CustomResourceDefinition");
    }

    #[test]
    fn read_doc_yaml_mapping() {
        let yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cfg\n";
        let v = read_doc(Some(yaml)).unwrap();
        assert_eq!(v["kind"], json!("ConfigMap"));
        assert_eq!(v["metadata"]["name"], json!("cfg"));
    }

    #[test]
    fn matches_kind_exact_case_folded() {
        let r = ar("", "v1", "ServiceAccount", "serviceaccounts");
        assert!(matches(&r, "serviceaccount"));
    }

    #[test]
    fn extract_gvk_unversioned_string_becomes_core_group() {
        // kube GroupVersion treats a lone segment as core API version (no group).
        let doc = json!({"apiVersion":"not-valid","kind":"Pod"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "");
        assert_eq!(g.version, "not-valid");
        assert_eq!(g.kind, "Pod");
    }

    #[test]
    fn extract_gvk_networking_group() {
        let doc = json!({"apiVersion":"networking.k8s.io/v1","kind":"Ingress"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "networking.k8s.io");
        assert_eq!(g.kind, "Ingress");
    }

    #[test]
    fn read_doc_json_null_literal() {
        let v = read_doc(Some("null")).unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn matches_job_plural_and_singular() {
        let r = ar("batch", "v1", "Job", "jobs");
        assert!(matches(&r, "jobs"));
        assert!(matches(&r, "job"));
    }

    #[test]
    fn extract_gvk_kind_missing_errors() {
        let doc = json!({"apiVersion":"v1"});
        assert!(extract_gvk(&doc).is_err());
    }

    #[test]
    fn matches_rejects_empty_selector() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(!matches(&r, ""));
    }

    #[test]
    fn extract_gvk_alpha_version_segment() {
        let doc = json!({"apiVersion":"certificates.k8s.io/v1alpha1","kind":"CertificateSigningRequest"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.version, "v1alpha1");
    }

    #[test]
    fn emit_ndjson_preserves_unicode_key() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"名前": "値"})).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("名前") || s.contains("\\u"));
    }

    #[test]
    fn expand_shortname_pv_only() {
        assert_eq!(expand_shortname("pv"), Some("persistentvolumes"));
    }

    #[test]
    fn expand_shortname_sa_serviceaccount() {
        assert_eq!(expand_shortname("sa"), Some("serviceaccounts"));
    }

    #[test]
    fn expand_shortname_crd() {
        assert_eq!(expand_shortname("crd"), Some("customresourcedefinitions"));
    }

    #[test]
    fn matches_statefulset_plural() {
        let r = ar("apps", "v1", "StatefulSet", "statefulsets");
        assert!(matches(&r, "statefulset"));
    }

    #[test]
    fn extract_gvk_storage_group() {
        let doc = json!({"apiVersion":"storage.k8s.io/v1","kind":"StorageClass"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "storage.k8s.io");
        assert_eq!(g.kind, "StorageClass");
    }

    #[test]
    fn read_doc_json_number_root() {
        let v = read_doc(Some("42")).unwrap();
        assert_eq!(v, json!(42));
    }

    #[test]
    fn matches_cronjob_kind_alias() {
        let r = ar("batch", "v1", "CronJob", "cronjobs");
        assert!(matches(&r, "cronjob"));
    }

    #[test]
    fn emit_ndjson_scalar_string() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!("line")).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "\"line\"\n");
    }

    #[test]
    fn extract_gvk_kind_non_string_errors() {
        let doc = json!({"apiVersion":"v1","kind":123});
        assert!(extract_gvk(&doc).is_err());
    }

    #[test]
    fn expand_shortname_ing_ingress() {
        assert_eq!(expand_shortname("ing"), Some("ingresses"));
    }

    #[test]
    fn expand_shortname_hpa() {
        assert_eq!(expand_shortname("hpa"), Some("horizontalpodautoscalers"));
    }

    #[test]
    fn matches_service_plural() {
        let r = ar("", "v1", "Service", "services");
        assert!(matches(&r, "services"));
    }

    #[test]
    fn extract_gvk_autoscaling_group() {
        let doc = json!({"apiVersion":"autoscaling/v2","kind":"HorizontalPodAutoscaler"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.group, "autoscaling");
    }

    #[test]
    fn read_doc_json_bool_root() {
        assert!(read_doc(Some("true")).unwrap().is_boolean());
    }

    #[test]
    fn matches_rejects_unrelated_kind() {
        let r = ar("", "v1", "Pod", "pods");
        assert!(!matches(&r, "service"));
    }

    #[test]
    fn emit_ndjson_number_scalar() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!(99)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "99\n");
    }

    #[test]
    fn extract_gvk_coordination_lease() {
        let doc = json!({"apiVersion":"coordination.k8s.io/v1","kind":"Lease"});
        let g = extract_gvk(&doc).unwrap();
        assert_eq!(g.kind, "Lease");
    }
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
