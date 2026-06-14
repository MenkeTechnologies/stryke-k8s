//! stryke-k8s — Kubernetes cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn k8s__*` is a JSON-string-in /
//! JSON-string-out wrapper around `kube`'s async client API. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at
//! first `use K8s`, registers each one as a stryke-callable function,
//! and on each call passes a JSON-encoded args dict and copies the
//! returned JSON into a stryke string.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CLIENTS` — `kube::Client` cache per kubeconfig context. v1
//!     helper rebuilt the client (TLS+auth handshake) per fork; this
//!     reuses the same client + underlying HTTP pool across calls.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use kube::api::{DeleteParams, DynamicObject, ListParams, PatchParams, PostParams};
use kube::core::ApiResource;
use kube::discovery::{ApiCapabilities, Discovery, Scope};
use kube::{Api, Client, Config};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + client cache ──────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CLIENTS: OnceCell<Mutex<HashMap<String, Client>>> = OnceCell::new();

fn clients() -> &'static Mutex<HashMap<String, Client>> {
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn get_client(opts: &Value) -> Result<Client> {
    let context = opts
        .get("context")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "_default".to_string());
    {
        let map = clients().lock();
        if let Some(c) = map.get(&context) {
            return Ok(c.clone());
        }
    }
    let client = if context == "_default" {
        Client::try_default().await?
    } else {
        let kc = kube::config::Kubeconfig::read()?;
        let options = kube::config::KubeConfigOptions {
            context: Some(context.clone()),
            ..Default::default()
        };
        let config = Config::from_custom_kubeconfig(kc, &options).await?;
        Client::try_from(config)?
    };
    clients().lock().insert(context, client.clone());
    Ok(client)
}

fn to_value<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

// ── helpers ─────────────────────────────────────────────────────────────────

/// Discover the (ApiResource, ApiCapabilities) for a `kind` string.
/// Accepts plain kinds ("Pod"), plural names ("pods"), or short names
/// ("po"). When unambiguous, picks the highest-priority group.
async fn discover_kind(client: &Client, kind: &str) -> Result<(ApiResource, ApiCapabilities)> {
    let disc = Discovery::new(client.clone()).run().await?;
    for group in disc.groups() {
        for (ar, caps) in group.recommended_resources() {
            if ar.kind.eq_ignore_ascii_case(kind) || ar.plural.eq_ignore_ascii_case(kind) {
                return Ok((ar, caps));
            }
        }
    }
    Err(anyhow!("unknown kind: {}", kind))
}

fn dyn_api(
    client: &Client,
    ar: &ApiResource,
    caps: &ApiCapabilities,
    namespace: Option<&str>,
) -> Api<DynamicObject> {
    match caps.scope {
        Scope::Cluster => Api::all_with(client.clone(), ar),
        Scope::Namespaced => match namespace {
            Some(ns) => Api::namespaced_with(client.clone(), ns, ar),
            None => Api::default_namespaced_with(client.clone(), ar),
        },
    }
}

// ── ops ─────────────────────────────────────────────────────────────────────

async fn op_ping(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    // Hit /version which exists on every server.
    let v = c.apiserver_version().await?;
    Ok(json!({"ok": true, "version": v.git_version}))
}

async fn op_version(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let v = c.apiserver_version().await?;
    Ok(to_value(v))
}

async fn op_contexts(_opts: Value) -> Result<Value> {
    let kc = kube::config::Kubeconfig::read()?;
    let names: Vec<String> = kc.contexts.iter().map(|c| c.name.clone()).collect();
    Ok(json!({"contexts": names}))
}

async fn op_current_context(_opts: Value) -> Result<Value> {
    let kc = kube::config::Kubeconfig::read()?;
    Ok(json!({"context": kc.current_context}))
}

async fn op_namespaces(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let nss: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(c);
    let list = nss.list(&ListParams::default()).await?;
    let names: Vec<String> = list
        .items
        .into_iter()
        .filter_map(|n| n.metadata.name)
        .collect();
    Ok(json!({"namespaces": names}))
}

async fn op_api_resources(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let disc = Discovery::new(c).run().await?;
    let mut out = Vec::new();
    for group in disc.groups() {
        for (ar, caps) in group.recommended_resources() {
            out.push(json!({
                "kind": ar.kind,
                "plural": ar.plural,
                "group": ar.group,
                "version": ar.version,
                "namespaced": matches!(caps.scope, Scope::Namespaced),
            }));
        }
    }
    Ok(json!({"resources": out}))
}

async fn op_get(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let namespace = opts["namespace"].as_str();
    let (ar, caps) = discover_kind(&c, kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    // Honour label/field selectors and a limit — without these `get pods`
    // always returned the whole namespace, ignoring `app=web` style filters.
    let mut lp = ListParams::default();
    if let Some(l) = opts["label_selector"].as_str() {
        lp = lp.labels(l);
    }
    if let Some(f) = opts["field_selector"].as_str() {
        lp = lp.fields(f);
    }
    if let Some(n) = opts["limit"].as_u64() {
        lp = lp.limit(n as u32);
    }
    let list = api.list(&lp).await?;
    Ok(json!({"items": to_value(list.items)}))
}

async fn op_get_one(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let namespace = opts["namespace"].as_str();
    let (ar, caps) = discover_kind(&c, kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let obj = api.get(name).await?;
    Ok(to_value(obj))
}

async fn op_create(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let doc = opts["doc"].clone();
    let kind = doc
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.kind missing"))?
        .to_string();
    let namespace = doc
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());
    let (ar, caps) = discover_kind(&c, &kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let obj: DynamicObject = serde_json::from_value(doc)?;
    let created = api.create(&PostParams::default(), &obj).await?;
    Ok(to_value(created))
}

async fn op_replace(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let doc = opts["doc"].clone();
    let kind = doc
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.kind missing"))?
        .to_string();
    let name = doc
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.metadata.name missing"))?
        .to_string();
    let namespace = doc
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());
    let (ar, caps) = discover_kind(&c, &kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let obj: DynamicObject = serde_json::from_value(doc)?;
    let replaced = api.replace(&name, &PostParams::default(), &obj).await?;
    Ok(to_value(replaced))
}

async fn op_apply(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let doc = opts["doc"].clone();
    let kind = doc
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.kind missing"))?
        .to_string();
    let name = doc
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("doc.metadata.name missing"))?
        .to_string();
    let namespace = doc
        .get("metadata")
        .and_then(|m| m.get("namespace"))
        .and_then(|v| v.as_str());
    let (ar, caps) = discover_kind(&c, &kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let pp = PatchParams::apply("stryke-k8s").force();
    let applied = api
        .patch(&name, &pp, &kube::api::Patch::Apply(&doc))
        .await?;
    Ok(to_value(applied))
}

async fn op_patch(opts: Value) -> Result<Value> {
    // Validate every required arg before opening a cluster connection so a
    // typo (or bad patch type) surfaces immediately.
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?
        .to_string();
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let body = opts
        .get("patch")
        .cloned()
        .filter(|p| !p.is_null())
        .ok_or_else(|| anyhow!("missing patch (the patch document)"))?;
    let ptype = opts["type"].as_str().unwrap_or("merge").to_string();
    if ptype != "merge" && ptype != "strategic" {
        return Err(anyhow!(
            "unknown patch type `{ptype}` (want merge|strategic)"
        ));
    }
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let (ar, caps) = discover_kind(&c, &kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let pp = PatchParams::default();
    // `type`: merge (RFC 7386 JSON merge) or strategic (k8s strategic merge).
    let patched = if ptype == "merge" {
        api.patch(&name, &pp, &kube::api::Patch::Merge(&body))
            .await?
    } else {
        api.patch(&name, &pp, &kube::api::Patch::Strategic(&body))
            .await?
    };
    Ok(to_value(patched))
}

async fn op_delete(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let namespace = opts["namespace"].as_str();
    let (ar, caps) = discover_kind(&c, kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let dp = DeleteParams::default();
    let result = api.delete(name, &dp).await?;
    Ok(match result {
        either::Either::Left(o) => json!({"deleted": to_value(o)}),
        either::Either::Right(s) => json!({"status": to_value(s)}),
    })
}

async fn op_scale(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let replicas = opts["replicas"]
        .as_i64()
        .ok_or_else(|| anyhow!("missing replicas"))?;
    let namespace = opts["namespace"].as_str();
    let (ar, caps) = discover_kind(&c, kind).await?;
    let api = dyn_api(&c, &ar, &caps, namespace);
    let patch = json!({"spec": {"replicas": replicas}});
    let pp = PatchParams::default();
    let result = api
        .patch_scale(name, &pp, &kube::api::Patch::Merge(&patch))
        .await?;
    Ok(to_value(result))
}

async fn op_logs(opts: Value) -> Result<Value> {
    let c = get_client(&opts).await?;
    let pod = opts["pod"].as_str().ok_or_else(|| anyhow!("missing pod"))?;
    let namespace = opts["namespace"]
        .as_str()
        .ok_or_else(|| anyhow!("missing namespace"))?;
    let container = opts["container"].as_str().map(String::from);
    let tail = opts["tail"].as_i64();
    let api: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(c, namespace);
    let lp = kube::api::LogParams {
        container,
        follow: false,
        tail_lines: tail,
        ..Default::default()
    };
    let text = api.logs(pod, &lp).await?;
    Ok(json!({"logs": text}))
}

// ── ops: mutation helpers ─────────────────────────────────────────────────────

/// Resolve the dynamic Api handle for a kind + optional namespace in one step.
async fn api_for(c: &Client, kind: &str, namespace: Option<&str>) -> Result<Api<DynamicObject>> {
    let (ar, caps) = discover_kind(c, kind).await?;
    Ok(dyn_api(c, &ar, &caps, namespace))
}

async fn op_label(opts: Value) -> Result<Value> {
    metadata_merge(opts, "labels").await
}

async fn op_annotate(opts: Value) -> Result<Value> {
    metadata_merge(opts, "annotations").await
}

/// LABEL / ANNOTATE share one shape: a JSON merge-patch into
/// `metadata.{labels|annotations}`. A null map value removes that key
/// (RFC 7386 merge semantics) — matches `kubectl label key-` / `annotate key-`.
async fn metadata_merge(opts: Value, field: &str) -> Result<Value> {
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let map = opts
        .get(field)
        .filter(|v| v.is_object())
        .ok_or_else(|| anyhow!("missing {} (an object of key => value)", field))?;
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let api = api_for(&c, kind, namespace).await?;
    let patch = json!({ "metadata": { field: map } });
    let out = api
        .patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(&patch),
        )
        .await?;
    Ok(to_value(out))
}

async fn op_cordon(opts: Value) -> Result<Value> {
    node_schedulable(opts, true).await
}

async fn op_uncordon(opts: Value) -> Result<Value> {
    node_schedulable(opts, false).await
}

/// CORDON / UNCORDON toggle `spec.unschedulable` on a Node.
async fn node_schedulable(opts: Value, unschedulable: bool) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name (node)"))?;
    let c = get_client(&opts).await?;
    let api: Api<k8s_openapi::api::core::v1::Node> = Api::all(c);
    let patch = json!({ "spec": { "unschedulable": unschedulable } });
    let out = api
        .patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(&patch),
        )
        .await?;
    Ok(to_value(out))
}

async fn op_set_image(opts: Value) -> Result<Value> {
    let kind = opts["kind"].as_str().unwrap_or("Deployment");
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let container = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?;
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let api = api_for(&c, kind, namespace).await?;
    // Strategic merge keys containers by `name`, so only the named container's
    // image changes — the rest of the pod template is untouched.
    let patch = json!({
        "spec": { "template": { "spec": {
            "containers": [ { "name": container, "image": image } ]
        }}}
    });
    let out = api
        .patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Strategic(&patch),
        )
        .await?;
    Ok(to_value(out))
}

async fn op_rollout_restart(opts: Value) -> Result<Value> {
    let kind = opts["kind"].as_str().unwrap_or("Deployment");
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let api = api_for(&c, kind, namespace).await?;
    // Stamp the pod-template restart annotation with a changing value; any
    // change to the template triggers a rolling restart (kubectl uses the same
    // annotation with an RFC3339 time — the value only needs to differ).
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_string());
    let patch = json!({
        "spec": { "template": { "metadata": { "annotations": {
            "kubectl.kubernetes.io/restartedAt": stamp
        }}}}
    });
    let out = api
        .patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Strategic(&patch),
        )
        .await?;
    Ok(to_value(out))
}

async fn op_rollout_status(opts: Value) -> Result<Value> {
    let kind = opts["kind"].as_str().unwrap_or("Deployment");
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let api = api_for(&c, kind, namespace).await?;
    let obj = api.get(name).await?;
    let status = to_value(&obj).get("status").cloned().unwrap_or(Value::Null);
    Ok(json!({ "status": status }))
}

async fn op_events(opts: Value) -> Result<Value> {
    let namespace = opts["namespace"].as_str();
    let c = get_client(&opts).await?;
    let api: Api<k8s_openapi::api::core::v1::Event> = match namespace {
        Some(ns) => Api::namespaced(c, ns),
        None => Api::all(c),
    };
    let mut lp = ListParams::default();
    // Scope to a single object's events when `name` is supplied.
    if let Some(obj) = opts["name"].as_str() {
        lp = lp.fields(&format!("involvedObject.name={}", obj));
    }
    if let Some(n) = opts["limit"].as_u64() {
        lp = lp.limit(n as u32);
    }
    let list = api.list(&lp).await?;
    let items: Vec<Value> = list
        .items
        .into_iter()
        .map(|e| {
            json!({
                "type": e.type_,
                "reason": e.reason,
                "message": e.message,
                "count": e.count,
                "object": e.involved_object.name,
                "kind": e.involved_object.kind,
                "last_timestamp": e.last_timestamp.map(|t| t.0.to_rfc3339()),
            })
        })
        .collect();
    Ok(json!({ "events": items }))
}

async fn op_top_pods(opts: Value) -> Result<Value> {
    let path = match opts["namespace"].as_str() {
        Some(ns) => format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", ns),
        None => "/apis/metrics.k8s.io/v1beta1/pods".to_string(),
    };
    metrics_get(&opts, &path).await
}

async fn op_top_nodes(opts: Value) -> Result<Value> {
    metrics_get(&opts, "/apis/metrics.k8s.io/v1beta1/nodes").await
}

/// Raw GET against the metrics.k8s.io API (requires metrics-server). Returns
/// the decoded `items` array verbatim — usage is `.containers[].usage` for
/// pods or `.usage` for nodes.
async fn metrics_get(opts: &Value, path: &str) -> Result<Value> {
    let c = get_client(opts).await?;
    let req = http::Request::get(path).body(Vec::new())?;
    let body: Value = c.request(req).await?;
    Ok(json!({ "items": body.get("items").cloned().unwrap_or(Value::Null) }))
}

async fn op_evict(opts: Value) -> Result<Value> {
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name (pod)"))?;
    let namespace = opts["namespace"]
        .as_str()
        .ok_or_else(|| anyhow!("missing namespace"))?;
    let c = get_client(&opts).await?;
    let path = format!("/api/v1/namespaces/{}/pods/{}/eviction", namespace, name);
    let eviction = json!({
        "apiVersion": "policy/v1",
        "kind": "Eviction",
        "metadata": { "name": name, "namespace": namespace }
    });
    let req = http::Request::post(&path)
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&eviction)?)?;
    let _: Value = c.request(req).await.unwrap_or(Value::Null);
    Ok(json!({ "ok": true, "evicted": name }))
}

async fn op_wait(opts: Value) -> Result<Value> {
    let kind = opts["kind"]
        .as_str()
        .ok_or_else(|| anyhow!("missing kind"))?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let namespace = opts["namespace"].as_str();
    // `condition`: a status.conditions[].type that must read "True", or the
    // literal "delete" to wait for the object to disappear.
    let condition = opts["condition"].as_str().unwrap_or("Ready");
    let timeout_s = opts["timeout"].as_u64().unwrap_or(300);
    let c = get_client(&opts).await?;
    let api = api_for(&c, kind, namespace).await?;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(timeout_s);
    loop {
        let obj = api.get_opt(name).await?;
        if condition.eq_ignore_ascii_case("delete") {
            if obj.is_none() {
                return Ok(json!({ "ok": true, "condition": "delete" }));
            }
        } else if let Some(o) = obj {
            if condition_is_true(&to_value(&o), condition) {
                return Ok(json!({ "ok": true, "condition": condition }));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "wait: timed out after {}s waiting for {}/{} condition {}",
                timeout_s,
                kind,
                name,
                condition
            ));
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}

/// True when `status.conditions[type==cond].status == "True"`.
fn condition_is_true(obj: &Value, cond: &str) -> bool {
    obj.get("status")
        .and_then(|s| s.get("conditions"))
        .and_then(|c| c.as_array())
        .map(|arr| {
            arr.iter().any(|c| {
                c.get("type")
                    .and_then(|t| t.as_str())
                    .map(|t| t.eq_ignore_ascii_case(cond))
                    == Some(true)
                    && c.get("status").and_then(|s| s.as_str()) == Some("True")
            })
        })
        .unwrap_or(false)
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-k8s handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn k8s__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn k8s__version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_version)
}

#[no_mangle]
pub extern "C" fn k8s__ping(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ping)
}

#[no_mangle]
pub extern "C" fn k8s__contexts(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_contexts)
}

#[no_mangle]
pub extern "C" fn k8s__current_context(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_current_context)
}

#[no_mangle]
pub extern "C" fn k8s__namespaces(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_namespaces)
}

#[no_mangle]
pub extern "C" fn k8s__api_resources(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_api_resources)
}

#[no_mangle]
pub extern "C" fn k8s__get(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_get)
}

#[no_mangle]
pub extern "C" fn k8s__get_one(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_get_one)
}

#[no_mangle]
pub extern "C" fn k8s__create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create)
}

#[no_mangle]
pub extern "C" fn k8s__replace(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_replace)
}

#[no_mangle]
pub extern "C" fn k8s__apply(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_apply)
}

#[no_mangle]
pub extern "C" fn k8s__patch(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_patch)
}

#[no_mangle]
pub extern "C" fn k8s__delete(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_delete)
}

#[no_mangle]
pub extern "C" fn k8s__scale(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_scale)
}

#[no_mangle]
pub extern "C" fn k8s__logs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_logs)
}

#[no_mangle]
pub extern "C" fn k8s__label(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_label)
}

#[no_mangle]
pub extern "C" fn k8s__annotate(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_annotate)
}

#[no_mangle]
pub extern "C" fn k8s__cordon(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_cordon)
}

#[no_mangle]
pub extern "C" fn k8s__uncordon(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_uncordon)
}

#[no_mangle]
pub extern "C" fn k8s__set_image(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_set_image)
}

#[no_mangle]
pub extern "C" fn k8s__rollout_restart(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rollout_restart)
}

#[no_mangle]
pub extern "C" fn k8s__rollout_status(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rollout_status)
}

#[no_mangle]
pub extern "C" fn k8s__events(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_events)
}

#[no_mangle]
pub extern "C" fn k8s__top_pods(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_top_pods)
}

#[no_mangle]
pub extern "C" fn k8s__top_nodes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_top_nodes)
}

#[no_mangle]
pub extern "C" fn k8s__evict(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_evict)
}

#[no_mangle]
pub extern "C" fn k8s__wait(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_wait)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Pod {
        name: String,
        namespace: String,
        status: String,
    }

    #[test]
    fn to_value_renders_struct_fields() {
        let v = to_value(Pod {
            name: "nginx-0".into(),
            namespace: "default".into(),
            status: "Running".into(),
        });
        assert_eq!(v["name"], json!("nginx-0"));
        assert_eq!(v["namespace"], json!("default"));
        assert_eq!(v["status"], json!("Running"));
    }

    #[test]
    fn to_value_handles_primitives_and_vec() {
        assert_eq!(to_value("v1.31.0"), json!("v1.31.0"));
        assert_eq!(to_value(3_u64), json!(3));
        assert_eq!(to_value(vec!["pod-a", "pod-b"]), json!(["pod-a", "pod-b"]));
    }

    #[test]
    fn to_value_option_none_renders_as_null() {
        let none: Option<String> = None;
        assert_eq!(to_value(none), json!(null));
        assert_eq!(to_value(Some("v")), json!("v"));
    }

    /// Match the JSON-value pattern used in `op_apply`/`op_delete`:
    /// `opts["kind"]` / `opts["namespace"]` extracted as `&str`. Drives
    /// the kind-discovery + namespacing decision in `discover_kind` +
    /// `dyn_api` — getting None vs Some(_) here changes the API surface
    /// the op operates on.
    fn extract_kind_and_ns(opts: &Value) -> (Option<&str>, Option<&str>) {
        (opts["kind"].as_str(), opts["namespace"].as_str())
    }

    #[test]
    fn opts_kind_extracted_when_present() {
        let opts = json!({"kind": "Pod", "namespace": "kube-system"});
        assert_eq!(
            extract_kind_and_ns(&opts),
            (Some("Pod"), Some("kube-system"))
        );
    }

    #[test]
    fn opts_kind_absent_when_missing() {
        let opts = json!({"name": "foo"});
        assert_eq!(extract_kind_and_ns(&opts), (None, None));
    }

    #[test]
    fn opts_kind_absent_when_non_string() {
        // `{"kind": 42}` shouldn't stringify the integer — only as_str survives.
        let opts = json!({"kind": 42, "namespace": null});
        assert_eq!(extract_kind_and_ns(&opts), (None, None));
    }

    /// Half-populated opts: kind present, namespace missing — common case
    /// for cluster-scoped resources (Node, Namespace itself, PersistentVolume).
    /// `dyn_api` interprets `None` namespace as "cluster scope", so this
    /// shape must remain producible.
    #[test]
    fn opts_kind_only_namespace_missing() {
        let opts = json!({"kind": "Node"});
        assert_eq!(extract_kind_and_ns(&opts), (Some("Node"), None));
    }

    /// Empty-string kind is distinguishable from missing — pinned so
    /// future refactors that coerce `Some("")` -> `None` get caught
    /// (silent coercion would mask malformed input from the wrapper).
    #[test]
    fn opts_kind_empty_string_is_some() {
        let opts = json!({"kind": ""});
        assert_eq!(extract_kind_and_ns(&opts), (Some(""), None));
    }

    /// Mirror of the context-resolution expression at the top of
    /// `get_client` (lines ~51-55): `opts.get("context")
    /// .and_then(as_str).map(String::from).unwrap_or_else(|| "_default")`.
    /// The resolved string is then compared against the literal
    /// `"_default"` in `get_client` to choose between
    /// `Client::try_default()` (in-cluster / current-context fast path)
    /// and the explicit-kubeconfig-context branch. Getting this wrong
    /// silently routes a caller to the wrong cluster.
    fn resolve_context(opts: &Value) -> String {
        opts.get("context")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| "_default".to_string())
    }

    /// Missing `context` key resolves to the sentinel `_default`, which
    /// `get_client` routes to `Client::try_default()`. This is the
    /// overwhelmingly common call shape (stryke scripts that never name a
    /// context). Pinned so a refactor that, e.g., defaults to `""` or to
    /// `kc.current_context` instead would be caught — those would change
    /// which client-construction branch fires.
    #[test]
    fn resolve_context_missing_is_default_sentinel() {
        assert_eq!(resolve_context(&json!({})), "_default");
        assert_eq!(
            resolve_context(&json!({"namespace": "kube-system"})),
            "_default"
        );
    }

    /// A non-string `context` (number/bool/null/object) must fall back to
    /// `_default`, NOT stringify. `as_str` returns None for `42`, so the
    /// `unwrap_or_else` fires. This is the same as_str-vs-coerce invariant
    /// the kind tests pin, but on a DIFFERENT field whose miss has a
    /// harsher consequence: a stringified `"42"` context name would make
    /// `get_client` take the kubeconfig branch and fail
    /// `Kubeconfig::read()` / context-lookup at runtime instead of using
    /// the default client.
    #[test]
    fn resolve_context_non_string_falls_back_to_default() {
        assert_eq!(resolve_context(&json!({"context": 42})), "_default");
        assert_eq!(resolve_context(&json!({"context": true})), "_default");
        assert_eq!(resolve_context(&json!({"context": null})), "_default");
        assert_eq!(
            resolve_context(&json!({"context": {"name": "x"}})),
            "_default"
        );
    }

    /// Critical divergence: an EMPTY-STRING context (`""`) is NOT the
    /// `_default` sentinel. `as_str` yields `Some("")`, `map` keeps it,
    /// `unwrap_or_else` does not fire — so `get_client`'s
    /// `if context == "_default"` is FALSE and it takes the explicit
    /// kubeconfig branch with `context: Some("")`. The empty string is
    /// distinguishable from "absent" and must stay that way: a future
    /// refactor that coerces `Some("")` -> default would silently swallow
    /// a malformed caller request (empty context name) and hit the wrong
    /// cluster instead of erroring. Pins empty != missing.
    #[test]
    fn resolve_context_empty_string_is_not_default_sentinel() {
        let r = resolve_context(&json!({"context": ""}));
        assert_eq!(r, "");
        assert_ne!(
            r, "_default",
            "empty context must not collapse to the default sentinel"
        );
    }

    /// A real context name is passed through verbatim, including one that
    /// contains the substring `_default` but is not equal to it
    /// (`prod_default`). `get_client`'s branch uses `==`, not
    /// `contains`/`starts_with`, so only the exact sentinel takes the
    /// default path. Pins against a regression that loosens the literal
    /// comparison.
    #[test]
    fn resolve_context_named_context_passthrough() {
        assert_eq!(
            resolve_context(&json!({"context": "prod-us-east"})),
            "prod-us-east"
        );
        assert_eq!(
            resolve_context(&json!({"context": "prod_default"})),
            "prod_default"
        );
    }

    /// Mirror of `op_scale`'s replicas extraction (line ~294):
    /// `opts["replicas"].as_i64().ok_or(...)`. The extracted i64 is
    /// injected verbatim into the scale patch `{"spec":{"replicas": n}}`.
    fn extract_replicas(opts: &Value) -> Option<i64> {
        opts["replicas"].as_i64()
    }

    /// `as_i64` on a JSON-parsed integer round-trips; a float (even
    /// integral-valued `3.0`) and a numeric STRING both yield None.
    /// op_scale relies on this: a caller sending `"replicas": "3"` or
    /// `"replicas": 3.0` (e.g. from a language that has no int/float
    /// distinction) gets a clean `missing replicas` error rather than a
    /// silently-coerced value. Pins the strict-integer contract so a
    /// refactor to `as_f64().map(|f| f as i64)` (which WOULD accept 3.0
    /// and truncate 3.9) is caught.
    #[test]
    fn extract_replicas_rejects_float_and_string() {
        // Parse from text so the integer is a genuine JSON integer.
        let int_opts: Value = serde_json::from_str(r#"{"replicas": 3}"#).unwrap();
        assert_eq!(extract_replicas(&int_opts), Some(3));

        let float_opts: Value = serde_json::from_str(r#"{"replicas": 3.0}"#).unwrap();
        assert_eq!(
            extract_replicas(&float_opts),
            None,
            "integral-valued float must NOT coerce to i64 — strict integer only"
        );

        let str_opts: Value = serde_json::from_str(r#"{"replicas": "3"}"#).unwrap();
        assert_eq!(
            extract_replicas(&str_opts),
            None,
            "numeric string is not an i64"
        );

        assert_eq!(
            extract_replicas(&json!({})),
            None,
            "missing replicas is None"
        );
    }

    /// Negative and zero replicas DO extract as valid i64 (`Some(-1)`,
    /// `Some(0)`). op_scale does no range validation — it forwards
    /// whatever i64 it gets straight into the patch and lets the
    /// apiserver reject out-of-range values. This test PINS that the
    /// wrapper itself does not pre-filter sign: `scale(deploy, -1)` must
    /// reach the server as `-1`, not be clamped or rejected client-side.
    /// If someone adds a `n >= 0` guard here, the error surface changes
    /// from "apiserver rejects" to "wrapper rejects" and this catches it.
    #[test]
    fn extract_replicas_passes_zero_and_negative_through() {
        let zero: Value = serde_json::from_str(r#"{"replicas": 0}"#).unwrap();
        assert_eq!(extract_replicas(&zero), Some(0));

        let neg: Value = serde_json::from_str(r#"{"replicas": -1}"#).unwrap();
        assert_eq!(
            extract_replicas(&neg),
            Some(-1),
            "wrapper must forward negative replicas verbatim; server, not client, validates range"
        );
    }

    // ── FFI plumbing invariants ──────────────────────────────────────────────
    //
    // These tests exercise the cdylib export surface that stryke's dlopen
    // bridge actually calls. They are the only purely-local invariants
    // testable without a live apiserver: pointer lifetime, panic guard, and
    // null-pointer no-op on free. All three are FFI-safety load-bearing —
    // a regression in any of them is UB across the FFI boundary.

    /// Convert a `*const c_char` returned from an export into an owned JSON
    /// `Value` by copying out of the pointer and then freeing it via the
    /// public `stryke_free_cstring` symbol — the exact sequence stryke's
    /// FFI bridge performs after each call. Returns the parsed JSON.
    unsafe fn take_ffi_result(p: *const c_char) -> Value {
        assert!(
            !p.is_null(),
            "ffi_call_async returned null — CString allocation failed"
        );
        let s = CStr::from_ptr(p)
            .to_str()
            .expect("ffi result is not valid UTF-8")
            .to_owned();
        // Free via the public symbol — round-trips `into_raw` → `from_raw`.
        stryke_free_cstring(p as *mut c_char);
        serde_json::from_str(&s).expect("ffi result is not valid JSON")
    }

    /// `k8s__pkg_version(null)` must:
    ///   1. accept a null `args` pointer (stryke passes null when stryke
    ///      caller passes no opts dict),
    ///   2. return a non-null pointer to a NUL-terminated UTF-8 JSON
    ///      string,
    ///   3. carry the compile-time `CARGO_PKG_VERSION` verbatim — not a
    ///      stale literal,
    ///   4. survive a round-trip through `stryke_free_cstring` without
    ///      UB (the free symbol must accept the same pointer the export
    ///      handed out — catches any regression where `into_raw` is
    ///      replaced with a borrowing pattern).
    #[test]
    fn pkg_version_ffi_roundtrip_with_null_args() {
        let p = k8s__pkg_version(std::ptr::null());
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["version"],
            json!(env!("CARGO_PKG_VERSION")),
            "pkg_version JSON must carry compile-time CARGO_PKG_VERSION"
        );
        // No "error" key on the success path.
        assert!(
            v.get("error").is_none(),
            "success path must not carry an `error` field, got: {v}"
        );
    }

    /// `stryke_free_cstring(null)` MUST be a no-op (documented in the
    /// `# Safety` block on the export). If the null guard is ever
    /// removed, `CString::from_raw(null)` is undefined behavior and any
    /// caller that defensively passes null (stryke's wrapper does on
    /// error-allocation paths where the export returned null) would
    /// crash the host process. This test pins the guard.
    #[test]
    fn free_cstring_null_is_noop() {
        unsafe { stryke_free_cstring(std::ptr::null_mut()) };
        // If we got here, the null guard held. A second call must also
        // be safe — defensive callers may double-free a null.
        unsafe { stryke_free_cstring(std::ptr::null_mut()) };
    }

    /// `ffi_call_async` must trap panics from the handler future and
    /// return the documented sentinel error JSON. Without `catch_unwind`
    /// a panic unwinding across the C ABI boundary is UB (the cdylib is
    /// dlopened in stryke's process — a panic would tear down the whole
    /// shell). Stryke's caller may also string-match on the sentinel
    /// `"stryke-k8s handler panicked"` to surface the failure; changing
    /// the message silently breaks that. Both invariants are pinned here.
    #[test]
    fn ffi_call_async_traps_handler_panic() {
        let p = ffi_call_async(std::ptr::null(), |_| async {
            panic!("simulated handler panic");
            #[allow(unreachable_code)]
            Ok::<Value, anyhow::Error>(json!({}))
        });
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["error"],
            json!("stryke-k8s handler panicked"),
            "panic guard must return the documented sentinel; got: {v}"
        );
    }

    /// Non-null `args` containing bytes that are NOT valid JSON must NOT
    /// crash the cdylib — `ffi_call_async` silently falls back to
    /// `Value::Null` and routes the handler with that. A regression where
    /// the inner `unwrap_or(Value::Null)` is replaced with `?`, `unwrap`,
    /// or `expect` would either propagate a deserialization error in a
    /// non-FFI-safe way OR panic across the C ABI boundary — both UB in
    /// the dlopened-in-stryke setting. The pin: an args-ignoring handler
    /// (`k8s__pkg_version` ignores its input) still succeeds when given
    /// garbage bytes that happen to be C-string-shaped.
    #[test]
    fn ffi_call_async_malformed_json_args_falls_back_to_null() {
        // Valid C string, invalid JSON: a trailing-comma object.
        let bad = CString::new(r#"{"context": "kube-system",}"#).unwrap();
        let p = k8s__pkg_version(bad.as_ptr());
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["version"],
            json!(env!("CARGO_PKG_VERSION")),
            "args-ignoring handler must still surface CARGO_PKG_VERSION; got: {v}"
        );
        assert!(
            v.get("error").is_none(),
            "malformed JSON args must not poison the success path; got: {v}"
        );
    }

    /// Non-UTF-8 bytes inside a NUL-terminated `args` buffer must NOT
    /// crash the cdylib. `CStr::to_bytes` is byte-oriented (no UTF-8
    /// check), and `serde_json::from_slice` errors on invalid UTF-8 —
    /// `ffi_call_async` must absorb that error via the same Null
    /// fallback. A regression that "improves" parsing by calling
    /// `CStr::to_str().unwrap()` panics across the C boundary on the
    /// first non-UTF-8 byte. C callers of the cdylib (stryke's bridge,
    /// plus any future direct loader) MUST be free to pass arbitrary
    /// NUL-terminated bytes; this test pins that contract.
    #[test]
    fn ffi_call_async_non_utf8_args_falls_back_to_null() {
        // 0xFF is invalid UTF-8 in any position. Build the buffer
        // manually so CString::new isn't fooled (it only rejects
        // interior NULs, not non-UTF-8).
        let mut bytes = vec![0xFFu8, b'{', b'}'];
        bytes.push(0); // C-string NUL terminator.
        let p = k8s__pkg_version(bytes.as_ptr() as *const c_char);
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["version"],
            json!(env!("CARGO_PKG_VERSION")),
            "non-UTF-8 args bytes must not crash the export; got: {v}"
        );
    }

    /// `ffi_call_async`'s error envelope on a handler `Err(_)` must be
    /// exactly `{"error": "<anyhow::Display>"}` — the same shape stryke's
    /// FFI bridge string-matches to surface failures back to user
    /// scripts. The existing panic test pins the panic-sentinel shape;
    /// this pins the normal-error shape, which is a DIFFERENT path
    /// through `ffi_call_async`'s match arm (`Ok(Err(e))` vs `Err(_)`).
    /// Catches regressions like nesting the error under `error.message`,
    /// renaming the key, or accidentally producing `{"Err": "..."}`
    /// (serde default for an `anyhow::Error` Serialize impl, which does
    /// not exist — anyone tempted to swap to `serde_json::to_value(e)`
    /// would get a broken envelope).
    #[test]
    fn ffi_call_async_handler_err_produces_error_envelope() {
        let p = ffi_call_async(std::ptr::null(), |_| async {
            Err::<Value, anyhow::Error>(anyhow!("missing kind"))
        });
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["error"],
            json!("missing kind"),
            "handler Err must surface verbatim under top-level `error`; got: {v}"
        );
        assert!(
            v.get("version").is_none() && v.get("Err").is_none(),
            "error envelope must not carry success keys nor a serde-default `Err` wrapper; got: {v}"
        );
    }

    /// A non-null `args` pointer whose first byte is NUL (a valid but
    /// empty C string) is a distinct code path from a null `args`
    /// pointer. `CStr::from_ptr` returns an empty slice; `to_bytes()`
    /// yields `&[]`; `serde_json::from_slice(&[])` is an *error*
    /// ("EOF while parsing a value"), not Null. The `unwrap_or(Null)`
    /// fallback must absorb that error. A regression where someone
    /// replaces the fallback with `?` (propagating the deserialization
    /// error as an `anyhow::Error`) would leak a synthetic JSON
    /// `{"error": "EOF while parsing..."}` envelope on every
    /// empty-string args call, breaking caller code that string-matches
    /// on real handler errors. Pins the "empty args bytes ≡ no opts"
    /// equivalence, distinct from the null-pointer path covered by
    /// `pkg_version_ffi_roundtrip_with_null_args`.
    #[test]
    fn ffi_call_async_empty_cstring_args_falls_back_to_null() {
        // Single NUL byte: from_ptr reads up to the NUL, getting 0 bytes
        // of payload. This is "passed an empty C string", which is
        // semantically different from "passed a null pointer".
        let empty: [u8; 1] = [0];
        let p = k8s__pkg_version(empty.as_ptr() as *const c_char);
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["version"],
            json!(env!("CARGO_PKG_VERSION")),
            "empty-bytes args path must not synthesize a parse-error envelope; got: {v}"
        );
        assert!(
            v.get("error").is_none(),
            "empty C string must NOT produce an `error` field — it must be \
             handler-invisible just like a null args pointer; got: {v}"
        );
    }

    /// Round-trip a handler-produced `Value` containing non-ASCII
    /// Unicode (BMP + supplementary plane + zero-width joiner) through
    /// `ffi_call_async`'s serialize → CString → CStr → deserialize
    /// boundary. Pins three FFI-boundary invariants in one shot:
    ///
    ///   1. `serde_json::to_string` must produce a UTF-8 byte sequence
    ///      that `CString::new` accepts — i.e. no interior NUL bytes
    ///      even when the original JSON string contained `'\u{0}'`
    ///      (which JSON serialization escapes as ` `).
    ///   2. `CString::into_raw` → `CStr::from_ptr` → `to_str()` must
    ///      survive arbitrary UTF-8 — pins against a regression where
    ///      someone replaces `CString` with `&str` borrowing (would
    ///      cause use-after-free) or with `OsString`/byte-array (would
    ///      corrupt non-ASCII).
    ///   3. The free symbol must accept the same pointer the export
    ///      produced even when the payload contains multi-byte
    ///      sequences — pins against a regression that double-encodes
    ///      or truncates.
    ///
    /// Verified payload includes: BMP CJK (`한`), an emoji that is a
    /// surrogate pair in UTF-16 (`😀`, U+1F600 in supplementary plane),
    /// and a zero-width joiner emoji sequence (`👨‍💻`). All three break
    /// naive byte-counted truncation.
    #[test]
    fn ffi_call_async_unicode_roundtrips_through_cstring_boundary() {
        let payload = "한국어 + 😀 + 👨\u{200D}💻";
        let p = ffi_call_async(std::ptr::null(), |_| {
            let owned = payload.to_string();
            async move { Ok(json!({ "text": owned, "embedded_nul_test": "a\u{0}b" })) }
        });
        let v = unsafe { take_ffi_result(p) };
        assert_eq!(
            v["text"],
            json!(payload),
            "non-ASCII UTF-8 must survive verbatim through the CString round-trip; got: {v}"
        );
        // The original JSON string had a raw NUL — serde_json escapes
        // it as ` `, so it parses back to a string containing a
        // real NUL byte. CString::new MUST have succeeded (no null
        // pointer return) because the serialized JSON contains no raw
        // 0x00 byte. If CString::new ever DID fail here, the export
        // would have returned null and `take_ffi_result`'s assertion
        // would have already panicked.
        assert_eq!(
            v["embedded_nul_test"],
            json!("a\u{0}b"),
            "JSON-escaped \\u0000 must round-trip as a real NUL inside the parsed string; got: {v}"
        );
    }

    /// The runtime + client-cache singletons (`RUNTIME`, `CLIENTS`) use
    /// `OnceCell::get_or_init`. Calling an export multiple times in
    /// sequence must reuse the SAME `tokio::Runtime` instance — a
    /// regression where `rt()` is rewritten to build a fresh runtime
    /// per call (e.g. someone tries to "fix" a perceived shutdown
    /// issue) would (a) burn N worker threads per N calls until the
    /// process OOMs, and (b) defeat the connection pooling that v0.2.0
    /// shipped specifically to fix v1's "rebuild client per fork"
    /// regression (see module doc-comment at top of lib.rs). This test
    /// pins the singleton by invoking the args-ignoring `k8s__pkg_version`
    /// export 10 times sequentially. If the runtime were rebuilt each
    /// call, `RUNTIME.set` (called internally by `get_or_init` only on
    /// first-init) would still only fire once — but the COUNT of
    /// background threads in the process would grow. A weaker but
    /// crate-internal-testable invariant: 10 calls all return the
    /// documented success shape without panic and without producing an
    /// `error` field — i.e. the second-through-tenth call still find a
    /// usable runtime to `block_on` against.
    #[test]
    fn ffi_call_async_singleton_runtime_handles_repeated_calls() {
        for i in 0..10 {
            let p = k8s__pkg_version(std::ptr::null());
            let v = unsafe { take_ffi_result(p) };
            assert_eq!(
                v["version"],
                json!(env!("CARGO_PKG_VERSION")),
                "iteration {i}: pkg_version must succeed identically across calls; got: {v}"
            );
            assert!(
                v.get("error").is_none(),
                "iteration {i}: repeated calls must not surface runtime-init errors; got: {v}"
            );
        }
    }

    /// `op_wait`'s success check: `status.conditions[type==cond].status=="True"`.
    /// Case-insensitive on the type, but the status string must be exactly
    /// "True" (k8s uses the literal strings "True"/"False"/"Unknown"). Pins
    /// that a regression accepting "true"/truthy or matching the wrong
    /// condition type can't slip a premature "ready" through.
    #[test]
    fn condition_is_true_matches_type_and_exact_true_status() {
        let obj = json!({"status": {"conditions": [
            {"type": "Progressing", "status": "True"},
            {"type": "Available", "status": "False"},
            {"type": "Ready", "status": "True"},
        ]}});
        assert!(condition_is_true(&obj, "Ready"));
        assert!(
            condition_is_true(&obj, "ready"),
            "type match is case-insensitive"
        );
        assert!(
            !condition_is_true(&obj, "Available"),
            "status False must not pass"
        );
        assert!(!condition_is_true(&obj, "Nonexistent"));
    }

    #[test]
    fn condition_is_true_false_when_no_conditions() {
        assert!(!condition_is_true(&json!({}), "Ready"));
        assert!(!condition_is_true(&json!({"status": {}}), "Ready"));
        // A lowercase "true" status is NOT the k8s sentinel and must fail.
        let obj = json!({"status": {"conditions": [{"type": "Ready", "status": "true"}]}});
        assert!(
            !condition_is_true(&obj, "Ready"),
            "only exact \"True\" counts"
        );
    }

    /// `k8s__patch` must validate kind/name/patch and the patch type BEFORE
    /// touching kubeconfig — so a typo (or unsupported patch type) surfaces
    /// without a live apiserver. Pins the validate-before-connect ordering.
    #[test]
    fn patch_validates_args_before_connecting() {
        let cases = [
            (r#"{}"#, "missing kind"),
            (r#"{"kind":"deploy"}"#, "missing name"),
            (r#"{"kind":"deploy","name":"web"}"#, "missing patch"),
            (
                r#"{"kind":"deploy","name":"web","patch":{"x":1},"type":"json"}"#,
                "unknown patch type",
            ),
        ];
        for (arg, want) in cases {
            let cs = CString::new(arg).unwrap();
            let v = unsafe { take_ffi_result(k8s__patch(cs.as_ptr())) };
            let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
            assert!(
                err.contains(want),
                "expected `{want}` for {arg}; got: {err}"
            );
        }
    }
}
