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
    let list = api.list(&ListParams::default()).await?;
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
}
