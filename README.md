```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ k 8 s ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-k8s/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-k8s/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[KUBERNETES CLIENT FOR STRYKE // GET + APPLY + DELETE + SCALE + ROLLOUT + LABEL + CORDON + EVICT + EVENTS + TOP + WAIT + LOGS]`

> *"Any kubeconfig-reachable cluster, no kubectl."*

Kubernetes client for stryke. Get / apply / delete / scale / logs /
watch / exec against any kubeconfig-reachable cluster (kind, k3s,
minikube, EKS, GKE, AKS, OpenShift, vanilla). Opt-in package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-docker`](https://github.com/MenkeTechnologies/stryke-docker) · [`stryke-aws`](https://github.com/MenkeTechnologies/stryke-aws) · [`stryke-gcp`](https://github.com/MenkeTechnologies/stryke-gcp) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] CLI: `k8s`](#0x02-cli-k8s)
- [\[0x03\] GVK shortcuts](#0x03-gvk-shortcuts)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] FFI layer](#0x05-ffi-layer)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-k8s
```

From a local checkout:

```sh
cd ~/projects/stryke-k8s
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use K8s`. A shared tokio
runtime + `kube::Client` cache keyed by kubeconfig context is held in
`OnceCell` for the life of the process — no fork-per-call, no fresh
TLS+auth handshake each time.

## [0x01] Quick start

```stryke
use K8s

# Connection: $KUBECONFIG / ~/.kube/config / in-cluster SA — no setup.
p K8s::current_context()
p K8s::version()->{gitVersion}

# List — `kind` accepts short forms or strict GVK.
my @pods   = K8s::get "pods",  namespace => "default"
my @deploy = K8s::get "apps/v1/Deployment", namespace => "kube-system"

# Get one.
my $pod = K8s::get_one "pod", "echo-7d9f", namespace => "default"

# Server-side apply (full doc; idempotent).
K8s::apply {
    apiVersion => "v1",
    kind       => "ConfigMap",
    metadata   => { name => "cfg", namespace => "ci" },
    data       => { hello => "world" },
}

K8s::apply {
    apiVersion => "apps/v1",
    kind       => "Deployment",
    metadata   => { name => "echo", namespace => "ci" },
    spec       => {
        replicas => 2,
        selector => { matchLabels => { app => "echo" } },
        template => {
            metadata => { labels => { app => "echo" } },
            spec     => { containers => [{ name => "main", image => "nginx:1.27" }] },
        },
    },
}

# Scale.
K8s::scale "deploy", "echo", 5, namespace => "ci"

# Logs (buffered).
my $text = K8s::logs "echo-7d9f", namespace => "ci", tail => 100

# Logs (streaming).
K8s::logs_follow "echo-7d9f",
    namespace => "ci",
    callback  => sub ($line) { p $line }

# Watch — NDJSON of `{type, object}` events, one per change.
K8s::watch "pods",
    namespace => "ci",
    callback  => sub ($evt) {
        return unless defined $evt->{object}
        p "$evt->{type} $evt->{object}{metadata}{name}"
    }

# Exec inside a container.
K8s::exec "echo-7d9f", ["sh", "-c", "uptime"],
    namespace => "ci",
    callback  => sub ($stream, $data) { print $data }

# Delete (cascading on Namespace).
K8s::delete_resource "namespace", "ci"
```

> `K8s::logs_follow`, `K8s::watch`, and `K8s::exec` are deferred in the
> v0.2.x cdylib — they die until the callback FFI ships (see
> [\[0x05\] FFI layer](#0x05-ffi-layer)).

Per-call connection overrides on every public fn:

```stryke
my %prod = (context => "prod-eks")
K8s::get "pods", namespace => "payments", %prod
```

## [0x02] CLI: `k8s`

```sh
k8s get pods --namespace=default
k8s get svc -A
k8s get apps/v1/Deployment --namespace=kube-system

k8s get-one pod echo-7d9f --namespace=default
k8s apply --doc='{"apiVersion":"v1","kind":"ConfigMap",...}'
k8s delete deploy echo --namespace=ci --force
k8s scale  deploy echo --replicas=5 --namespace=ci

k8s logs   echo-7d9f --namespace=ci --tail=100
k8s logs   echo-7d9f --namespace=ci --follow --timestamps
k8s watch  pods --namespace=ci --label-selector=app=echo
k8s exec   echo-7d9f --namespace=ci --cmd sh -- -c "uptime"

k8s version
k8s ping
k8s contexts
k8s current-context
k8s api-resources
k8s namespaces

k8s build           # cargo build --release
```

Global flags (also env vars):

```
--context CTX               $KUBE_CONTEXT       kubeconfig context to use
--kubeconfig PATH           $KUBECONFIG         explicit kubeconfig file
--default-namespace NS      $KUBE_NAMESPACE     default if --namespace omitted
```

## [0x03] GVK shortcuts

Anywhere a `kind` is accepted, the helper resolves the input against the
cluster's discovery API. All of the following work for Pods:

```
pod        pods        po        v1/Pod        /v1/Pod
```

For namespaced typed kinds you can use the strict triple:

```
apps/v1/Deployment            batch/v1/Job
networking.k8s.io/v1/Ingress  rbac.authorization.k8s.io/v1/Role
```

Custom resources work the same way once installed:

```
example.com/v1/Widget
```

## [0x04] API reference

### Read paths

```stryke
K8s::get           $kind, %opts → @objects     # opts: namespace, label_selector,
                                               # field_selector, limit
K8s::get_one       $kind, $name, %opts → \%doc | undef
K8s::watch         $kind, %opts → $count       # deferred in v0.2.x — dies
K8s::namespaces    %opts → @{ {name, status, labels} }
K8s::api_resources %opts → @{ {group, version, kind, plural, namespaced, verbs} }
```

`get` now honours `label_selector => "app=web"` and
`field_selector => "status.phase=Running"` (and `limit`); without them it
lists the whole namespace. Events for a resource are reachable as
`K8s::get "events", field_selector => "involvedObject.name=$name"`.

### Write paths

```stryke
K8s::apply             \%doc, %opts → \%applied   # server-side apply; namespace
                                                  # comes from doc.metadata
K8s::create            \%doc, %opts → \%created
K8s::replace           \%doc, %opts → \%replaced
K8s::patch             $kind, $name, \%patch, %opts → \%patched   # opts: type (merge|strategic), namespace
K8s::delete_resource   $kind, $name, %opts → \%result   # opts: namespace
K8s::scale             $kind, $name, $replicas, %opts → \%scale
```

`patch` does a JSON merge patch by default (`type => "strategic"` for a
strategic merge). The common rollout/label/scheduling operations have
dedicated wrappers below so callers don't hand-build patch documents.

### Rollouts + workload ops

```stryke
K8s::set_image        $name, $container, $image, %opts → \%obj  # opts: kind, namespace
K8s::rollout_restart  $name, %opts → \%obj          # opts: kind (default Deployment), namespace
K8s::rollout_status   $name, %opts → \%status       # replicas / readyReplicas / conditions
K8s::rollout_history  $name, %opts → @revisions     # owned ReplicaSets, newest first
K8s::autoscale        $target_name, $max, %opts → \%hpa  # create HPA; opts: min, cpu_percent, target_kind
K8s::taint            $node, $key, %opts → \%node       # opts: value, effect (default NoSchedule)
K8s::untaint          $node, $key, %opts → \%node       # remove taint by key
K8s::label            $kind, $name, \%labels, %opts → \%obj      # key => undef removes
K8s::annotate         $kind, $name, \%annotations, %opts → \%obj
```

### Pure helpers (no cluster)

```stryke
K8s::valid_name($name, %opts)   → { name, mode, valid, reason }   # opts: mode => subdomain|label
K8s::parse_selector($selector)  → @{ {key, op, value?, values?} }  # =, ==, !=, in, notin, exists
K8s::build_selector($reqs)      → $selector                       # \@{key,op,value?|values?} → label-selector; inverse of parse_selector
K8s::selector_matches(\%labels, $selector) → bool                 # does a label map satisfy the selector? (apimachinery AND semantics; absent key matches NotIn/NotEqual)
K8s::parse_resource_ref($ref)   → { kind, name }                  # kind/name
K8s::parse_quantity($qty)       → { quantity, number, suffix, value }  # 100Mi→bytes, 500m→0.5 cores
K8s::format_quantity($value, $suffix?) → { quantity, number, suffix, value }  # bytes→100Mi; inverse of parse_quantity
```

`parse_quantity` resolves a resource quantity to its base-unit `value`:
binary suffixes (`Ki`/`Mi`/`Gi`/`Ti`/`Pi`/`Ei`) are powers of 1024, decimal
suffixes (`n`/`u`/`m`/`k`/`M`/`G`/`T`/`P`/`E`) powers of 1000 — so `100Mi`
→ `104857600` bytes and `500m` → `0.5` cores.

### Nodes + eviction

```stryke
K8s::cordon    $name, %opts → \%node       # spec.unschedulable = true
K8s::uncordon  $name, %opts → \%node       # spec.unschedulable = false
K8s::evict     $name, %opts → \%result     # graceful pod eviction; namespace required
```

### Events + metrics + wait

```stryke
K8s::events    %opts → @events             # opts: namespace, name (one object), limit
K8s::top_pods  %opts → @podmetrics         # metrics.k8s.io; needs metrics-server
K8s::top_nodes %opts → @nodemetrics
K8s::wait      $kind, $name, %opts → \%ok  # opts: condition (default Ready, or "delete"),
                                           # timeout (s, default 300), namespace
```

### Logs + exec

```stryke
K8s::logs          $pod, %opts → $text       # opts: namespace, container, tail
K8s::logs_follow   $pod, %opts → $count      # deferred in v0.2.x — dies
K8s::exec          $pod, \@cmd, %opts → $count
                                             # deferred in v0.2.x — dies
```

### Connection + plumbing

```stryke
K8s::version          %opts → \%info        # gitVersion, platform, etc.
K8s::ping             %opts → 1 | ""
K8s::contexts         %opts → @{ {name, cluster, user, namespace, current} }
K8s::current_context  %opts → $name
K8s::pkg_version()    → $version_string    # cdylib's CARGO_PKG_VERSION
```

## [0x05] FFI layer

Each `K8s::*` wrapper builds a JSON args dict and calls a sibling
`k8s__*` symbol resolved out of `libstryke_k8s.{dylib,so}`. The cdylib
is dlopened in-process on first `use K8s` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover
version/discovery, get/list, write paths (create / replace / apply / delete
/ scale / patch), rollouts (set_image / rollout_restart / rollout_status /
label / annotate), node scheduling (cordon / uncordon / evict), events,
metrics (top_pods / top_nodes), wait, snapshot logs, plus cluster-free
helpers (valid_name / parse_selector / build_selector / selector_matches / parse_resource_ref / parse_quantity / format_quantity). The
authoritative list is `[ffi].exports` in `stryke.toml`.

**Persistent state:**

* `RUNTIME` — one shared `tokio` multi-thread runtime drives every
  async call.
* `CLIENTS` — `kube::Client` cache keyed by kubeconfig context. v1
  helper rebuilt the client (TLS+auth handshake) per fork; this reuses
  the same client + underlying HTTP pool across calls.

**Deferred from v0.2.1:** streaming-only ops (`watch`, `logs --follow`,
`exec`). These need a callback FFI shape that v1's `FfiSig::StrToStr`
doesn't model. Calling them dies with a clear message.

<details>
<summary>v1 wire shape (historical)</summary>

Output:

* `get`, `watch`, `logs --follow`, `namespaces`, `api-resources`, `contexts`,
  `exec` → NDJSON
* `get-one`, `apply`, `create`, `replace`, `delete`, `scale`, `version`,
  `ping`, `current-context` → single JSON
* `logs` (buffered) → raw text
* errors → stderr + non-zero exit

</details>

## [0x06] Tests

```sh
cargo test                                   # compiles, no live cluster
KUBECONFIG=~/.kube/config s test t/          # live round-trip
```

Tests use a unique `stryke-test-$$` namespace and tear it down at exit.

Local test cluster:

```sh
# kind
kind create cluster --name stryke
# or k3s in docker
docker run --rm --name k3s -p 6443:6443 \
    -v $PWD/k3s-data:/output rancher/k3s:latest \
    server --disable=traefik --tls-san=127.0.0.1
```

## [0x07] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x08] Layout

```
stryke-k8s/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/lib.rs                       # single-file cdylib
  lib/
    K8s.stk                        # `use K8s`
  t/
    test_k8s.stk                   # live round-trip
    test_stryke_k8s_surface.stk
  examples/
    get.stk
    apply.stk
    logs.stk
    cluster_info.stk
    discover.stk
  .github/workflows/
    ci.yml                         # kind cluster + live round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

| v1 (helper era) | v2+ |
|---|---|
| Generic dynamic resources via discovery | Typed wrappers for top-N kinds (zero-alloc Pod/Deployment/Service) |
| Server-side apply | `kubectl diff` equivalent (dry-run + server-side three-way) |
| Logs (buffered + streaming) | Port-forward (TCP tunnel) |
| Exec (stdout/stderr stream) | Stdin attach + interactive TTY |
| Watch via kube `watcher` | Informer-style cache with resync |
| kubeconfig + in-cluster SA | OIDC / EKS-token / GKE-gcloud exec plugins parity |

## [0xFF] License

MIT.
