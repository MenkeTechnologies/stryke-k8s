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

### `[KUBERNETES CLIENT FOR STRYKE // GET + APPLY + DELETE + SCALE + LOGS + WATCH + EXEC]`

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
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
- [\[0x06\] Tests](#0x06-tests)
- [\[0x07\] Dev workflow](#0x07-dev-workflow)
- [\[0x08\] Layout](#0x08-layout)
- [\[0x09\] Roadmap](#0x09-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Install

```sh
cd ~/projects/stryke-k8s
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

## [0x01] Quick start

```stryke
use K8s

# Connection: $KUBECONFIG / ~/.kube/config / in-cluster SA — no setup.
p K8s::current_context()
p K8s::version()->{gitVersion}

# List — `kind` accepts short forms or strict GVK.
my @pods   = K8s::get "pods",  namespace => "default"
my @svcs   = K8s::get "svc",   all_namespaces => 1
my @deploy = K8s::get "apps/v1/Deployment", namespace => "kube-system"

# Selectors, limit.
my @web = K8s::get "pods",
    namespace      => "prod",
    label_selector => "app=web,tier!=batch",
    field_selector => "status.phase=Running",
    limit          => 50

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

Per-call connection overrides on every public fn:

```stryke
my %prod = (context => "prod-eks", kubeconfig => "/etc/k8s/prod.yaml")
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
K8s::get           $kind, %opts → @objects     # namespace, all_namespaces,
                                               # label_selector, field_selector, limit
K8s::get_one       $kind, $name, %opts → \%doc | undef
K8s::watch         $kind, %opts → $count       # callback => sub ($evt) { … }
K8s::namespaces    %opts → @{ {name, status, labels} }
K8s::api_resources %opts → @{ {group, version, kind, plural, namespaced, verbs} }
```

### Write paths

```stryke
K8s::apply             \%doc, %opts → \%applied   # opts: field_manager, force, namespace
K8s::create            \%doc, %opts → \%created
K8s::replace           \%doc, %opts → \%replaced
K8s::delete_resource   $kind, $name, %opts → \%result   # opts: namespace, grace_period, force
K8s::scale             $kind, $name, $replicas, %opts → \%scale
```

### Logs + exec

```stryke
K8s::logs          $pod, %opts → $text       # opts: namespace, container, tail,
                                             # since_seconds, previous, timestamps
K8s::logs_follow   $pod, %opts → $count      # callback => sub ($line) { … }
K8s::exec          $pod, \@cmd, %opts → $count
                                             # callback => sub ($stream, $data) { … }
                                             # $stream is "stdout" or "stderr"
```

### Connection + plumbing

```stryke
K8s::version          %opts → \%info        # gitVersion, platform, etc.
K8s::ping             %opts → 1 | ""
K8s::contexts         %opts → @{ {name, cluster, user, namespace, current} }
K8s::current_context  %opts → $name
K8s::helper_path()    → $abs_path
K8s::ensure_built()   → $abs_path
```

## [0x05] Helper protocol

```sh
stryke-k8s-helper get pods --namespace=default
stryke-k8s-helper get-one pod echo-7d9f --namespace=default
stryke-k8s-helper apply --doc='{"apiVersion":"v1","kind":"ConfigMap",...}'
stryke-k8s-helper scale deploy echo --replicas=5 --namespace=ci
stryke-k8s-helper logs echo-7d9f --namespace=ci --follow
stryke-k8s-helper watch pods --namespace=ci
stryke-k8s-helper exec echo-7d9f --namespace=ci --cmd sh -- -c uptime
```

Output:

* `get`, `watch`, `logs --follow`, `namespaces`, `api-resources`, `contexts`,
  `exec` → NDJSON
* `get-one`, `apply`, `create`, `replace`, `delete`, `scale`, `version`,
  `ping`, `current-context` → single JSON
* `logs` (buffered) → raw text
* errors → stderr + non-zero exit

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
  src/main.rs                      # single-file helper
  lib/
    K8s.stk                        # `use K8s`
  bin/
    k8s.stk                        # `k8s` CLI
    k8s-build.stk
  t/
    test_k8s.stk                   # live round-trip
  examples/
    get.stk
    apply.stk
    logs.stk
  .github/workflows/
    ci.yml                         # kind cluster + live round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x09] Roadmap

| v1 (this release) | v2+ |
|---|---|
| Generic dynamic resources via discovery | Typed wrappers for top-N kinds (zero-alloc Pod/Deployment/Service) |
| Server-side apply | `kubectl diff` equivalent (dry-run + server-side three-way) |
| Logs (buffered + streaming) | Port-forward (TCP tunnel) |
| Exec (stdout/stderr stream) | Stdin attach + interactive TTY |
| Watch via kube `watcher` | Informer-style cache with resync |
| kubeconfig + in-cluster SA | OIDC / EKS-token / GKE-gcloud exec plugins parity |

## [0xFF] License

MIT.
