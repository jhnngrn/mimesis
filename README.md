# mimesis

A framework for building federated peer-to-peer applications in Go.

Mimesis provides bilateral sync, multi-tenant HTTP serving, ed25519 peer
authentication, argon2 user auth, file-based persistence and an identity
format (`user@domain`) — so an application only has to implement its own
logic as modules.

## Concepts

A **Server** hosts multiple nodes keyed by username, each identified as
`user@domain`. Nodes communicate via signed HTTP messages.

A **Link** is a bilateral sync primitive between two identities. Each side
holds mirrored state with a shared counter and a deterministic turn bit
— both sides can send simultaneously, but the turn bit resolves every
collision deterministically, so conflicts never need manual resolution.
Pending sends and cached responses are persisted, so nothing is lost if
a node crashes mid-sync.

A **Module** handles application-specific sync methods and account API
calls. Modules are registered as factories and instantiated per node.
Every module has `OnConnect`/`OnClose` lifecycle hooks. After a link
connects, all parentless modules (registered but not declared as a child
of any other) automatically begin connecting via `module.connect`. Child
modules are declared with `DeclareChildren` — they connect after their
parent is connected, forming a hierarchy.

## Usage

```go
package main

import (
    "encoding/json"
    "github.com/jhnngrn/mimesis"
)

type myModule struct{ /* ... */ }

func (m *myModule) OnConnect(peer string) {}
func (m *myModule) OnClose(peer string)   {}
func (m *myModule) HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error) {
    // handle incoming sync
    return true, nil, nil
}
func (m *myModule) HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string {
    return nil
}
func (m *myModule) HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error) {
    return nil, nil, mimesis.ErrBadMethod
}
func (m *myModule) NextTx(peer string) (string, json.RawMessage, int64) {
    return "", nil, 0
}
func (m *myModule) Status() json.RawMessage {
    return nil
}
func (m *myModule) Busy(peer string) bool {
    return false
}

func main() {
    srv, err := mimesis.Boot()
    if err != nil {
        panic(err)
    }
    srv.Register("myapp", func(name, nodeID string, node *mimesis.Node, schedule mimesis.ScheduleFunc, state json.RawMessage) mimesis.Module {
        return &myModule{}
    })
    srv.Register("helper", newHelper)
    srv.DeclareChildren("myapp", []string{"helper"}) // helper connects after myapp
    mimesis.RunAndWait(srv)
}
```

```
go run . -domain myapp.example.com -port 3000
```

## API

### Server

- `Boot()` — parse flags, create a server
- `NewServer(Config)` — create with explicit config
- `srv.Register(name, factory)` — register a module factory
- `srv.DeclareChildren(parent, children)` — declare child modules under a parent (children connect after parent)
- `srv.Init()` — load persisted nodes, set up HTTP routes
- `srv.Run(ctx)` / `srv.Serve(ctx, listener)` — start serving
- `RunAndWait(srv)` — init + serve + block until SIGINT/SIGTERM

### HTTP endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/p2p/tx` | Signed peer-to-peer sync |
| GET | `/p2p/key` | Public key for signature verification |
| POST | `/api/auth` | User login (argon2) |
| POST | `/api/admin` | User management |
| POST | `/api/account` | Module account API |

### Module interface

```go
type Module interface {
    OnConnect(peer string)
    OnClose(peer string)
    HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
    HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
    HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)
    NextTx(peer string) (string, json.RawMessage, int64)
    Status() json.RawMessage
    Busy(peer string) bool
}
```

`OnConnect` is called when `module.connect` completes bilaterally — on both
the initiating and receiving side. `OnClose` is called when `module.close`
completes bilaterally, or when the link itself closes. Every module gets
these lifecycle hooks to create and clean up per-peer state.

### Node

The `*Node` received by the module factory:

- `node.Get(peer) *Link` — get an existing link
- `node.Connect(peer) (*Link, error)` — create an outgoing link to peer
- `node.ModuleMap() map[string]Module` — access other module instances
- `node.ModuleAvailable(peer, module) bool` — true if module is bilaterally connected and not closing

### Module lifecycle

Modules implement `Busy(peer string) bool` to report whether they have
active work with a peer. The sync loop checks `Busy` before sending
`module.close` or `link.close`, deferring the close until the module is
idle. Because `Busy` reads bilateral state that only changes through
req/resp exchanges, the peer is always in the sync loop when the
transition happens — no polling or timers needed.

`SetModules` marks unwanted modules with `IntentClose`. If a module is
re-added via `SetModules` before the close message is sent, the opposing
`IntentConnect` cancels `IntentClose` in `nextTx`.

### Link

Methods available on a `*Link`:

- `lk.Close()` — mark the link for closing (deferred until no module is Busy)
- `lk.Closing() bool` — true if the link is closing
- `lk.ConnectModules(modules, registered)` — request connection for modules (additive)


### Account commands

Built-in account API commands handled by mimesis directly:

**link.\*** — link-level:
- `link.accept {peer}` — accept an incoming link
- `link.reject {peer}` — reject an incoming link
- `link.set_modules {peer, modules}` — propose modules on a link
- `link.set_invite_mode {mode}` — set invite mode (open/invite/closed)

**module.\*** — module-level:
- `module.accept {peer, module}` — accept an incoming module proposal
- `module.reject {peer, module}` — reject an incoming module proposal

### Identity

Identities follow the format `user@domain`, normalized to lowercase.

```go
user, domain, err := mimesis.ParseIdentity("alice@example.com")
```
