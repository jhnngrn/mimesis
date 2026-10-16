// Package mimesis is a bilateral sync framework for federated p2p applications.
// It provides the sync primitive, identity format, server infrastructure,
// authentication and persistence for building p2p apps as modules.
package mimesis

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Errors — protocol-level errors
// ---------------------------------------------------------------------------

var (
	ErrStaleCounter    = errors.New("stale counter")
	ErrCounterAhead    = errors.New("counter ahead")
	ErrCollision       = errors.New("collision")
	ErrRefsHeld        = errors.New("close with refs > 0")
	ErrNoLink          = errors.New("no link")
	ErrMaxLinks        = errors.New("max links")
	ErrInvitesDisabled = errors.New("invites disabled")
	ErrTimestamp       = errors.New("timestamp out of range")
	ErrBadMethod       = errors.New("unknown sync method")
	ErrNotFound        = errors.New("not found")
)

// MaxTimestampFuture is the maximum number of seconds a peer's
// timestamp may be ahead of our local clock.  Generous to tolerate
// clock drift; the main defence against exploitation is the backward
// check (peer timestamp must not go below previously stored value).
const MaxTimestampFuture int64 = 600 // 10 minutes

// ---------------------------------------------------------------------------
// Link — bilateral sync state
// ---------------------------------------------------------------------------

// ConnectionStatus — what the link IS.
type ConnectionStatus string

const (
	Waiting   ConnectionStatus = "waiting"   // proposed, awaiting peer decision
	Incoming  ConnectionStatus = "incoming"  // peer proposed, awaiting local decision
	Connected ConnectionStatus = "connected" // bilaterally established
)

// ConnectionState is the bilateral connection state: what IS (Status),
// what to DO next (IntentConnect/IntentClose), and who holds it open (Refs).
// Close is deferred while Refs > 0.
type ConnectionState struct {
	Status        ConnectionStatus `json:"status,omitempty"`
	IntentConnect bool             `json:"intent_connect,omitempty"`
	IntentClose   bool             `json:"intent_close,omitempty"`
	Refs          uint             `json:"refs,omitempty"`
}

// Ref increments the reference count.
func (c *ConnectionState) Ref()   { c.Refs++ }

// Unref decrements the reference count.
func (c *ConnectionState) Unref() { if c.Refs > 0 { c.Refs-- } }

// UnrefClose decrements refs and sets IntentClose when refs reach 0.
func (c *ConnectionState) UnrefClose() {
	if c.Refs > 0 {
		c.Refs--
	}
	if c.Refs == 0 {
		c.IntentClose = true
	}
}

// LinkState is a ConnectionState (for the link itself) plus per-module connections.
type LinkState struct {
	ConnectionState
	Modules map[string]*ConnectionState `json:"modules,omitempty"`
}

// Module processes sync methods and account API calls for a namespace.
// HandleRequest returns (ok, kicks, err) where kicks is a list of peers
// that need kicking on other links (cross-link side effects).
// HandleResponse returns kicks for the same purpose.
// HandleAccount handles user-facing API calls.
// NextTx returns the next queued message to send on a peer.
// Returns (method, payload, queuedAt). Empty method means nothing to send.
// queuedAt is the unix-second timestamp of when the message was first queued;
// when multiple modules return a candidate for the same peer, the oldest wins.
type Module interface {
	HandleRequest(peer, method string, payload json.RawMessage, ts int64) (bool, []string, error)
	HandleResponse(peer, method string, payload json.RawMessage, ok bool, ts int64) []string
	HandleAccount(method string, payload json.RawMessage) (json.RawMessage, []string, error)
	NextTx(peer string) (string, json.RawMessage, int64)
	Status() json.RawMessage
}

// QueuedMsg is a message queued for sending on a link.
// QueuedAt is a unix-second timestamp of when the message was queued.
// When multiple modules return a candidate for the same peer, the
// oldest QueuedAt wins. Zero means unset — unset always loses to any
// set value; when both are unset (or equal), registration order wins.
type QueuedMsg struct {
	Method   string          `json:"method"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	QueuedAt int64           `json:"queued_at,omitempty"`
}

// PeerSend tracks the outgoing send lifecycle for a link at a given counter.
// InFlight means a goroutine is blocked on send. After a failed send,
// a retry Timer is set that blocks kicks until it fires.
// Scoped to Counter — if counter advances via HandleIncoming, the entry
// is stale and ignored.
type PeerSend struct {
	Counter  uint64
	InFlight bool
	Timer    *time.Timer
}

// Link holds the sync state for a bilateral connection.
type Link struct {
	State          LinkState     `json:"state"`
	Counter        uint64        `json:"counter"`
	TurnBit        uint64        `json:"turn_bit"`
	Pending        *QueuedMsg    `json:"pending,omitempty"`
	CachedResponse *TxResponse `json:"cached_response,omitempty"`
	Timestamp      int64         `json:"ts"`
	Sending        *PeerSend     `json:"-"`
}

// ModuleEnabled returns true if the named module is bilaterally active on this link.
func (l *Link) ModuleEnabled(name string) bool {
	c := l.State.Modules[name]
	return c != nil && c.Status == Connected
}

// Dominant returns true when this side owns the current counter slot.
func (l *Link) Dominant() bool { return l.Counter%2 == l.TurnBit }

// NewLink creates a Link with the turn bit derived from identity ordering.
func NewLink(self, peer string) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{TurnBit: bit}
}

// ---------------------------------------------------------------------------
// Node — collection of links for a node, including the sync loop
// ---------------------------------------------------------------------------

const retryDelay = 60 * time.Second

// nowSecs returns the current time as unix seconds.
func nowSecs() int64 { return time.Now().Unix() }

// Node manages the set of bilateral links for a node.
type Node struct {
	mu         sync.Mutex
	m          map[string]*Link
	self       string
	InviteMode InviteMode
	Protected  map[string]struct{} // modules that SetModules cannot remove
	moduleMap  map[string]Module
	// send delivers a sync message to a peer and returns the response body.
	send   func(peer string, msg TxRequest) ([]byte, error)
	// commit persists the current state. Called under the mutex.
	commit func()
	timers  []*time.Timer
	stopped bool
}

// NewNode creates a Node for the given identity.
func NewNode(self string, m map[string]*Link, mode InviteMode, send func(string, TxRequest) ([]byte, error), commit func()) *Node {
	if m == nil {
		m = map[string]*Link{}
	}
	if mode == "" {
		mode = InviteModeInvite
	}
	return &Node{m: m, self: self, InviteMode: mode, Protected: map[string]struct{}{}, moduleMap: map[string]Module{}, send: send, commit: commit}
}

// Self returns the node identity.
func (n *Node) Self() string { return n.self }

// RegisterModule adds a module to the link collection under the given name.
func (n *Node) RegisterModule(name string, m Module) {
	n.moduleMap[name] = m
}

// ModuleMap returns the registered modules for serialization.
func (n *Node) ModuleMap() map[string]Module { return n.moduleMap }

func (n *Node) Get(peer string) *Link { return n.m[peer] }
func (n *Node) Len() int              { return len(n.m) }
func (n *Node) Full() bool            { return len(n.m) >= MaxLinks }
func (n *Node) Delete(peer string)    { delete(n.m, peer) }
func (n *Node) Map() map[string]*Link { return n.m }

// Create adds a new link to peer and returns it.
func (n *Node) Create(peer string) (*Link, error) {
	_, _, err := ParseIdentity(peer)
	if err != nil {
		return nil, err
	}
	peer = strings.ToLower(peer)
	lk := NewLink(n.self, peer)
	n.m[peer] = lk
	return lk, nil
}

// ---------------------------------------------------------------------------
// Sync lifecycle — request/response validation
// ---------------------------------------------------------------------------

// ValidateRequest checks whether an incoming request at the given counter
// is valid for this link. Returns:
//   - (*TxResponse, nil) — stale counter but cached response exists
//   - (nil, error) — invalid (stale without cache, ahead, or collision)
//   - (nil, nil) — valid, proceed with dispatch
func (l *Link) ValidateRequest(counter uint64) (*TxResponse, error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && l.CachedResponse.Counter == counter {
			resp := *l.CachedResponse
			return &resp, nil
		}
		return nil, ErrStaleCounter
	}
	if counter > l.Counter {
		return nil, ErrCounterAhead
	}
	if l.Pending != nil && l.Dominant() {
		return nil, ErrCollision
	}
	return nil, nil
}

// ValidateTimestamp checks that ts is within acceptable bounds:
//   - not before the link's latest timestamp (monotonicity)
//   - not more than MaxTimestampFuture seconds ahead of now
func (l *Link) ValidateTimestamp(ts, now int64) error {
	if ts < l.Timestamp {
		return ErrTimestamp
	}
	if ts > now+MaxTimestampFuture {
		return ErrTimestamp
	}
	return nil
}

// AcceptRequest finalizes an incoming request. Call after the handler
// has processed the request. Returns the response to send back.
func (l *Link) AcceptRequest(reqCounter uint64, ok bool, ts int64) *TxResponse {
	resp := TxResponse{Counter: reqCounter, OK: ok, Timestamp: ts}
	l.CachedResponse = &resp
	l.Counter++
	l.Timestamp = ts
	l.Pending = nil
	return &resp
}

// AcceptResponse finalizes a received response to our outgoing message.
// Call after the response handler has processed the response.
func (l *Link) AcceptResponse(ts int64) {
	l.Counter++
	l.Timestamp = ts
	l.Pending = nil
}


// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

// TxRequest is the wire envelope for a peer-to-peer transaction.
type TxRequest struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Method    string          `json:"method"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Resp      *TxResponse   `json:"resp,omitempty"`
}

// TxResponse is the wire envelope for a transaction response.
type TxResponse struct {
	Counter   uint64 `json:"counter"`
	OK        bool   `json:"ok"`
	Timestamp int64  `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// InviteMode — node-level policy for incoming connections/modules
// ---------------------------------------------------------------------------

type InviteMode string

const (
	InviteModeClosed InviteMode = "closed" // reject all incoming
	InviteModeInvite InviteMode = "invite" // incoming, manual accept
	InviteModeOpen   InviteMode = "open"   // auto-accept all
)

const MaxLinks = 64

func enc(v any) json.RawMessage { d, _ := json.Marshal(v); return d }

func splitMethod(method string) (ns, name string) {
	if i := strings.IndexByte(method, '.'); i >= 0 {
		return method[:i], method[i+1:]
	}
	return "", method
}

// OlderQueuedMsg reports whether a should win over b.
// Zero QueuedAt means unset — unset loses to any set value.
// Equal values (including both zero) return false, preserving
// the earlier candidate.
func OlderQueuedMsg(a, b *QueuedMsg) bool {
	if b == nil {
		return true
	}
	if a.QueuedAt == 0 && b.QueuedAt == 0 {
		return false
	}
	if a.QueuedAt == 0 {
		return false
	}
	if b.QueuedAt == 0 {
		return true
	}
	return a.QueuedAt < b.QueuedAt
}

// ---------------------------------------------------------------------------
// Link-level request/response handling
// ---------------------------------------------------------------------------

// dispatchRequest dispatches an incoming sync request by namespace.
// Link-level methods are handled inline; registered modules are
// dispatched by name. Returns (ok, kicks, error).
func (n *Node) dispatchRequest(peer, fullMethod string, payload json.RawMessage, ts int64) (bool, []string, error) {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
		l := n.Get(peer)
		if l == nil {
			return false, nil, fmt.Errorf("no link to %s", peer)
		}
		switch method {
		case "connect":
			if l.State.IntentConnect || l.State.Status == Waiting {
				l.State.Status = Connected
				l.State.IntentConnect = false
				return true, nil, nil
			}
			if n.InviteMode == InviteModeOpen {
				l.State.Status = Connected
				return true, nil, nil
			}
			return false, nil, nil
		case "close":
			if l.State.Refs > 0 {
				return false, nil, ErrRefsHeld
			}
			n.Delete(peer)
			return true, nil, nil
		case "module":
			ok, err := l.reqModule(payload, n.moduleMap, n.InviteMode)
			return ok, nil, err
		}
		return false, nil, fmt.Errorf("unknown link method %q", method)
	}
	lk := n.Get(peer)
	if lk == nil {
		return false, nil, fmt.Errorf("no link to %s", peer)
	}
	mod := n.moduleMap[ns]
	if mod == nil || !lk.ModuleEnabled(ns) {
		return false, nil, fmt.Errorf("unknown method %q", fullMethod)
	}
	return mod.HandleRequest(peer, method, payload, ts)
}

// dispatchResponse dispatches a sync response by namespace.
func (n *Node) dispatchResponse(peer, fullMethod string, payload json.RawMessage, ok bool, ts int64) []string {
	ns, method := splitMethod(fullMethod)
	if ns == "link" {
		l := n.Get(peer)
		if l == nil {
			return nil
		}
		switch method {
		case "connect":
			if ok {
				l.State.Status = Connected
			} else {
				l.State.Status = Waiting
			}
			l.State.IntentConnect = false
		case "close":
			if ok {
				n.Delete(peer)
			} else {
				l.State.IntentClose = false
			}
		case "module":
			l.respModule(payload, ok)
		}
		return nil
	}
	mod := n.moduleMap[ns]
	if mod == nil {
		return nil
	}
	return mod.HandleResponse(peer, method, payload, ok, ts)
}

// nextTx returns the next message to send on a peer link.
// Link-level messages (connect/close/module) have absolute priority.
// Among registered modules, the oldest QueuedAt wins.
func (n *Node) nextTx(peer string) *QueuedMsg {
	// Link-level messages have absolute priority.
	lk := n.Get(peer)
	if lk == nil {
		return nil
	}
	if lk.State.IntentClose && lk.State.Refs == 0 {
		return &QueuedMsg{Method: "link.close"}
	}
	if lk.State.IntentConnect {
		return &QueuedMsg{Method: "link.connect"}
	}
	if lk.State.Status == Connected {
		for name, mod := range lk.State.Modules {
			if mod.IntentConnect {
				return &QueuedMsg{
					Method:  "link.module",
					Payload: enc(map[string]string{"name": name}),
				}
			}
		}
	}
	if lk.State.Status != Connected {
		return nil
	}
	var best *QueuedMsg
	for name, mod := range n.moduleMap {
		if !lk.ModuleEnabled(name) {
			continue
		}
		method, payload, queuedAt := mod.NextTx(peer)
		if method != "" {
			msg := &QueuedMsg{
				Method:   name + "." + method,
				Payload:  payload,
				QueuedAt: queuedAt,
			}
			if OlderQueuedMsg(msg, best) {
				best = msg
			}
		}
	}
	return best
}

// ---------------------------------------------------------------------------
// Module negotiation (internal)
// ---------------------------------------------------------------------------

func (l *Link) reqModule(payload json.RawMessage, modules map[string]Module, inviteMode InviteMode) (bool, error) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil || p.Name == "" {
		return false, fmt.Errorf("bad payload")
	}
	if _, ok := modules[p.Name]; !ok {
		return false, nil // we don't speak this module
	}
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	mod := l.State.Modules[p.Name]
	// Already connected — idempotent ACK.
	if mod != nil && mod.Status == Connected {
		return true, nil
	}
	// Mutual — we also want this module.
	if mod != nil && (mod.IntentConnect || mod.Status == Waiting) {
		mod.Status = Connected
		mod.IntentConnect = false
		return true, nil
	}
	// Open mode — auto-accept supported modules.
	if inviteMode == InviteModeOpen {
		if mod == nil {
			mod = &ConnectionState{}
			l.State.Modules[p.Name] = mod
		}
		mod.Status = Connected
		return true, nil
	}
	// Mark as incoming — awaiting local accept/reject.
	if mod == nil {
		mod = &ConnectionState{}
		l.State.Modules[p.Name] = mod
	}
	mod.Status = Incoming
	return false, nil // NACK — not accepted yet
}

func (l *Link) respModule(payload json.RawMessage, ok bool) {
	var p struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(payload, &p) != nil {
		return
	}
	mod := l.State.Modules[p.Name]
	if mod == nil {
		return
	}
	if ok {
		mod.Status = Connected
	} else {
		mod.Status = Waiting
	}
	mod.IntentConnect = false
}

// ---------------------------------------------------------------------------
// Account operations
// ---------------------------------------------------------------------------

// HandleAccount dispatches an account API call by namespace.
// Handles locking, commit, and kicks internally — like HandleIncoming.
func (n *Node) HandleAccount(fullMethod string, payload json.RawMessage) (json.RawMessage, error) {
	n.mu.Lock()
	ns, method := splitMethod(fullMethod)

	var result json.RawMessage
	var kicks []string
	var err error

	switch {
	case ns == "link" && method == "set_invite_mode":
		var p struct {
			Mode InviteMode `json:"mode"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		switch p.Mode {
		case InviteModeClosed, InviteModeInvite, InviteModeOpen:
		default:
			n.mu.Unlock()
			return nil, fmt.Errorf("invalid mode %q", p.Mode)
		}
		n.InviteMode = p.Mode
		result = enc("ok")

	case ns == "link" && method == "set_modules":
		var p struct {
			Peer    string   `json:"peer"`
			Modules []string `json:"modules"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		lk.SetModules(p.Modules, n.moduleMap, n.Protected)
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "link" && method == "accept_module":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.AcceptModule(p.Module); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		result = enc("ok")
		kicks = []string{p.Peer}

	case ns == "link" && method == "reject_module":
		var p struct {
			Peer   string `json:"peer"`
			Module string `json:"module"`
		}
		if err = json.Unmarshal(payload, &p); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("bad request")
		}
		p.Peer = strings.ToLower(p.Peer)
		lk := n.Get(p.Peer)
		if lk == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no link to %s", p.Peer)
		}
		if err = lk.RejectModule(p.Module); err != nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("no incoming module %s from %s", p.Module, p.Peer)
		}
		result = enc("ok")

	case ns == "" && method == "status":
		result = n.Status()

	case ns == "link":
		n.mu.Unlock()
		return nil, fmt.Errorf("unknown link method %q", method)

	default:
		mod := n.moduleMap[ns]
		if mod == nil {
			n.mu.Unlock()
			return nil, fmt.Errorf("unknown method %q", fullMethod)
		}
		var modKicks []string
		result, modKicks, err = mod.HandleAccount(method, payload)
		kicks = append(kicks, modKicks...)
	}

	if err == nil {
		n.commit()
	}
	n.mu.Unlock()
	for _, k := range kicks {
		n.Kick(k)
	}
	return result, err
}

// Status returns the link collection status as JSON, including module status.
func (n *Node) Status() json.RawMessage {
	links := make(map[string]any)
	for peer, l := range n.m {
		links[peer] = map[string]any{
			"state": l.State,
		}
	}
	result := map[string]any{
		"links":       links,
		"invite_mode": n.InviteMode,
	}
	for name, mod := range n.moduleMap {
		if extra := mod.Status(); extra != nil {
			var m map[string]any
			if json.Unmarshal(extra, &m) == nil {
				result[name] = m
			}
		}
	}
	return enc(result)
}

// SetModules proposes the given modules on this link, filtering by supported.
// Modules not in the desired set and not yet connected are removed,
// unless they are in the protected set.
func (l *Link) SetModules(modules []string, registered map[string]Module, protected map[string]struct{}) {
	if l.State.Modules == nil {
		l.State.Modules = map[string]*ConnectionState{}
	}
	desired := map[string]bool{}
	for _, name := range modules {
		if _, ok := registered[name]; ok {
			desired[name] = true
		}
	}
	for name := range desired {
		mod := l.State.Modules[name]
		if mod == nil {
			l.State.Modules[name] = &ConnectionState{IntentConnect: true}
		} else if mod.Status != Connected {
			mod.IntentConnect = true
		}
	}
	for name, mod := range l.State.Modules {
		if _, prot := protected[name]; prot {
			continue
		}
		if !desired[name] && mod.Status != Connected {
			delete(l.State.Modules, name)
		}
	}
}

// AcceptModule accepts an incoming module proposal.
func (l *Link) AcceptModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	mod.IntentConnect = true
	return nil
}

// RejectModule rejects an incoming module proposal.
func (l *Link) RejectModule(name string) error {
	mod := l.State.Modules[name]
	if mod == nil || mod.Status != Incoming {
		return fmt.Errorf("no incoming module %s", name)
	}
	delete(l.State.Modules, name)
	return nil
}

// ---------------------------------------------------------------------------
// Sync loop — incoming
// ---------------------------------------------------------------------------

// QuickReject returns true if this request can be rejected before
// expensive signature verification (no existing link and not a connect/close).
func (n *Node) QuickReject(from, method string) bool {
	from = strings.ToLower(from)
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.Get(from) == nil && method != "link.connect" && method != "link.close"
}

// HandleIncoming processes an incoming transaction. The caller is responsible
// for authentication (signature verification).
func (n *Node) HandleIncoming(env TxRequest) (*TxResponse, error) {
	env.From = strings.ToLower(env.From)
	n.mu.Lock()

	lk := n.Get(env.From)
	if lk == nil {
		if env.Method == "link.close" {
			n.mu.Unlock()
			return &TxResponse{Counter: env.Counter, OK: true, Timestamp: nowSecs()}, nil
		}
		if env.Method != "link.connect" {
			n.mu.Unlock()
			return nil, ErrNoLink
		}
		if n.InviteMode == InviteModeClosed {
			n.mu.Unlock()
			return nil, ErrInvitesDisabled
		}
		if n.Full() {
			n.mu.Unlock()
			return nil, ErrMaxLinks
		}
		var err error
		lk, err = n.Create(env.From)
		if err != nil {
			n.mu.Unlock()
			return nil, err
		}
		lk.State.Status = Incoming
	}

	now := nowSecs()

	// Piggybacked response.
	if env.Resp != nil && lk.Pending != nil && env.Resp.Counter == lk.Counter {
		if err := lk.ValidateTimestamp(env.Resp.Timestamp, now); err != nil {
			n.mu.Unlock()
			return nil, err
		}
		pbKicks := n.dispatchResponse(env.From, lk.Pending.Method, lk.Pending.Payload, env.Resp.OK, env.Resp.Timestamp)
		lk.AcceptResponse(env.Resp.Timestamp)
		n.commit()
		for _, k := range pbKicks {
			n.Kick(k)
		}
	}

	// Validate counter and collision.
	if cached, err := lk.ValidateRequest(env.Counter); err != nil {
		n.mu.Unlock()
		return nil, err
	} else if cached != nil {
		n.mu.Unlock()
		return cached, nil
	}

	ts := env.Timestamp
	if lk.Dominant() {
		ts = now
		if ts < lk.Timestamp {
			ts = lk.Timestamp
		}
	}

	// Validate peer timestamp (monotonicity + forward cap).
	if err := lk.ValidateTimestamp(ts, now); err != nil {
		n.mu.Unlock()
		return nil, err
	}

	// Dispatch to handler.
	ok, kicks, reqErr := n.dispatchRequest(env.From, env.Method, env.Payload, ts)
	if reqErr != nil {
		n.mu.Unlock()
		return nil, reqErr
	}

	resp := lk.AcceptRequest(env.Counter, ok, ts)
	n.commit()
	n.mu.Unlock()
	n.Kick(env.From)
	for _, k := range kicks {
		n.Kick(k)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Sync loop — outgoing
// ---------------------------------------------------------------------------

// Kick triggers a sendPending for the given peer.
func (n *Node) Kick(peer string) { go n.sendPending(peer) }

// KickAll kicks all peers. Used at boot to resume pending work.
func (n *Node) KickAll() {
	n.mu.Lock()
	peers := make([]string, 0, n.Len())
	for peer := range n.m {
		peers = append(peers, peer)
	}
	n.mu.Unlock()
	for _, p := range peers {
		n.Kick(p)
	}
}

// Schedule runs fn after delay under the Node mutex.
// fn returns a list of peers to kick. Node commits and kicks.
// The timer is tracked so Stop() can cancel it.
//
// Schedule is called from two contexts: during init (no mutex held)
// and from within a callback (mutex held). The timer append is safe
// in both cases — init has no concurrency, callbacks hold the mutex.
func (n *Node) Schedule(delay time.Duration, fn func() []string) {
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		kicks := fn()
		n.commit()
		n.mu.Unlock()
		for _, k := range kicks {
			n.Kick(k)
		}
	})
	// No mutex lock here — called under mutex (callback) or during
	// init (no concurrency). Locking would deadlock in the callback case.
	n.timers = append(n.timers, t)
}

// Stop cancels all scheduled and retry timers and marks the Node as
// stopped. After Stop, Schedule callbacks and sendPending are no-ops.
// Must be called when deleting a node to prevent zombie timers from
// re-persisting the node's state.
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	for _, t := range n.timers {
		t.Stop()
	}
	n.timers = nil
	for _, lk := range n.m {
		if lk.Sending != nil && lk.Sending.Timer != nil {
			lk.Sending.Timer.Stop()
			lk.Sending = nil
		}
	}
	n.mu.Unlock()
}

func (n *Node) sendPending(peer string) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	lk := n.Get(peer)
	if lk == nil {
		n.mu.Unlock()
		return
	}
	if s := lk.Sending; s != nil {
		if s.Counter == lk.Counter && (s.InFlight || s.Timer != nil) {
			n.mu.Unlock()
			return
		}
		if s.Timer != nil {
			s.Timer.Stop()
		}
		lk.Sending = nil
	}
	promoted := false
	if lk.Pending == nil {
		lk.Pending = n.nextTx(peer)
		if lk.Pending == nil {
			n.mu.Unlock()
			return
		}
		promoted = true
	}
	counter := lk.Counter
	lk.Sending = &PeerSend{Counter: counter, InFlight: true}
	msg := lk.Pending
	env := TxRequest{
		From: n.self, To: peer, Counter: counter,
		Method: msg.Method, Payload: msg.Payload,
	}
	if lk.Dominant() {
		env.Timestamp = nowSecs()
		if env.Timestamp < lk.Timestamp {
			env.Timestamp = lk.Timestamp
		}
	}
	if cr := lk.CachedResponse; cr != nil {
		env.Resp = cr
	}
	if promoted {
		n.commit()
	}
	n.mu.Unlock()

	body, err := n.send(peer, env)

	n.mu.Lock()
	if err != nil {
		log.Printf("[link] %s->%s %s: %v", n.self, peer, msg.Method, err)
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}
	var resp TxResponse
	if json.Unmarshal(body, &resp) != nil {
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}
	lk = n.Get(peer)
	if lk == nil || lk.Pending != msg || lk.Counter != counter || resp.Counter != counter {
		if lk != nil {
			lk.Sending = nil
		}
		n.mu.Unlock()
		return
	}

	if err := lk.ValidateTimestamp(resp.Timestamp, nowSecs()); err != nil {
		log.Printf("[link] %s->%s: bad response timestamp: %v", n.self, peer, err)
		n.setRetry(peer, counter)
		n.mu.Unlock()
		return
	}

	extraKicks := n.dispatchResponse(peer, msg.Method, msg.Payload, resp.OK, resp.Timestamp)
	lk.AcceptResponse(resp.Timestamp)

	var kickPeer bool
	if n.Get(peer) != nil {
		lk.Pending = n.nextTx(peer)
		kickPeer = lk.Pending != nil
	}
	n.commit()
	lk.Sending = nil
	n.mu.Unlock()
	if kickPeer {
		n.Kick(peer)
	}
	for _, k := range extraKicks {
		n.Kick(k)
	}
}

// setRetry sets a per-peer retry timer. Caller must hold Lock.
func (n *Node) setRetry(peer string, counter uint64) {
	if n.stopped {
		return
	}
	lk := n.Get(peer)
	if lk == nil {
		return
	}
	if lk.Sending != nil && lk.Sending.Timer != nil {
		lk.Sending.Timer.Stop()
	}
	ps := &PeerSend{
		Counter:    counter,
	}
	ps.Timer = time.AfterFunc(retryDelay, func() {
		n.mu.Lock()
		if lk := n.Get(peer); lk != nil && lk.Sending == ps {
			lk.Sending = nil
		}
		n.mu.Unlock()
		n.sendPending(peer)
	})
	lk.Sending = ps
}
