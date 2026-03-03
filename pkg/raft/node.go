package raft

import "context"

// Node represents a node in a raft cluster. It is the primary interface
// for the application to interact with the raft state machine.
type Node interface {
	// Tick increments the internal logical clock for the Node by a single tick.
	// Election timeouts and heartbeat timeouts are measured in ticks.
	Tick()

	// Campaign causes the Node to transition to candidate state and start
	// campaigning to become leader.
	Campaign(ctx context.Context) error

	// Propose proposes that data be appended to the log. Proposals can be
	// lost without notice, therefore it is user's job to ensure proposal
	// retries.
	Propose(ctx context.Context, data []byte) error

	// Step advances the state machine using the given message. ctx.Err()
	// will be returned, if any.
	Step(ctx context.Context, msg Message) error

	// Ready returns a channel that receives Ready structs when the Node has
	// state to process.
	Ready() <-chan Ready

	// Advance notifies the Node that the application has saved progress
	// up to the last Ready. It prepares the node to return the next
	// available Ready.
	Advance()

	// ReadIndex requests a read state. The read state will be set in the
	// Ready. Read state has a read index.
	ReadIndex(ctx context.Context, rctx []byte) error

	// Status returns the current status of the raft state machine.
	Status() Status

	// ReportUnreachable reports the given node is not reachable for the
	// last send.
	ReportUnreachable(id uint64)

	// ReportSnapshot reports the status of the sent snapshot.
	ReportSnapshot(id uint64, status SnapshotStatus)

	// Stop performs any necessary termination of the Node.
	Stop()
}

type msgWithResult struct {
	m      Message
	result chan error
}

// node implements the Node interface. It multiplexes raft state machine
// operations through channels and runs a select loop in a goroutine.
type node struct {
	propc    chan msgWithResult
	recvc    chan Message
	readyc   chan Ready
	advancec chan struct{}
	tickc    chan struct{}
	done     chan struct{}
	stop     chan struct{}
	status   chan chan Status

	rn *RawNode
}

func newNode(rn *RawNode) node {
	return node{
		propc:    make(chan msgWithResult),
		recvc:    make(chan Message),
		readyc:   make(chan Ready),
		advancec: make(chan struct{}),
		tickc:    make(chan struct{}, 128),
		done:     make(chan struct{}),
		stop:     make(chan struct{}),
		status:   make(chan chan Status),
		rn:       rn,
	}
}

// run is the main event loop for the node. It multiplexes all inputs
// (proposals, messages, ticks) and outputs (Ready) through channels.
func (n *node) run() {
	var (
		propc      chan msgWithResult
		readyc     chan Ready
		advancec   chan struct{}
		rd         Ready
		hasReady   bool
	)

	r := n.rn.raft

	lead := None

	for {
		if advancec != nil {
			// We're waiting for the application to call Advance.
			readyc = nil
		} else if hasReady {
			// Ready was already computed; arm the channel.
			readyc = n.readyc
		} else if n.rn.HasReady() {
			// Compute Ready only once per cycle.
			rd = n.rn.Ready()
			readyc = n.readyc
			hasReady = true
		} else {
			readyc = nil
		}

		// Only accept proposals if we know there's a leader.
		if lead != r.lead {
			if r.hasLeader() {
				propc = n.propc
			} else {
				propc = nil
			}
			lead = r.lead
		}

		select {
		case pm := <-propc:
			m := pm.m
			m.From = r.id
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:
			_ = r.Step(m)
		case <-n.tickc:
			n.rn.Tick()
		case readyc <- rd:
			advancec = n.advancec
		case <-advancec:
			n.rn.Advance(rd)
			rd = Ready{}
			advancec = nil
			hasReady = false
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

// Tick implements the Node interface.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		// Tick channel is full; a tick was dropped.
	}
}

// Campaign implements the Node interface.
func (n *node) Campaign(ctx context.Context) error {
	return n.step(ctx, Message{Type: MsgHup})
}

// Propose implements the Node interface.
func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, Message{
		Type:    MsgProp,
		Entries: []Entry{{Data: data}},
	})
}

// Step implements the Node interface.
func (n *node) Step(ctx context.Context, m Message) error {
	// Ignore unexpected local messages.
	if IsLocalMsg(m.Type) {
		return nil
	}
	return n.step(ctx, m)
}

func (n *node) step(ctx context.Context, m Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *node) stepWait(ctx context.Context, m Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

func (n *node) stepWithWaitOption(ctx context.Context, m Message, wait bool) error {
	if m.Type != MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}

	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}

	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

// Ready implements the Node interface.
func (n *node) Ready() <-chan Ready {
	return n.readyc
}

// Advance implements the Node interface.
func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

// ReadIndex implements the Node interface.
func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, Message{
		Type:    MsgReadIndex,
		Entries: []Entry{{Data: rctx}},
	})
}

// Status implements the Node interface.
func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

// ReportUnreachable implements the Node interface.
func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- Message{Type: MsgUnreachable, From: id}:
	case <-n.done:
	}
}

// ReportSnapshot implements the Node interface.
func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure
	select {
	case n.recvc <- Message{Type: MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

// Stop implements the Node interface.
func (n *node) Stop() {
	select {
	case n.stop <- struct{}{}:
		// Not already stopped, so trigger it.
	case <-n.done:
		// Node has already been stopped - no need to do anything.
		return
	}
	// Block until the stop has been acknowledged by run().
	<-n.done
}

// ErrStopped is returned by methods after a Node has been stopped.
var ErrStopped = errorf("raft: stopped")

// StartNode returns a new Node given configuration and a list of raft peers.
// It appends a ConfChange entry for each given peer to the initial log.
func StartNode(c *Config, peers []Peer) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}
	if err := rn.Bootstrap(peers); err != nil {
		panic(err)
	}

	n := newNode(rn)
	go n.run()
	return &n
}

// RestartNode returns a new Node restored from previous state saved in
// Storage. It does NOT call Bootstrap; the caller must ensure that Storage
// already contains the state from a previous run.
func RestartNode(c *Config) Node {
	rn, err := NewRawNode(c)
	if err != nil {
		panic(err)
	}

	n := newNode(rn)
	go n.run()
	return &n
}
