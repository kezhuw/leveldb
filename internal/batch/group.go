package batch

type Group struct {
	Sync          bool
	Batch         *Batch
	replys        []chan error
	first         Batch
	fixed         Batch
	pendingSync   bool
	pendingBatch  []byte
	pendingReplyc chan error
	batchSize     int
	maxBatchSize  int
}

func (g *Group) Empty() bool {
	return len(g.replys) == 0
}

func (g *Group) setFirst(sync bool, batch []byte, replyc chan error) {
	g.Sync = sync
	g.first.Reset(batch)
	g.replys = append(g.replys, replyc)
	g.Batch = &g.first
	g.batchSize = len(batch)
	switch {
	case g.batchSize <= (128 << 10):
		g.maxBatchSize = g.batchSize + (128 << 10)
	default:
		g.maxBatchSize = 1 << 20
	}
}

func (g *Group) HasPending() bool {
	return g.pendingReplyc != nil
}

func (g *Group) Push(sync bool, batch []byte, replyc chan error) {
	switch {
	case len(g.replys) == 0:
		g.setFirst(sync, batch, replyc)
	case !g.Sync && sync:
		fallthrough
	case g.batchSize > g.maxBatchSize:
		g.pendingSync = sync
		g.pendingBatch = batch
		g.pendingReplyc = replyc
	default:
		if g.fixed.Empty() {
			g.fixed.Append(g.first.Bytes())
			g.Batch = &g.fixed
		}
		g.fixed.Append(batch)
		g.replys = append(g.replys, replyc)
	}
}

func (g *Group) Rewind() {
	g.fixed.Clear()
	g.first.Reset(nil)
	g.replys = g.replys[:0]
	if g.pendingReplyc != nil {
		g.setFirst(g.pendingSync, g.pendingBatch, g.pendingReplyc)
		g.pendingBatch = nil
		g.pendingReplyc = nil
	}
}

func (g *Group) Send(err error) {
	for i, replyc := range g.replys {
		replyc <- err
		g.replys[i] = nil
	}
}
