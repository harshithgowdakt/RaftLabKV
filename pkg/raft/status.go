package raft

// Status contains information about this Raft peer and its view of the system.
type Status struct {
	ID uint64

	HardState
	SoftState

	Applied   uint64
	Progress  map[uint64]Progress
	LeadTransferee uint64
}

func getStatus(r *raft) Status {
	s := Status{
		ID:             r.id,
		HardState:      r.hardState(),
		SoftState:      *r.softState(),
		Applied:        r.raftLog.applied,
		LeadTransferee: r.leadTransferee,
	}

	if r.state == StateLeader {
		s.Progress = make(map[uint64]Progress)
		for id, pr := range r.trk.Progress {
			s.Progress[id] = *pr
		}
	}

	return s
}
