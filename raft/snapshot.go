package raft

func (r *Raft) sendSnapshot(peer int) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              r.currentTerm,
		LeaderId:          r.me,
		LastIncludedIndex: r.log.snapshotIndex,
		LastIncludedTerm:  r.log.snapshotTerm,
		Data:              r.persister.ReadSnapshot(),
	}
	term := r.currentTerm
	r.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if !r.sendRPC(peer, "Raft.InstallSnapshot", args, reply) {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}

	if r.state != Leader || r.currentTerm != term {
		return
	}

	// after a snapshot, follower is caught up to at least snapshotIndex
	if r.matchIndex[peer] < args.LastIncludedIndex {
		r.matchIndex[peer] = args.LastIncludedIndex
	}
	r.nextIndex[peer] = r.matchIndex[peer] + 1
}

func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.mu.Lock()

	reply.Term = r.currentTerm

	if args.Term < r.currentTerm {
		r.mu.Unlock()
		return nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
		reply.Term = r.currentTerm
	}

	r.tickReset()
	r.mu.Unlock()

	// hand off to the KV layer — it decides whether to actually install
	r.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	return nil
}
