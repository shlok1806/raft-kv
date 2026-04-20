package raft

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
)

// Persister is an interface so tests can swap in an in-memory version
// without touching the filesystem.
type Persister interface {
	SaveRaftState(data []byte)
	ReadRaftState() []byte
	SaveSnapshot(data []byte)
	ReadSnapshot() []byte
	SaveStateAndSnapshot(state, snapshot []byte)
}

// FilePersister writes state to a directory on disk. Simple, not fancy.
type FilePersister struct {
	dir string
}

func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FilePersister{dir: dir}, nil
}

func (p *FilePersister) statePath() string {
	return filepath.Join(p.dir, "raft-state")
}

func (p *FilePersister) snapPath() string {
	return filepath.Join(p.dir, "snapshot")
}

func (p *FilePersister) SaveRaftState(data []byte) {
	// write to a temp file then rename — avoids a torn write on crash
	tmp := p.statePath() + ".tmp"
	_ = os.WriteFile(tmp, data, 0644)
	_ = os.Rename(tmp, p.statePath())
}

func (p *FilePersister) ReadRaftState() []byte {
	data, _ := os.ReadFile(p.statePath())
	return data
}

func (p *FilePersister) SaveSnapshot(data []byte) {
	tmp := p.snapPath() + ".tmp"
	_ = os.WriteFile(tmp, data, 0644)
	_ = os.Rename(tmp, p.snapPath())
}

func (p *FilePersister) ReadSnapshot() []byte {
	data, _ := os.ReadFile(p.snapPath())
	return data
}

func (p *FilePersister) SaveStateAndSnapshot(state, snapshot []byte) {
	p.SaveRaftState(state)
	p.SaveSnapshot(snapshot)
}

// MemPersister keeps everything in memory — for tests.
type MemPersister struct {
	state    []byte
	snapshot []byte
}

func (m *MemPersister) SaveRaftState(data []byte) {
	c := make([]byte, len(data))
	copy(c, data)
	m.state = c
}

func (m *MemPersister) ReadRaftState() []byte { return m.state }

func (m *MemPersister) SaveSnapshot(data []byte) {
	c := make([]byte, len(data))
	copy(c, data)
	m.snapshot = c
}

func (m *MemPersister) ReadSnapshot() []byte { return m.snapshot }

func (m *MemPersister) SaveStateAndSnapshot(state, snapshot []byte) {
	m.SaveRaftState(state)
	m.SaveSnapshot(snapshot)
}

// --- Raft encode / decode helpers ---

type persistedState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
	SnapIndex   int
	SnapTerm    int
}

func (r *Raft) persist() {
	r.persister.SaveRaftState(r.encodeState())
}

func (r *Raft) persistWithSnapshot(snap []byte) {
	r.persister.SaveStateAndSnapshot(r.encodeState(), snap)
}

func (r *Raft) encodeState() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	ps := persistedState{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log.entries,
		SnapIndex:   r.log.snapshotIndex,
		SnapTerm:    r.log.snapshotTerm,
	}
	if err := enc.Encode(ps); err != nil {
		panic("raft: encode state: " + err.Error())
	}
	return buf.Bytes()
}

func (r *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var ps persistedState
	if err := dec.Decode(&ps); err != nil {
		panic("raft: decode state: " + err.Error())
	}

	r.currentTerm = ps.CurrentTerm
	r.votedFor = ps.VotedFor
	r.log.entries = ps.Log
	r.log.snapshotIndex = ps.SnapIndex
	r.log.snapshotTerm = ps.SnapTerm
}
