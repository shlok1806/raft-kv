package raft

// LogEntry is a single slot in the replicated log.
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// raftLog wraps the slice and tracks the snapshot offset so that real log
// indices and slice indices don't get conflated (they shift after compaction).
type raftLog struct {
	entries []LogEntry

	// after a snapshot, entries[0] is a dummy whose Term/Index describe
	// the last entry included in the snapshot
	snapshotIndex int
	snapshotTerm  int
}

func newRaftLog() *raftLog {
	// sentinel at position 0; real entries start at index 1
	return &raftLog{
		entries: []LogEntry{{Term: 0, Index: 0}},
	}
}

func (l *raftLog) lastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) lastTerm() int {
	return l.entries[len(l.entries)-1].Term
}

// at returns the entry with the given absolute log index.
// Panics if out of range — callers are responsible for bounds checking.
func (l *raftLog) at(index int) LogEntry {
	offset := l.snapshotIndex
	return l.entries[index-offset]
}

func (l *raftLog) termAt(index int) int {
	if index == l.snapshotIndex {
		return l.snapshotTerm
	}
	return l.at(index).Term
}

// slice returns entries [from, end) by absolute index.
func (l *raftLog) slice(from, end int) []LogEntry {
	offset := l.snapshotIndex
	return l.entries[from-offset : end-offset]
}

func (l *raftLog) append(entries ...LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateFrom discards entries from absolute index `from` onward.
func (l *raftLog) truncateFrom(from int) {
	offset := l.snapshotIndex
	l.entries = l.entries[:from-offset]
}

// compactThrough discards entries up to and including absolute index `idx`.
// The entry at idx becomes the new sentinel.
func (l *raftLog) compactThrough(idx, term int) {
	offset := l.snapshotIndex
	if idx-offset >= len(l.entries) {
		l.entries = []LogEntry{{Term: term, Index: idx}}
	} else {
		l.entries = l.entries[idx-offset:]
		l.entries[0] = LogEntry{Term: term, Index: idx}
	}
	l.snapshotIndex = idx
	l.snapshotTerm = term
}

func (l *raftLog) len() int {
	return len(l.entries) - 1 // don't count the sentinel
}
