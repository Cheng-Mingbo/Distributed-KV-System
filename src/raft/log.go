package raft

// LogEntry is a log entry in the Raft log.
type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// getFirstLog returns the first log entry.
func (rf *Raft) getFirstLog() LogEntry {
	return rf.logs[0]
}

// getCommitedLog returns the commited log entry.
func (rf *Raft) getCommitedLog() LogEntry {
	return rf.logs[rf.commitIndex-rf.getFirstLog().Index]
}

// matchLog checks if the log entry at a given index has the given term.
// It returns true if there is a match, false otherwise.
func (rf *Raft) matchLog(term, index int) bool {
	idx := index - rf.getFirstLog().Index
	if idx < 0 || idx >= len(rf.logs) {
		return false
	}
	return rf.logs[idx].Term == term
}

func shrinkEntriesArray(entries []LogEntry) []LogEntry {
	if len(entries) == 0 {
		return entries // Return original slice if already empty
	}

	return append([]LogEntry{}, entries...) // Create a new slice with a copy of the entries
}

// isUpToDate returns true if the given term and index are at least as up-to-date as the Raft node's last log term and index.
func (rf *Raft) isUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (lastLog.Term == term && index >= lastLog.Index)
}

// getLastLog returns the last log entry.
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}
