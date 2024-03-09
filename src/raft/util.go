package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// StableHeartbeatTimeout returns the stable heartbeat timeout.
func StableHeartbeatTimeout() time.Duration {
	return 125 * time.Millisecond
}

// RandomElectionTimeout returns a random election timeout.
func RandomElectionTimeout() time.Duration {
	ms := 300 + rand.Intn(350)
	return time.Duration(ms) * time.Millisecond
}

func SearchNextIndex(entries []LogEntry, conflictTerm int) int {
	left := 0
	right := len(entries)

	for left < right {
		mid := left + (right-left)/2
		if entries[mid].Term < conflictTerm {
			left = mid + 1
		} else if entries[mid].Term > conflictTerm {
			right = mid
		} else {
			left = mid + 1
		}
	}
	return left
}
