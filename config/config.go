package config

import "time"

// ClusterConfig holds everything a node needs to know about the cluster.
// Timeouts are deliberately wide defaults — tune them per deployment.
type ClusterConfig struct {
	Peers []string // peer addresses, e.g. ["localhost:8001", "localhost:8002"]

	// election timer fires in [Min, Max); randomness avoids split votes
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	HeartbeatInterval time.Duration

	// compact the log once it grows past this many entries
	SnapshotThreshold int

	// where to persist raft state / snapshots on disk
	DataDir string
}

func DefaultConfig() *ClusterConfig {
	return &ClusterConfig{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		SnapshotThreshold:  100,
		DataDir:            "data",
	}
}
