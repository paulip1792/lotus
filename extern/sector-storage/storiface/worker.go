package storiface

import (
	"time"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname string
	Host     string // host or host:port

	Resources WorkerResources
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info WorkerInfo

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
}

type WorkerJob struct {
	ID     uint64
	Sector abi.SectorID
	Task   sealtasks.TaskType

	Start time.Time
}
