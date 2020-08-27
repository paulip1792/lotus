package sectorstorage

import (
	"context"
	"net/url"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
)

type equalHostSelector struct {
	index      stores.SectorIndex
	sector     abi.SectorID
	alloc      stores.SectorFileType
}

func newEqualHostSelector(index stores.SectorIndex, sector abi.SectorID, alloc stores.SectorFileType) *equalHostSelector {
	return &equalHostSelector{
		index:      index,
		sector:     sector,
		alloc:      alloc,
	}
}

func (s *equalHostSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if _, supported := tasks[task]; !supported {
		return false, nil
	}

	log.Infow("equalHostSelector.Ok", "workerHost", whnd.info.Host, "sectorNumber", s.sector.Number)
	best, err := s.index.StorageFindSector(ctx, s.sector, s.alloc, spt, false)
	if err != nil {
		return false, xerrors.Errorf("finding best storage: %w", err)
	}

	for _, info := range best {
		for _, rawURL := range info.URLs {
			u, err := url.Parse(rawURL)
			if err != nil {
				log.Warnw("url.Parse", "url", u, "err", err, "sectorNumber", s.sector.Number, "workerHost", whnd.info.Host)
				continue
			}
			log.Infow("matching", "sectorNumber", s.sector.Number, "workerHost", whnd.info.Host, "sectorStorageHost", u.Host)
			if whnd.info.Host == u.Host {
				return true, nil
			}
		}
	}

	return false, nil
}

func (s *equalHostSelector) Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
}

var _ WorkerSelector = &equalHostSelector{}
