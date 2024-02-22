package ytsys

import (
	"context"
	"fmt"
)

type ChunkIntegrity struct {
	C int64

	LVC int64
	DMC int64
	PMC int64
	URC int64
	QMC int64

	RequisitionUpdateEnabled bool
	RefreshEnabled           bool
	ReplicatorEnabled        bool
}

func (i *ChunkIntegrity) String() string {
	return fmt.Sprintf(`chunks: %d, lvc %d, dmc: %d, pmc: %d, urc: %d, qmc: %d, requisition_update_enabled: %t, refresh_enabled: %t, replicator_enabled: %t`,
		i.C, i.LVC, i.DMC, i.PMC, i.URC, i.QMC,
		i.RequisitionUpdateEnabled, i.RefreshEnabled, i.ReplicatorEnabled)
}

func (i *ChunkIntegrity) Check(maxURC float64) bool {
	return i.LVC == 0 && i.DMC == 0 && i.PMC == 0 && i.QMC == 0 &&
		(i.C == 0 || i.C > 0 && float64(i.URC)/float64(i.C) <= maxURC) &&
		i.RequisitionUpdateEnabled && i.RefreshEnabled && i.ReplicatorEnabled
}

func (i *ChunkIntegrity) CheckUnrecoverable() bool {
	return i.LVC == 0 && i.QMC == 0 && i.RequisitionUpdateEnabled && i.RefreshEnabled && i.ReplicatorEnabled
}

func LoadIntegrityIndicators(ctx context.Context, dc *Client) (*ChunkIntegrity, error) {
	var err error

	i := &ChunkIntegrity{}

	if i.C, err = dc.GetChunkCount(ctx); err != nil {
		return nil, err
	}
	if i.LVC, err = dc.GetLVC(ctx); err != nil {
		return nil, err
	}
	if i.DMC, err = dc.GetDMC(ctx); err != nil {
		return nil, err
	}
	if i.PMC, err = dc.GetPMC(ctx); err != nil {
		return nil, err
	}
	if i.URC, err = dc.GetURC(ctx); err != nil {
		return nil, err
	}
	if i.QMC, err = dc.GetQMC(ctx); err != nil {
		return nil, err
	}
	if i.RequisitionUpdateEnabled, err = dc.GetChunkRequisitionUpdateEnabled(ctx); err != nil {
		return nil, err
	}
	if i.RefreshEnabled, err = dc.GetChunkRefreshEnabled(ctx); err != nil {
		return nil, err
	}
	if i.ReplicatorEnabled, err = dc.GetChunkReplicatorEnabled(ctx); err != nil {
		return nil, err
	}

	return i, nil
}
