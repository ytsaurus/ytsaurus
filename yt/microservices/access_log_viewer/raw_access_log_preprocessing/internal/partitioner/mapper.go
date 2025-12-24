package partitioner

import (
	"time"

	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/internal/consts"
	"go.ytsaurus.tech/yt/microservices/access_log_viewer/raw_access_log_preprocessing/pkg/instanttime"
)

const (
	isoEventtimeFormat = "2006-01-02 15:04:05"
)

type partition struct {
	MinTimestamp instanttime.InstantTime
	MaxTimestamp instanttime.InstantTime
}

type PartitioningMapper struct {
	Partitions       []*partition
	OutputTableCount int
}

func (s *PartitioningMapper) InputTypes() []any {
	return []any{&consts.AccessLogRow{}}
}

func (s *PartitioningMapper) OutputTypes() []any {
	var outputTypes []any
	for i := 0; i < s.OutputTableCount; i++ {
		outputTypes = append(outputTypes, &consts.AccessLogRow{})
	}
	return outputTypes
}

func (s *PartitioningMapper) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	for in.Next() {
		var row consts.AccessLogRow
		in.MustScan(&row)
		row.IsoEventtime = time.Time(row.Instant).Format(isoEventtimeFormat)
		for i, partition := range s.Partitions {
			if !time.Time(row.Instant).Before(time.Time(partition.MinTimestamp)) &&
				time.Time(row.Instant).Before(time.Time(partition.MaxTimestamp)) {
				out[i].MustWrite(&row)
			}
		}
	}
	return nil
}
