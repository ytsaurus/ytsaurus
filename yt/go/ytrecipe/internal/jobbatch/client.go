package jobbatch

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

type Client struct {
	yc     yt.Client
	mr     mapreduce.Client
	l      log.Structured
	config Config
}

type Config struct {
	ClientInfo interface{}
	GroupKey   string

	TablePath ypath.Path

	BatchingDelay time.Duration
	PollInterval  time.Duration
	StartDeadline time.Duration
}

func NewClient(yc yt.Client, mr mapreduce.Client, l log.Structured, config Config) *Client {
	return &Client{
		yc:     yc,
		mr:     mr,
		l:      l,
		config: config,
	}
}

const jobStateSizeLimit = 1 << 20

func encodeJobState(job mapreduce.Job) ([]byte, error) {
	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&job); err != nil {
		return nil, err
	}

	if buf.Len() > jobStateSizeLimit {
		return nil, fmt.Errorf("job state size exceeds row size limit: %d > %d", buf.Len(), jobStateSizeLimit)
	}

	return buf.Bytes(), nil
}

func readJobs(r yt.TableReader, err error) ([]Job, error) {
	if err != nil {
		return nil, err
	}

	defer r.Close()

	var jobs []Job
	for r.Next() {
		var job Job
		if err := r.Scan(&job); err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}
	return jobs, r.Err()
}

func (c *Client) isJobPicked(job Job) bool {
	return job.PickedAt != nil && time.Since(*job.PickedAt) < c.config.StartDeadline
}

func jobRows(jobs []Job) []interface{} {
	var r []interface{}
	for _, j := range jobs {
		r = append(r, j)
	}
	return r
}

func (c *Client) selectWaitingJobs(ctx context.Context, tx yt.TabletTx, selfID guid.GUID) (selectedJobs []Job, selectedSelf bool, err error) {
	query := fmt.Sprintf("* from [%s] where GroupKey = %q and Operation = #",
		c.config.TablePath, c.config.GroupKey)

	jobs, err := readJobs(tx.SelectRows(ctx, query, nil))
	if err != nil {
		return
	}

	for _, job := range jobs {
		if c.isJobPicked(job) {
			c.l.Info("job was picked by another client",
				log.Any("picked_by", job.PickedBy),
				log.Any("picked_at", job.PickedAt))
		}

		if job.ID == selfID {
			selectedSelf = true
		}

		if job.PickedAt == nil {
			c.l.Info("picking job", log.String("job_id", job.ID.String()))
		} else {
			c.l.Warn("picking job lost by another client",
				log.String("job_id", job.ID.String()),
				log.Any("picked_by", job.PickedBy),
				log.Any("picked_at", job.PickedAt))
		}
		selectedJobs = append(selectedJobs, job)
	}

	return
}

func (c *Client) checkJobStatus(ctx context.Context, selfID guid.GUID) (op *Operation, wait bool, err error) {
	jobKey := Job{GroupKey: c.config.GroupKey, ID: selfID}
	jobs, err := readJobs(c.yc.LookupRows(ctx, c.config.TablePath, []interface{}{jobKey}, nil))
	if err != nil {
		return
	}

	if len(jobs) != 1 {
		err = fmt.Errorf("job %s vanished from table", selfID)
		return
	}

	job := jobs[0]
	if job.Operation != nil {
		c.l.Info("operation was started by another client",
			log.Any("picked_by", job.PickedBy),
			log.Any("picked_at", job.PickedAt))
		return jobs[0].Operation, false, nil
	}

	if c.isJobPicked(job) {
		c.l.Info("job was picked by another client",
			log.Any("picked_by", job.PickedBy),
			log.Any("picked_at", job.PickedAt))
		wait = true
		return
	}

	return
}

func (c *Client) launchAggregateOperation(ctx context.Context, jobs []Job) (*Operation, error) {
	spec := mergeSpecs(jobs)

	aj := &AggregateJob{}
	for _, j := range jobs {
		aj.JobStates = append(aj.JobStates, j.JobState)
	}

	c.l.Info("launching operation", log.Int("job_count", len(jobs)))
	op, err := c.mr.Vanilla(spec, map[string]mapreduce.Job{"jobs": aj})
	if err != nil {
		return nil, err
	}

	return &Operation{ID: op.ID()}, nil
}

func (c *Client) Schedule(ctx context.Context, meta *SpecPart, job mapreduce.Job) (*Operation, error) {
	jobState, err := encodeJobState(job)
	if err != nil {
		return nil, err
	}

	jobID := guid.New()
	c.l.Info("started batching job", log.String("job_id", jobID.String()))

	queueRow := []interface{}{
		Job{
			GroupKey: c.config.GroupKey,
			ID:       jobID,
			JobState: jobState,
			SpecPart: meta,
		},
	}

	err = c.yc.InsertRows(ctx, c.config.TablePath, queueRow, nil)
	if err != nil {
		return nil, err
	}

	select {
	case <-time.After(c.config.BatchingDelay):
		c.l.Info("batching delay elapsed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	for {
		select {
		case <-time.After(c.config.PollInterval):
			c.l.Info("polling job state")
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		op, wait, err := c.checkJobStatus(ctx, jobID)
		if err != nil {
			return nil, err
		} else if op != nil {
			return op, nil
		} else if wait {
			continue
		}

		tx, err := c.yc.BeginTabletTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		defer tx.Abort()

		selectedJobs, selectedSelf, err := c.selectWaitingJobs(ctx, tx, jobID)
		if err != nil {
			return nil, err
		}

		if !selectedSelf {
			c.l.Info("aborting scheduling; job was scheduled by another client")
			continue
		}

		pickTime := time.Now()
		for i := range selectedJobs {
			selectedJobs[i].PickedAt = &pickTime
			selectedJobs[i].PickedBy = c.config.ClientInfo
		}

		if err := tx.InsertRows(ctx, c.config.TablePath, jobRows(selectedJobs), nil); err != nil {
			return nil, err
		}

		err = tx.Commit()
		if yterrors.ContainsErrorCode(err, yterrors.CodeTransactionLockConflict) {
			c.l.Info("got row lock conflict; retrying")
			continue
		} else if err != nil {
			return nil, err
		}

		op, err = c.launchAggregateOperation(ctx, selectedJobs)
		if err != nil {
			return nil, err
		}

		for i := range selectedJobs {
			selectedJobs[i].Operation = op
		}

		if err := c.yc.InsertRows(ctx, c.config.TablePath, jobRows(selectedJobs), nil); err != nil {
			return nil, err
		}

		return op, nil
	}
}
