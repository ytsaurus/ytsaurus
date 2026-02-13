package agent

import (
	"context"
	"math/rand/v2"
	"sync"
	"syscall"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type JobCheckerConfig struct {
	CheckPeriod      *yson.Duration `yson:"check_period"`
	WorkerNumber     *int           `yson:"worker_number"`
	TrackedExitCodes *[]int         `yson:"tracked_exit_codes"`
}

const (
	DefaultFinishedJobsCheckPeriod       = yson.Duration(5 * time.Minute)
	DefaultFinishedJobsCheckWorkerNumber = 10
)

func (c *JobCheckerConfig) CheckPeriodOrDefault() yson.Duration {
	if c.CheckPeriod != nil {
		return *c.CheckPeriod
	}
	return DefaultFinishedJobsCheckPeriod
}

func (c *JobCheckerConfig) WorkerNumberOrDefault() int {
	if c.WorkerNumber != nil {
		return *c.WorkerNumber
	}
	return DefaultFinishedJobsCheckWorkerNumber
}

func (c *JobCheckerConfig) TrackedExitCodesOrDefault() []int {
	if c.TrackedExitCodes != nil {
		return *c.TrackedExitCodes
	}

	defaultTrackedSignals := []syscall.Signal{
		syscall.SIGABRT,
		syscall.SIGSEGV,
		syscall.SIGILL,
		syscall.SIGKILL}

	defaultExitCodes := make([]int, 0)
	for _, signal := range defaultTrackedSignals {
		defaultExitCodes = append(defaultExitCodes, 128+int(signal))
	}

	return defaultExitCodes
}

type JobCheckerResult struct {
	ID          yt.OperationID
	CrashedJobs []yt.JobID
}

type JobCheckerResultBatch struct {
	Results []JobCheckerResult
	AggrErr error
}

func CheckFinishedJobs(ctx context.Context, ytc yt.Client, l log.Logger, cfg *JobCheckerConfig) (chan<- []OperationStatus, <-chan JobCheckerResultBatch) {
	trackedExitCodes := make(map[int]struct{})
	for _, code := range cfg.TrackedExitCodesOrDefault() {
		trackedExitCodes[code] = struct{}{}
	}

	checker := &jobChecker{
		ytc:              ytc,
		l:                l,
		trackedExitCodes: trackedExitCodes,
		workerNumber:     cfg.WorkerNumberOrDefault(),
		opInfos:          make(map[yt.OperationID]trackedOpInfo),
	}
	return checker.start(ctx, cfg)
}

type trackedOpInfo struct {
	lastFailedJobCounter   int
	targetFailedJobCounter *int
}

type jobChecker struct {
	ytc yt.Client
	l   log.Logger

	workerNumber int

	trackedExitCodes map[int]struct{}

	expireCandidates []yt.OperationID
	opInfos          map[yt.OperationID]trackedOpInfo
}

func (c *jobChecker) start(ctx context.Context, cfg *JobCheckerConfig) (chan<- []OperationStatus, <-chan JobCheckerResultBatch) {
	period := time.Duration(cfg.CheckPeriodOrDefault())
	c.l.Debug("jobs checking started", log.Duration("period", period))

	inputCh := make(chan []OperationStatus, 1)
	outputCh := make(chan JobCheckerResultBatch, 1)

	ticker := time.NewTicker(period)

	go func() {
		for {
			select {
			case <-ctx.Done():
				c.l.Debug("jobs checking finished")
				ticker.Stop()
				return

			case inputTasks := <-inputCh:
				c.processRunningOperations(inputTasks)

			case <-ticker.C:
				outputCh <- c.checkJobsRoutine(ctx)

			}
		}
	}()

	return inputCh, outputCh
}

func (c *jobChecker) processRunningOperations(ops []OperationStatus) {
	for _, op := range ops {
		var newCounter int
		if op.OperationStatus.BriefProgress.TotalJobCounter != nil {
			newCounter = int(op.OperationStatus.BriefProgress.TotalJobCounter.Failed)
		}

		info, ok := c.opInfos[op.ID]
		if !ok {
			c.opInfos[op.ID] = trackedOpInfo{lastFailedJobCounter: newCounter}
		}
		info = c.opInfos[op.ID]

		info.targetFailedJobCounter = &newCounter

		c.opInfos[op.ID] = info
	}
}

func buildAggrError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	return yterrors.Err("job checker failed to list jobs of some operations",
		yterrors.Attr("random_error", errs[rand.IntN(len(errs))]),
		yterrors.Attr("error_count", len(errs)),
	)
}

func (c *jobChecker) checkJobsRoutine(ctx context.Context) JobCheckerResultBatch {
	c.l.Debug("starting jobs checking routine", log.Int("tracked_ops_cnt", len(c.opInfos)))

	expiredCnt := 0
	for _, opID := range c.expireCandidates {
		if c.opInfos[opID].targetFailedJobCounter == nil {
			delete(c.opInfos, opID)
			expiredCnt++
		}
	}
	c.expireCandidates = c.expireCandidates[:0]

	skippedCnt := 0
	tasks := make([]jobsCheckWorkerTask, 0, len(c.opInfos))
	for opID, info := range c.opInfos {
		var limitPtr *int
		if info.targetFailedJobCounter != nil {
			limit := *info.targetFailedJobCounter - info.lastFailedJobCounter

			if limit <= 0 {
				info.targetFailedJobCounter = nil
				c.opInfos[opID] = info
				skippedCnt++
				c.l.Debug("skip operation for jobs check cause absence of failed jobs", log.String("op_id", opID.String()))
				continue
			}

			limitPtr = &limit
		}

		c.l.Debug("schedule jobs check task",
			log.String("op_id", opID.String()),
			log.Int("offset", info.lastFailedJobCounter),
			log.Any("limit", limitPtr))

		tasks = append(tasks, jobsCheckWorkerTask{
			opID:   opID,
			offset: info.lastFailedJobCounter,
			limit:  limitPtr,
		})
	}

	workerResults := make([][]jobsCheckWorkerResultOrError, c.workerNumber)

	var wg sync.WaitGroup
	wg.Add(c.workerNumber)
	for i := 0; i < c.workerNumber; i++ {
		go func(workerIdx int) {
			defer wg.Done()

			for taskIdx := workerIdx; taskIdx < len(tasks); taskIdx += c.workerNumber {
				resultOrErr := c.runTask(ctx, tasks[taskIdx])
				workerResults[workerIdx] = append(workerResults[workerIdx], resultOrErr)
			}
		}(i)
	}
	wg.Wait()

	results := make([]JobCheckerResult, 0)
	errs := make([]error, 0)
	for _, workerResult := range workerResults {
		for _, result := range workerResult {
			if result.err != nil {
				c.l.Error("job checker failed to list jobs", log.Error(result.err))
				errs = append(errs, result.err)
				continue
			}

			opID := result.opID
			info := c.opInfos[opID]
			info.lastFailedJobCounter += result.processedFailedJobCnt

			if info.targetFailedJobCounter == nil && result.processedFailedJobCnt == 0 {
				c.expireCandidates = append(c.expireCandidates, opID)
			}

			if info.targetFailedJobCounter != nil && info.lastFailedJobCounter == *info.targetFailedJobCounter {
				info.targetFailedJobCounter = nil
			}

			c.opInfos[opID] = info
			results = append(results, JobCheckerResult{
				ID:          opID,
				CrashedJobs: result.crashedJobs,
			})
		}
	}

	c.l.Debug("jobs checking routine completed",
		log.Int("checked_ops_cnt", len(tasks)),
		log.Int("skipped_ops_cnt", skippedCnt),
		log.Int("expired_ops_cnt", expiredCnt),
		log.Int("expire_candidate_ops_cnt", len(c.expireCandidates)))

	return JobCheckerResultBatch{
		Results: results,
		AggrErr: buildAggrError(errs),
	}
}

type jobsCheckWorkerResultOrError struct {
	opID                  yt.OperationID
	crashedJobs           []yt.JobID
	processedFailedJobCnt int

	err error
}

type jobsCheckWorkerTask struct {
	opID   yt.OperationID
	offset int
	limit  *int
}

func userJobExitCodeFromError(jobErr yterrors.Error) (int, bool) {
	if jobErr.Code != yterrors.CodeUserJobFailed {
		return 0, false
	}

	for _, innerErr := range jobErr.InnerErrors {
		if innerErr != nil && innerErr.Code == yterrors.CodeProcessNonZeroExitCode {
			if exitCodeAttr, ok := innerErr.Attributes["exit_code"]; ok {
				return int(exitCodeAttr.(int64)), true
			}
		}
	}

	return 0, false
}

func (c *jobChecker) runTask(ctx context.Context, task jobsCheckWorkerTask) (result jobsCheckWorkerResultOrError) {
	defer func() {
		c.l.Debug("jobs checker finished task",
			log.Any("op_id", task.opID),
			log.Int("offset", task.offset),
			log.Any("limit", task.limit),
			log.Any("result_err", result.err),
			log.Int("result_processed_job_cnt", result.processedFailedJobCnt),
			log.Any("result_crashed_jobs", result.crashedJobs))
	}()
	result.opID = task.opID

	opts := &yt.ListJobsOptions{
		JobState:  &yt.JobFailed,
		SortField: &yt.SortFieldFinishTime,
		SortOrder: &yt.Ascending,
		Limit:     task.limit,
		Offset:    &task.offset,
	}

	list, err := c.ytc.ListJobs(ctx, task.opID, opts)
	if err != nil {
		result.err = yterrors.Err(err, yterrors.Attr("operation_id", task.opID))
		return
	}

	if list == nil {
		return
	}

	result.processedFailedJobCnt = len(list.Jobs)
	for _, job := range list.Jobs {
		if exitCode, ok := userJobExitCodeFromError(job.Error); ok {
			if _, ok := c.trackedExitCodes[exitCode]; ok {
				result.crashedJobs = append(result.crashedJobs, job.ID)
			}
		}
	}

	return
}
