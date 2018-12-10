#pragma once

#include "private.h"

#include "serialize.h"

#include <yt/core/actions/signal.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TAutoMergeDirector
{
public:
    //! Upper limit for maximum intermediate chunk count.
    DEFINE_BYVAL_RO_PROPERTY(int, MaxIntermediateChunkCount);

    //! Maximum number of chunks per each merge job.
    DEFINE_BYVAL_RO_PROPERTY(int, ChunkCountPerMergeJob);

public:
    //! Used only for persistence.
    TAutoMergeDirector() = default;

    TAutoMergeDirector(int maxIntermediateChunkCount, int maxChunkCountPerMergeJob, TOperationId operationId);

    //! Returns true if new job fits in the maxIntermediateChunkCount limit, false otherwise.
    //! If it returns false and the number of running auto-merge jobs is zero, the force-flush
    //! mode is enabled.
    //! This method is called from the main task.
    bool CanScheduleTaskJob(int intermediateChunkCountEstimate) const;

    //! Returns true if it is time to flush intermediate chunks by putting them into a single
    //! auto-merge job.
    //! This method is called from the auto-merge task.
    bool CanScheduleMergeJob(int intermediateChunkCount) const;

    void OnTaskJobStarted(int intermediateChunkCountEstimate);

    //! Method that is called when main task job is finished to discount the original intermediate
    //! chunk count estimate. Newly created chunks will be accounted in each auto-merge task.
    void OnTaskJobFinished(int intermediateChunkCountEstimate);

    //! This method is called by auto-merge task after teleporting all large chunks with the actual
    //! count of intermediate chunks that should be processed when new stripe is added or resumed.
    //! It is also called during stripe suspension to discount lost chunks.
    //! NB: this method should be called right after the OnTaskJobFinished method in order to not
    //! have situation when intermediate chunks are undercounted.
    void AccountMergeInputChunks(int intermediateChunkCount);

    //! Method that is called when auto merge job is started.
    void OnMergeJobStarted();

    //! Method that is called when auto merge job in a given output table is finished.
    //! If force-flush mode is enabled, it becomes disabled.
    void OnMergeJobFinished(int unregisteredIntermediateChunkCount);

    //! Method that is called when the task has been completed to inform
    //! that we should flush all remaining intermediate chunks.
    void OnTaskCompleted();

    //! Return maximum number of new task jobs that may be started in current situation.
    int GetTaskPendingJobCountLimit();

    void Persist(const TPersistenceContext& context);

    DEFINE_SIGNAL(void(), StateChanged);

private:
    //! Id of the operation this auto-merge director belongs to.
    TOperationId OperationId_;

    //! Number of all intermediate chunks to be auto-merged. It is a sum of two values:
    //! number of chunks already created by the finished jobs (that is precise) and
    //! number of chunks that were not created yet (they are accounted using the stripe list
    //! output chunk count estimate).
    //! NB: This field is intentionally transient as we have no easy way to discount chunks
    //! that were released in the previous incarnation of the scheduler.
    int CurrentIntermediateChunkCount_ = 0;

    //! The number of currently running auto-merge jobs.
    int RunningMergeJobCount_ = 0;

    //! The number of currently running task jobs.
    int RunningTaskJobCount_ = 0;

    //! Flag showing that there are currently too many intermediate chunks.
    mutable bool ForceScheduleMergeJob_ = false;

    //! Flag showing that the task has been completed.
    bool TaskCompleted_ = false;

    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NControllerAgent
