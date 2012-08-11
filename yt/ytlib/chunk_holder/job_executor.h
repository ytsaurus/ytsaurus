#pragma once

#include "public.h"

#include <ytlib/chunk_holder/chunk.pb.h>

#include <ytlib/misc/guid.h>
#include <ytlib/misc/async_stream_state.h>
#include <ytlib/actions/cancelable_context.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/cell_node/public.h>
#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

class TJobExecutor;

//! Represents a replication job on a chunk holder.
class TJob
    : public TRefCounted
{
public:
    TJob(
        TBootstrap* bootstrap,
        EJobType jobType,
        const TJobId& jobId,
        const TChunkId& chunkId,
        const std::vector<Stroka>& targetAddresses);

    //! Returns the type.
    EJobType GetType() const;

    //! Returns the id.
    TJobId GetJobId() const;

    //! Returns the current state.
    EJobState GetState() const;

    //! Returns the addresses of chunk holders where the chunk is being replicated to.
    std::vector<Stroka> GetTargetAddresses() const;

private:
    friend class TJobExecutor;

    TBootstrap* Bootstrap;
    EJobType JobType;
    TJobId JobId;
    EJobState State;
    TChunkId ChunkId;
    NProto::TChunkMeta ChunkMeta;
    std::vector<Stroka> TargetAddresses;

    NChunkClient::IAsyncWriterPtr Writer;
    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableInvoker;
    
    NLog::TTaggedLogger Logger;

    void Start();
    void Stop();

    void RunRemove();

    void RunReplicate();
    void ReplicateBlock(int blockIndex, TError error);

    void SetCompleted();
    void SetFailed(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

//! Controls chunk replication and removal on a chunk holder.
/*!
 *  Each chunk holder has a set of currently active replication jobs.
 *  These jobs are started by the master and are used for two purposes:
 *  making additional replicas of chunks lacking enough of them and
 *  moving chunks around chunk holders to ensure even distribution.
 *  
 *  Each job is represented by an instance of TJob class.
 *  A job is created by calling #StartJob and stopped by calling #StopJob methods.
 *  
 *  Each job may be either running, completed or failed.
 *  Completed and failed job do not vanish automatically. It is the responsibility
 *  of the master to stop them.
 *  
 *  The status of all jobs is propagated to the master with each heartbeat.
 *  This way the master obtains the outcomes of each job it had started.
 * 
 *  A job is identified by its id, which is assigned by the master when a job is started.
 *  Using master-controlled id assignment eliminates the need for additional RPC round-trips
 *  for getting these ids from the holder.
 */
class TJobExecutor
    : public TRefCounted
{
public:
    //! Constructs a new instance.
    explicit TJobExecutor(TBootstrap* bootstrap);
    
    //! Starts a new job with the given parameters.
    TJobPtr StartJob(
        EJobType jobType,
        const TJobId& jobId,
        const TChunkId& chunkId,
        const std::vector<Stroka>& targetAddresses);

    //! Stops the job.
    void StopJob(TJobPtr job);

    //! Finds job by its id. Returns NULL if no job is found.
    TJobPtr FindJob(const TJobId& jobId);

    //! Gets all active jobs.
    std::vector<TJobPtr> GetAllJobs();

private:
    friend class TJob;
    typedef yhash_map<TJobId, TJobPtr> TJobMap;

    TBootstrap* Bootstrap;

    TJobMap Jobs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

