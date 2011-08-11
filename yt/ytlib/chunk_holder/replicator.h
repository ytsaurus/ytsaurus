#pragma once

#include "common.h"
#include "chunk_store.h"
#include "block_store.h"

#include "../chunk_client/chunk_writer.h"

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EJobState,
    (Running)
    (Completed)
    (Failed)
);

////////////////////////////////////////////////////////////////////////////////

class TReplicator;

//! Represent a replication job on a chunk holder.
class TJob
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TJob> TPtr;

    //! Returns job id.
    int GetJobId() const;

    //! Returns the current state.
    EJobState GetState() const;

    //! Returns the addresses of chunk holders where the chunk is being replicated to.
    yvector<Stroka> GetTargetAddresses() const;

    //! Returns the chunk that is being replicated.
    TChunk::TPtr GetChunk() const;

private:
    friend class TReplicator;

    TBlockStore::TPtr BlockStore;
    int JobId;
    EJobState State;
    TChunk::TPtr Chunk;
    yvector<Stroka> TargetAddresses;
    IChunkWriter::TPtr Writer;
    TCancelableInvoker::TPtr CancelableInvoker;
    
    TJob(
        IInvoker::TPtr serviceInvoker,
        TBlockStore::TPtr blockStore,
        int jobId,
        TChunk::TPtr chunk,
        const yvector<Stroka>& targetAddresses);

    void Start();
    void Stop();

    bool ReplicateBlock(int blockIndex);
    void OnBlockLoaded(
        TCachedBlock::TPtr cachedBlock,
        int blockIndex);
    void OnWriterClosed(TVoid);
};

////////////////////////////////////////////////////////////////////////////////

//! Controls chunk replication on a chunk holder.
/*!
 *  Each chunk holder has a set of currently active replication jobs.
 *  These jobs are started by the master and are used for two purposes:
 *  making additional replicas of chunks lacking enough of them and
 *  redistributing the chunks among chunk holders to ensure even distribution.
 *  
 *  Each job is represented by an instance of TJob class.
 *  A job is created by calling #StartJob and stopped by calling #StopJob methods.
 *  
 *  Each job may be either running, completed or failed.
 *  Completed and failed job do not vanish automatically. It is the responsibility
 *  of the master to stop them.
 *  
 *  The status of all jobs is send to the master with each heartbeat.
 *  This way the master obtains the outcomes of each started jobs.
 * 
 *  A job is identified by its id. This id is assigned by the master when a job is started.
 *  Using master-controlled id assigned eliminates the need for additional RPC roundtrips
 *  for getting these ids from the holder.
 *  
 */
class TReplicator
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TReplicator> TPtr;

    //! Constructs a new instance.
    TReplicator(
        TBlockStore::TPtr blockStore,
        IInvoker::TPtr serviceInvoker);
    
    //! Starts a new job with the given parameters.
    TJob::TPtr StartJob(
        int jobId,
        TChunk::TPtr chunk,
        const yvector<Stroka>& targetAddresses);

    //! Stops the job.
    void StopJob(TJob::TPtr job);

    // TODO: is it needed?
    //! Stop all currently active jobs.
    void StopAllJobs();

    //! Finds job by its id. Return NULL if no job is found.
    TJob::TPtr FindJob(int jobId);

    //! Gets all active jobs.
    yvector<TJob::TPtr> GetAllJobs();

private:
    typedef yhash_map<int, TJob::TPtr> TJobMap;

    TBlockStore::TPtr BlockStore;
    IInvoker::TPtr ServiceInvoker;

    TJobMap Jobs;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

