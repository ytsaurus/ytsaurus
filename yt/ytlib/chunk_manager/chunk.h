#pragma once

#include "common.h"

#include "../transaction/common.h"
#include "../chunk_holder/replicator.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

using NChunkHolder::EJobState;
using NChunkHolder::EJobType;
using NChunkHolder::TJobId;
using NChunkHolder::TJobIdHash;

struct TJob
{
    TJob()
    { }

    TJob(
        EJobType type,
        const TJobId& jobId,
        const TChunkId& chunkId,
        Stroka runnerAddress,
        const yvector<Stroka>& targetAddresses,
        EJobState state)
        : Type(type)
        , JobId(jobId)
        , ChunkId(chunkId)
        , RunnerAddress(runnerAddress)
        , TargetAddresses(targetAddresses)
        , State(state)
    { }

    TJob(const TJob& other)
        : Type(other.Type)
        , JobId(other.JobId)
        , ChunkId(other.ChunkId)
        , RunnerAddress(other.RunnerAddress)
        , TargetAddresses(other.TargetAddresses)
        , State(other.State)
    { }

    TJob& operator = (const TJob& other)
    {
        // TODO: implement
        YASSERT(false);
        return *this;
    }

    EJobType Type;
    TJobId JobId;
    TChunkId ChunkId;
    Stroka RunnerAddress;
    yvector<Stroka> TargetAddresses;
    EJobState State;

};

struct TJobList
{
    typedef yvector<TJobId> TJobs;

    TJobList()
    { }

    TJobList(const TChunkId& chunkId)
        : ChunkId(chunkId)
    { }

    TJobList(const TJobList& other)
        : ChunkId(other.ChunkId)
        , Jobs(other.Jobs)
    { }

    TJobList& operator = (const TJobList& other)
    {
        // TODO: implement
        YASSERT(false);
        return *this;
    }

    void AddJob(const TJobId& id)
    {
        Jobs.push_back(id);
    }

    void RemoveJob(const TJobId& id)
    {
        TJobs::iterator it = Find(Jobs.begin(), Jobs.end(), id);
        if (it != Jobs.end()) {
            Jobs.erase(it);
        }
    }
    

    TChunkId ChunkId;
    TJobs Jobs;

   /*
    typedef yvector<TReplicationJob::TPtr> TJobs;

    bool IsEmpty() const
    {
        return Jobs.empty();
    }

    int GetPotentialRepCount() const
    {
        int result = 0;
        for (TJobs::const_iterator it = Jobs.begin(); it != Jobs.end(); ++it) {
            TReplicationJob::TPtr job = *it;
            result += job->TargetAddresses().ysize();
        }
        return 0;
    }

    void StartJob(TReplicationJob::TPtr job)
    {
        Jobs.push_back(job);
    }

    bool StopJob(const TJobId& jobId)
    {
        for (TJobs::iterator it = Jobs.begin(); it != Jobs.end(); ++it) {
            TReplicationJob::TPtr job = *it;
            if (job->GetJobId() == jobId) {
                Jobs.erase(it);
                return true;
            }
        }
        return false;
    }

    // TODO: drop?
    TReplicationJob::TPtr FindJob(const TJobId& jobId)
    {
        for (TJobs::iterator it = Jobs.begin(); it != Jobs.end(); ++it) {
            TReplicationJob::TPtr job = *it;
            if (job->GetJobId() == jobId) {
                return job;
            }
        }
        return NULL;
    }

    // TODO: drop?
    TReplicationJob::TPtr GetJob(const TJobId& jobId)
    {
        TReplicationJob::TPtr job = FindJob(jobId);
        YASSERT(~job != NULL);
        return job;
    }

private:
    TJobs Jobs;
    */
};

////////////////////////////////////////////////////////////////////////////////

//typedef ui64 TChunkGroupId;

struct TChunk
{
    typedef yvector<int> TLocations;

    static const i64 UnknownSize = -1;

    TChunk()
    { }

    TChunk(
        const TChunkId& id,
        const TTransactionId& transactionId)
        : Id(id)
        , Size(UnknownSize)
        , TransactionId(transactionId)
        , PendingReplicaCount(0)
    { }

    TChunk(const TChunk& other)
        : Id(other.Id)
        , Size(other.Size)
        , TransactionId(other.TransactionId)
        , Locations(other.Locations)
        , PendingReplicaCount(other.PendingReplicaCount)
    { }

    TChunk& operator = (const TChunk& other)
    {
        // TODO: implement
        YASSERT(false);
        return *this;
    }

    //TChunkGroupId GetGroupId() const
    //{
    //    return static_cast<ui64>(Id.Parts[0]) << 32 + static_cast<ui64>(Id.Parts[1]);
    //}

    bool IsVisible(const NTransaction::TTransactionId& transactionId) const
    {
        return
            TransactionId != NTransaction::TTransactionId() ||
            TransactionId == transactionId;
    }


    void AddLocation(int holderId)
    {
        if (!IsIn(Locations, holderId)) {
            Locations.push_back(holderId);
        }
    }

    void RemoveLocation(int holderId)
    {
        TLocations::iterator it = Find(Locations.begin(), Locations.end(), holderId);
        if (it != Locations.end()) {
            Locations.erase(it);
        }
    }

    //int GetTargetReplicaCount() const
    //{
    //    // TODO: make configurable
    //    return 3;
    //}

    //int GetActualReplicaCount() const
    //{
    //    return Locations_.ysize();
    //}

    //int GetPotentialReplicaCount() const
    //{
    //    int result = GetActualReplicaCount();
    //    if (~Replication != NULL) {
    //        result += Replication->GetPotentialRepCount();
    //    }
    //    return result;
    //}

    //int GetReplicaDelta() const
    //{
    //    return GetPotentialReplicaCount() - GetTargetReplicaCount(); 
    //}


    //TChunkReplication* GetReplication()
    //{
    //    if (~Replication == NULL) {
    //        Replication.Reset(new TChunkReplication());
    //    }
    //    return ~Replication;
    //}

    //void TryTrimReplication()
    //{
    //    if (~Replication != NULL && Replication->IsEmpty()) {
    //        Replication.Destroy();
    //    }
    //}

    TChunkId Id;
    i64 Size;
    NTransaction::TTransactionId TransactionId;
    TLocations Locations;
    i8 PendingReplicaCount;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
