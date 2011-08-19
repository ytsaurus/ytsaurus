#pragma once

#include "common.h"

#include "../transaction/common.h"
#include "../chunk_holder/replicator.h"

namespace NYT {
namespace NChunkManager {

////////////////////////////////////////////////////////////////////////////////

using NChunkHolder::EJobState;
using NChunkHolder::TJobId;
using NChunkHolder::TJobIdHash;

class TReplicationJob
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TReplicationJob> TPtr;

    TReplicationJob(
        const TJobId& jobId,
        const TChunkId& chunkId,
        const yvector<Stroka>& targetAddresses,
        EJobState state)
        : JobId(jobId)
        , ChunkId(chunkId)
        , TargetAddresses_(targetAddresses)
        , State_(state)
    { }

    TJobId GetJobId() const
    {
        return JobId;
    }

    TChunkId GetChunkId() const
    {
        return ChunkId;
    }

    const yvector<Stroka>& TargetAddresses() const
    {
        return TargetAddresses_;
    }

    EJobState& State()
    {
        return State_;
    }

private:
    TJobId JobId;
    TChunkId ChunkId;
    yvector<Stroka> TargetAddresses_;
    EJobState State_;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkReplication
    : public TNonCopyable
{
public:
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

};

////////////////////////////////////////////////////////////////////////////////

typedef ui64 TChunkGroupId;

class TChunk
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChunk> TPtr;
    typedef yvector<int> TLocations;

    static const i64 UnknownSize = -1;

    //! For seralization
    TChunk()
    { }

    TChunk(const TChunkId& id)
        : Id(id)
        , Size_(UnknownSize)
    { }

    //! For putting in TMetaStateRefMap
    TChunk(TChunk& chunk)
        : Id(chunk.Id)
        , Size_(chunk.Size_)
        , TransactionId_(chunk.TransactionId_)
        , Locations_(chunk.Locations_)
    { }

    TChunkId GetId() const
    {
        return Id;
    }

    TChunkGroupId GetGroupId() const
    {
        return static_cast<ui64>(Id.Parts[0]) << 32 + static_cast<ui64>(Id.Parts[1]);
    }


    i64& Size()
    {
        return Size_;
    }


    TTransactionId& TransactionId()
    {
        return TransactionId_;
    }

    bool IsVisible(NTransaction::TTransactionId transactionId) const
    {
        return
            TransactionId_ != NTransaction::TTransactionId() ||
            TransactionId_ == transactionId;
    }


    void AddLocation(int holderId)
    {
        if (!IsIn(Locations_, holderId)) {
            Locations_.push_back(holderId);
        }
    }

    void RemoveLocation(int holderId)
    {
        TLocations::iterator it = Find(Locations_.begin(), Locations_.end(), holderId);
        if (it != Locations_.end()) {
            Locations_.erase(it);
        }
    }

    TLocations& Locations()
    {
        return Locations_;
    }


    int GetTargetReplicaCount() const
    {
        // TODO: make configurable
        return 3;
    }

    int GetActualReplicaCount() const
    {
        return Locations_.ysize();
    }

    int GetPotentialReplicaCount() const
    {
        int result = GetActualReplicaCount();
        if (~Replication != NULL) {
            result += Replication->GetPotentialRepCount();
        }
        return result;
    }

    int GetReplicaDelta() const
    {
        return GetPotentialReplicaCount() - GetTargetReplicaCount(); 
    }


    TChunkReplication* GetReplication()
    {
        if (~Replication == NULL) {
            Replication.Reset(new TChunkReplication());
        }
        return ~Replication;
    }

    void TryTrimReplication()
    {
        if (~Replication != NULL && Replication->IsEmpty()) {
            Replication.Destroy();
        }
    }

private:
    TChunkId Id;
    i64 Size_;
    NTransaction::TTransactionId TransactionId_;
    TLocations Locations_;
    ::THolder<TChunkReplication> Replication;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkManager
} // namespace NYT
