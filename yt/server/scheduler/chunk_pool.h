#pragma once

#include "helpers.h"
#include "private.h"
#include "progress_counter.h"
#include "serialize.h"

#include <yt/server/chunk_server/public.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeStatistics
{
    int ChunkCount = 0;
    i64 DataSize = 0;
    i64 RowCount = 0;
    i64 MaxBlockSize = 0;

    void Persist(const TPersistenceContext& context);
};

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

typedef SmallVector<TChunkStripeStatistics, 1> TChunkStripeStatisticsVector;

//! Adds up input statistics and returns a single-item vector with the sum.
TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics);

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe(bool foreign = false, bool solid = false);
    explicit TChunkStripe(NChunkClient::TInputDataSlicePtr dataSlice, bool foreign = false);

    TChunkStripeStatistics GetStatistics() const;
    int GetChunkCount() const;

    int GetTableIndex() const;

    int GetInputStreamIndex() const;

    void Persist(const TPersistenceContext& context);

    SmallVector<NChunkClient::TInputDataSlicePtr, 1> DataSlices;
    int WaitingChunkCount = 0;
    bool Foreign = false;
    bool Solid = false;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList() = default;
    TChunkStripeList(int stripeCount);

    TChunkStripeStatisticsVector GetStatistics() const;
    TChunkStripeStatistics GetAggregateStatistics() const;

    void Persist(const TPersistenceContext& context);

    std::vector<TChunkStripePtr> Stripes;

    TNullable<int> PartitionTag;

    //! If True then TotalDataSize and TotalRowCount are approximate (and are hopefully upper bounds).
    bool IsApproximate = false;

    i64 TotalDataSize = 0;
    i64 LocalDataSize = 0;

    i64 TotalRowCount = 0;

    int TotalChunkCount = 0;
    int LocalChunkCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripeList)

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual IPersistent
{
    typedef int TCookie;
    static const TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;

    virtual void Suspend(TCookie cookie) = 0;
    virtual void Resume(TCookie cookie, TChunkStripePtr stripe) = 0;
    virtual void Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputBase
    : public virtual IChunkPoolInput
{
public:
    // IChunkPoolInput implementation.

    virtual void Finish() override;

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override;

protected:
    bool Finished = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TInputTable
    : public TLockedUserObject
{
    //! Number of chunks in the whole table (without range selectors).
    int ChunkCount = -1;
    std::vector<NChunkClient::TInputChunkPtr> Chunks;
    NTableClient::TTableSchema Schema;
    NTableClient::ETableSchemaMode SchemaMode;
    bool IsDynamic;

    //! Set to true when schema of the table is compatible with the output
    //! teleport table and when no special options set that disallow chunk
    //! teleporting (like force_transform = %true).
    bool IsTeleportable = false;

    bool IsForeign() const;

    bool IsPrimary() const;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
    : public virtual IPersistent
{
    typedef int TCookie;
    static constexpr TCookie NullCookie = -1;

    virtual i64 GetTotalDataSize() const = 0;
    virtual i64 GetRunningDataSize() const = 0;
    virtual i64 GetCompletedDataSize() const = 0;
    virtual i64 GetPendingDataSize() const = 0;

    virtual i64 GetTotalRowCount() const = 0;

    virtual bool IsCompleted() const = 0;

    virtual int GetTotalJobCount() const = 0;
    virtual int GetPendingJobCount() const = 0;
    virtual const TProgressCounter& GetJobCounter() const = 0;

    //! Approximate average stripe list statistics to estimate memory usage.
    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    //! The main purpose of this method is to be much cheaper than #GetStripeList,
    //! and to eliminate creation/desctuction of a stripe list if we have already reached
    //! JobSpecSliceThrottler limit. This is particularly useful for a shuffle chunk pool.
    virtual int GetStripeListSliceCount(TCookie cookie) const = 0;

    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const = 0;

    virtual void Completed(TCookie cookie, const TCompletedJobSummary& jobSummary) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Aborted(TCookie cookie) = 0;
    virtual void Lost(TCookie cookie) = 0;

    //! Raised when all the output cookies from this pool no longer correspond to valid jobs.
    DECLARE_INTERFACE_SIGNAL(void(const TError& error), PoolOutputInvalidated);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IChunkPoolOutput
{
public:
    TChunkPoolOutputBase();

    // IChunkPoolOutput implementation.

    virtual i64 GetTotalDataSize() const override;

    virtual i64 GetRunningDataSize() const override;

    virtual i64 GetCompletedDataSize() const override;

    virtual i64 GetPendingDataSize() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual const TProgressCounter& GetJobCounter() const override;

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override;

    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const override;

public:
    DEFINE_SIGNAL(void(const TError& error), PoolOutputInvalidated)

protected:
    TProgressCounter DataSizeCounter;
    TProgressCounter RowCounter;
    TProgressCounter JobCounter;

    std::vector<NChunkClient::TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendableStripe
{
public:
    DEFINE_BYVAL_RW_PROPERTY(IChunkPoolOutput::TCookie, ExtractedCookie);
    DEFINE_BYVAL_RW_PROPERTY(bool, Teleport, false);

public:
    TSuspendableStripe();
    explicit TSuspendableStripe(TChunkStripePtr stripe);

    const TChunkStripePtr& GetStripe() const;
    const TChunkStripeStatistics& GetStatistics() const;
    void Suspend();
    bool IsSuspended() const;
    void Resume(TChunkStripePtr stripe);

    //! Resume chunk and return a hashmap that defines the correspondence between
    //! the old and new chunks. If building such mapping is impossible (for example,
    //! the new stripe contains more data slices, or the new data slices have different
    //! read limits or boundary keys), exception is thrown.
    yhash<NChunkClient::TInputChunkPtr, NChunkClient::TInputChunkPtr> ResumeAndBuildChunkMapping(TChunkStripePtr stripe);

    //! Replaces the original stripe with the current stripe.
    void ReplaceOriginalStripe();

    void Persist(const TPersistenceContext& context);

private:
    TChunkStripePtr Stripe_;
    TChunkStripePtr OriginalStripe_ = nullptr;
    bool Suspended_ = false;
    TChunkStripeStatistics Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

std::unique_ptr<IChunkPool> CreateAtomicChunkPool();

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    IJobSizeConstraintsPtr jobSizeConstraints,
    TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig);

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual IPersistent
{
    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

class TInputStreamDescriptor
{
public:
    //! Used only for persistence.
    TInputStreamDescriptor() = default;
    TInputStreamDescriptor(bool isTeleportable, bool isPrimary, bool isVersioned);

    bool IsTeleportable() const;
    bool IsPrimary() const;
    bool IsForeign() const;
    bool IsVersioned() const;
    bool IsUnversioned() const;

    void Persist(const TPersistenceContext& context);

private:
    bool IsTeleportable_;
    bool IsPrimary_;
    bool IsVersioned_;
};

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDescriptor IntermediateInputStreamDescriptor;

////////////////////////////////////////////////////////////////////////////////

class TInputStreamDirectory
{
public:
    //! Used only for persistence
    TInputStreamDirectory() = default;
    explicit TInputStreamDirectory(
        std::vector<TInputStreamDescriptor> descriptors,
        TInputStreamDescriptor defaultDescriptor = IntermediateInputStreamDescriptor);

    const TInputStreamDescriptor& GetDescriptor(int inputStreamIndex) const;

    int GetDescriptorCount() const;

    void Persist(const TPersistenceContext& context);
private:
    std::vector<TInputStreamDescriptor> Descriptors_;
    TInputStreamDescriptor DefaultDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

extern TInputStreamDirectory IntermediateInputStreamDirectory;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

