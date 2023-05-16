#pragma once

#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

//! Class implementing Persist using YT_UNIMPLMENTED().
struct TDummyPersistent
    : public virtual IPersistent
{
    void Persist(const TPersistenceContext& /*context*/) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputMock
    : public virtual IPersistentChunkPoolInput
    , public virtual TDummyPersistent
{
public:
    MOCK_METHOD(TCookie, Add, (TChunkStripePtr), (override));
    MOCK_METHOD(TCookie, AddWithKey, (TChunkStripePtr, TChunkStripeKey), (override));
    MOCK_METHOD(void, Suspend, (TCookie), (override));
    MOCK_METHOD(void, Resume, (TCookie), (override));
    MOCK_METHOD(void, Reset, (TCookie, TChunkStripePtr, TInputChunkMappingPtr), (override));
    MOCK_METHOD(void, Finish, (), (override));
    MOCK_METHOD(bool, IsFinished, (), (const, override));
};

DEFINE_REFCOUNTED_TYPE(TChunkPoolInputMock)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolJobSplittingHostMock
    : public virtual IPersistentChunkPoolJobSplittingHost
    , public virtual TDummyPersistent
{
public:
    MOCK_METHOD(bool, IsSplittable, (TOutputCookie cookie), (const, override));
};

DEFINE_REFCOUNTED_TYPE(TChunkPoolJobSplittingHostMock)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputMockBase
    : public virtual IPersistentChunkPoolOutput
    , public virtual TDummyPersistent
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);
    DEFINE_SIGNAL_OVERRIDE(void(), Completed);
    DEFINE_SIGNAL_OVERRIDE(void(), Uncompleted);

public:
    MOCK_METHOD(TOutputOrderPtr, GetOutputOrder, (), (const, override));
    MOCK_METHOD(i64, GetLocality, (NNodeTrackerClient::TNodeId), (const, override));
    MOCK_METHOD(NTableClient::TChunkStripeStatisticsVector, GetApproximateStripeStatistics, (), (const, override));
    MOCK_METHOD(TCookie, Extract, (NNodeTrackerClient::TNodeId), (override));
    MOCK_METHOD(TChunkStripeListPtr, GetStripeList, (TCookie cookie), (override));
    MOCK_METHOD(bool, IsCompleted, (), (const, override));
    MOCK_METHOD(int, GetStripeListSliceCount, (TCookie), (const, override));
    MOCK_METHOD(void, Completed, (TCookie, const NControllerAgent::TCompletedJobSummary&), (override));
    MOCK_METHOD(void, Failed, (TCookie), (override));
    MOCK_METHOD(void, Aborted, (TCookie, NScheduler::EAbortReason), (override));
    MOCK_METHOD(void, Lost, (TCookie), (override));

    const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetRowCounter() const override;
    const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const override;

    void TeleportChunk(NChunkClient::TInputChunkPtr teleportChunk);
    void Complete();

    NControllerAgent::TProgressCounterPtr JobCounter = New<NControllerAgent::TProgressCounter>();
    NControllerAgent::TProgressCounterPtr DataWeightCounter = New<NControllerAgent::TProgressCounter>();
    NControllerAgent::TProgressCounterPtr RowCounter = New<NControllerAgent::TProgressCounter>();
    NControllerAgent::TProgressCounterPtr DataSliceCounter = New<NControllerAgent::TProgressCounter>();
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputMock
    : public TChunkPoolOutputMockBase
    , public TChunkPoolJobSplittingHostMock
{ };

DEFINE_REFCOUNTED_TYPE(TChunkPoolOutputMock)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolMock
    : public IMultiChunkPool
    , public TChunkPoolInputMock
    , public TChunkPoolOutputMock
{ };

////////////////////////////////////////////////////////////////////////////////

class TTestJobSplittingBase
    : public TJobSplittingBase
{
public:
    using TJobSplittingBase::GetMaxVectorSize;
    using TJobSplittingBase::RegisterChildCookies;
    using TJobSplittingBase::Completed;
};

DEFINE_REFCOUNTED_TYPE(TTestJobSplittingBase)

using TTestJobSplittingBasePtr = TIntrusivePtr<TTestJobSplittingBase>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
