#pragma once

#include "private.h"
#include "task_host.h"

#include <yt/yt/ytlib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class IInputManagerHost
    : public virtual TRefCounted
    // XXX(coteeq): ITaskHost has many methods, that IInputManagerHost also needs.
    // I hope there is a nicer way of doing this, other than just inheriting ITaskHost.
    , public virtual ITaskHost
{
public:
    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;

    virtual bool IsBoundaryKeysFetchEnabled() const = 0;

    virtual void MaybeCancel(NScheduler::ECancelationStage cancelationStage) = 0;

    virtual const NChunkClient::TThrottlerManagerPtr& GetChunkLocationThrottlerManager() const = 0;
    virtual TCancelableContextPtr GetCancelableContext() const = 0;

    virtual IInvokerPtr GetChunkScraperInvoker() const = 0;

    virtual NChunkClient::EChunkAvailabilityPolicy GetChunkAvailabilityPolicy() const = 0;
    virtual void ValidateInputTablesTypes() const = 0;

    virtual void InvokeSafely(std::function<void()> callback) = 0;

    //! If fetching chunk slice statistics is not possible for the operation, returns an error with a reason.
    virtual TError GetUseChunkSliceStatisticsError() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInputManagerHost)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EInputChunkState,
    (Active)
    (Skipped)
    (Waiting)
);

struct TFetchInputTablesStatistics
{
    i64 ChunkCount = 0;
    i64 ExtensionSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TInputManager
    : public NYT::TRefCounted
{
public:
    TInputManager() = default;
    TInputManager(
        IInputManagerHost* host,
        NLogging::TLogger logger);

    void InitializeClient(NApi::NNative::IClientPtr client);
    void InitializeStructures(TInputTransactionsManagerPtr inputTransactions);

    const std::vector<TInputTablePtr>& GetInputTables() const;

    void LockInputTables();
    void ValidateOutputTableLockedCorrectly(TOutputTablePtr outputTable) const;

    void Prepare();
    TFetchInputTablesStatistics FetchInputTables();

    bool CanInterruptJobs() const;

    void RegisterInputStripe(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task);
    NChunkClient::TInputChunkPtr GetInputChunk(NChunkClient::TChunkId chunkId, int chunkIndex) const;

    void Persist(const TPersistenceContext& context);
    // COMPAT(coteeq)
    void LoadInputNodeDirectory(const TPersistenceContext& context);
    void LoadInputChunkMap(const TPersistenceContext& context);
    void LoadPathToInputTables(const TPersistenceContext& context);
    void LoadInputTables(const TPersistenceContext& context);

    void RegisterUnavailableInputChunks(bool reportIfFound = false);
    void BuildUnavailableInputChunksYson(NYTree::TFluentAny fluent) const;

    size_t GetUnavailableInputChunkCount() const;

    // If failed chunk was an input chunk, then marks chunk as failed and returns true.
    // If failed chunk was an intermediate chunk, then does nothing and returns false.
    bool OnInputChunkFailed(NChunkClient::TChunkId chunkId, TJobId jobId);

    NChunkClient::IFetcherChunkScraperPtr CreateFetcherChunkScraper() const;

    const NNodeTrackerClient::TNodeDirectoryPtr& GetInputNodeDirectory() const;

private:
    // NB: InputManager does not outlive its host.
    IInputManagerHost* Host_;
    NLogging::TSerializableLogger Logger; // NOLINT

    NApi::NNative::IClientPtr Client_;

    THashMap<TString, std::vector<TInputTablePtr>> PathToInputTables_;

    TInputTransactionsManagerPtr InputTransactions_;
    NNodeTrackerClient::TNodeDirectoryPtr InputNodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();

    std::vector<TInputTablePtr> InputTables_;

    struct TStripeDescriptor
    {
        NChunkPools::TChunkStripePtr Stripe;
        NChunkPools::IChunkPoolInput::TCookie Cookie = NChunkPools::IChunkPoolInput::NullCookie;
        TTaskPtr Task;

        void Persist(const TPersistenceContext& context);
    };

    struct TInputChunkDescriptor
        : public TRefTracked<TInputChunkDescriptor>
    {
        TCompactVector<TStripeDescriptor, 1> InputStripes;
        TCompactVector<NChunkClient::TInputChunkPtr, 1> InputChunks;
        EInputChunkState State = EInputChunkState::Active;

        void Persist(const TPersistenceContext& context);
    };

    THashMap<NChunkClient::TChunkId, TInputChunkDescriptor> InputChunkMap_;
    THashSet<NChunkClient::TChunkId> UnavailableInputChunkIds_;
    NChunkClient::TChunkScraperPtr InputChunkScraper_;
    int ChunkLocatedCallCount_ = 0;

    bool InputHasOrderedDynamicStores_ = false;
    bool InputHasStaticTableWithHunks_ = false;

    void FetchInputTablesAttributes();

    void InitInputChunkScraper();

    void RegisterInputChunk(const NChunkClient::TInputChunkPtr& inputChunk);

    //! Callback called by TChunkScraper when information on some chunk is fetched.
    void OnInputChunkLocated(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::TChunkReplicaWithMediumList& replicas,
        bool missing);
    void OnInputChunkUnavailable(
        NChunkClient::TChunkId chunkId,
        TInputChunkDescriptor* descriptor);
    void OnInputChunkAvailable(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::TChunkReplicaWithMediumList& replicas,
        TInputChunkDescriptor* descriptor);

    void RegisterUnavailableInputChunk(NChunkClient::TChunkId chunkId);
    void UnregisterUnavailableInputChunk(NChunkClient::TChunkId chunkId);

    TFormattableView<THashSet<NChunkClient::TChunkId>, TDefaultFormatter> MakeUnavailableInputChunksView() const;

    NChunkClient::TMasterChunkSpecFetcherPtr MakeChunkSpecFetcher() const;
};

DEFINE_REFCOUNTED_TYPE(TInputManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
