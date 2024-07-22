#pragma once

#include "private.h"
#include "task_host.h"

#include <yt/yt/ytlib/chunk_pools/chunk_pool.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TNodeDirectoryBuilderFactory
{
public:
    TNodeDirectoryBuilderFactory(
        NProto::TJobSpecExt* jobSpecExt,
        TInputManagerPtr inputManager,
        NScheduler::EOperationType operationType);

    std::shared_ptr<NNodeTrackerClient::TNodeDirectoryBuilder> GetNodeDirectoryBuilder(
        const NChunkPools::TChunkStripePtr& stripe);

private:
    NProto::TJobSpecExt* const JobSpecExt_;
    const TInputManagerPtr InputManager_;
    THashMap<NScheduler::TClusterName, std::shared_ptr<NNodeTrackerClient::TNodeDirectoryBuilder>> Builders_;
    const bool IsRemoteCopy_;
};

////////////////////////////////////////////////////////////////////////////////

struct IInputManagerHost
    // XXX(coteeq): ITaskHost has many methods, that IInputManagerHost also needs.
    // I hope there is a nicer way of doing this, other than just inheriting ITaskHost.
    : public virtual ITaskHost
{
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

class TUnavailableChunksWatcher
    : public TRefCounted
{
public:
    explicit TUnavailableChunksWatcher(
        std::vector<NChunkClient::IFetcherChunkScraperPtr> chunkScrapers);
    i64 GetUnavailableChunkCount() const;

private:
    const std::vector<NChunkClient::IFetcherChunkScraperPtr> ChunkScrapers_;
};

DEFINE_REFCOUNTED_TYPE(TUnavailableChunksWatcher)

////////////////////////////////////////////////////////////////////////////////

class TCombiningSamplesFetcher
    : public TRefCounted
{
public:
    TCombiningSamplesFetcher() = default;
    explicit TCombiningSamplesFetcher(
        std::vector<NTableClient::TSamplesFetcherPtr> samplesFetchers);
    TFuture<void> Fetch() const;
    std::vector<NTableClient::TSample> GetSamples() const;

private:
    const std::vector<NTableClient::TSamplesFetcherPtr> SamplesFetchers_;
};

DEFINE_REFCOUNTED_TYPE(TCombiningSamplesFetcher)

////////////////////////////////////////////////////////////////////////////////

class TInputCluster
    : public TRefCounted
{
public:
    NScheduler::TClusterName Name;
    NLogging::TSerializableLogger Logger;

    using TPathToInputTablesMapping = THashMap<TString, std::vector<TInputTablePtr>>;

    DEFINE_BYREF_RW_PROPERTY(NApi::NNative::IClientPtr, Client, nullptr);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::TNodeDirectoryPtr, NodeDirectory, New<NNodeTrackerClient::TNodeDirectory>());
    DEFINE_BYREF_RW_PROPERTY(std::vector<TInputTablePtr>, InputTables);
    DEFINE_BYREF_RW_PROPERTY(NChunkClient::TChunkScraperPtr, ChunkScraper, nullptr);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NChunkClient::TChunkId>, UnavailableInputChunkIds);
    DEFINE_BYREF_RW_PROPERTY(TPathToInputTablesMapping, PathToInputTables);

    TInputCluster() = default;
    TInputCluster(
        NScheduler::TClusterName name,
        NLogging::TLogger logger);

    void LockTables();
    void ValidateOutputTableLockedCorrectly(const TOutputTablePtr& outputTable) const;

    void ReportIfHasUnavailableChunks() const;

    // NB: Note that this is local client, not <this cluster's client>.
    void InitializeClient(NApi::NNative::IClientPtr localClient);

    void Persist(const TPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TInputCluster)

////////////////////////////////////////////////////////////////////////////////

class TInputManager
    : public NYT::TRefCounted
{
public:
    TInputManager() = default;
    TInputManager(
        IInputManagerHost* host,
        NLogging::TLogger logger);

    void InitializeClients(NApi::NNative::IClientPtr client);
    void InitializeStructures(
        NApi::NNative::IClientPtr client,
        const TInputTransactionsManagerPtr& inputTransactionsManager);

    const std::vector<TInputTablePtr>& GetInputTables() const;

    void LockInputTables();
    void ValidateOutputTableLockedCorrectly(const TOutputTablePtr& outputTable) const;

    void Prepare();
    TFetchInputTablesStatistics FetchInputTables();

    bool CanInterruptJobs() const;

    void RegisterInputStripe(const NChunkPools::TChunkStripePtr& stripe, const TTaskPtr& task);
    NChunkClient::TInputChunkPtr GetInputChunk(NChunkClient::TChunkId chunkId, int chunkIndex) const;

    void Persist(const TPersistenceContext& context);
    // COMPAT(coteeq)
    void PrepareToBeLoadedFromAncientVersion();
    void LoadInputNodeDirectory(const TPersistenceContext& context);
    void LoadInputChunkMap(const TPersistenceContext& context);
    void LoadPathToInputTables(const TPersistenceContext& context);
    void LoadInputTables(const TPersistenceContext& context);
    void LoadInputHasOrderedDynamicStores(const TPersistenceContext& context);

    void RegisterUnavailableInputChunks(bool reportIfFound = false);
    void BuildUnavailableInputChunksYson(NYTree::TFluentAny fluent) const;

    int GetUnavailableInputChunkCount() const;

    // If failed chunk was an input chunk, then marks chunk as failed and returns true.
    // If failed chunk was an intermediate chunk, then does nothing and returns false.
    bool OnInputChunkFailed(NChunkClient::TChunkId chunkId, TJobId jobId);

    NChunkClient::IFetcherChunkScraperPtr CreateFetcherChunkScraper(const TInputClusterPtr& cluster) const;
    NChunkClient::IFetcherChunkScraperPtr CreateFetcherChunkScraper(const NScheduler::TClusterName& clusterName) const;

    std::pair<NTableClient::IChunkSliceFetcherPtr, TUnavailableChunksWatcherPtr> CreateCombiningChunkSliceFetcher() const;

    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory(
        const NScheduler::TClusterName& clusterName) const;
    NApi::NNative::IClientPtr GetClient(const NScheduler::TClusterName& clusterName) const;

    friend class TNodeDirectoryBuilderFactory;

    //! Returns the list of all input chunks collected from all primary input tables of cluster *clusterName.
    //! If !clusterName.has_value(), then returns all chunks of all input tables.
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryChunks(
        bool versioned,
        std::optional<NScheduler::TClusterName> clusterName = {}) const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryUnversionedChunks() const;
    std::vector<NChunkClient::TInputChunkPtr> CollectPrimaryVersionedChunks() const;

    std::pair<NTableClient::IChunkSliceFetcherPtr, TUnavailableChunksWatcherPtr> CreateChunkSliceFetcher() const;

    std::pair<TCombiningSamplesFetcherPtr, TUnavailableChunksWatcherPtr> CreateSamplesFetcher(
        const NTableClient::TTableSchemaPtr& sampleSchema,
        const NTableClient::TRowBufferPtr& rowBuffer,
        i64 sampleCount,
        int maxSampleSize) const;

private:
    // NB: InputManager does not outlive its host.
    IInputManagerHost* Host_;
    NLogging::TSerializableLogger Logger; // NOLINT

    THashMap<NScheduler::TClusterName, TInputClusterPtr> Clusters_;
    TClusterResolverPtr ClusterResolver_;

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

        int GetTableIndex() const;

        void Persist(const TPersistenceContext& context);
    };

    THashMap<NChunkClient::TChunkId, TInputChunkDescriptor> InputChunkMap_;
    int ChunkLocatedCallCount_ = 0;

    bool InputHasOrderedDynamicStores_ = false;
    bool InputHasStaticTableWithHunks_ = false;

    void FetchInputTablesAttributes();

    void InitInputChunkScrapers();

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

    const TInputClusterPtr& GetClusterOrCrash(const NScheduler::TClusterName& clusterName) const;
    const TInputClusterPtr& GetClusterOrCrash(NChunkClient::TChunkId chunkId) const;
    const TInputClusterPtr& GetClusterOrCrash(const TInputChunkDescriptor& chunkDescriptor) const;

    NChunkClient::TMasterChunkSpecFetcherPtr CreateChunkSpecFetcher(const TInputClusterPtr& cluster) const;
};

DEFINE_REFCOUNTED_TYPE(TInputManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
