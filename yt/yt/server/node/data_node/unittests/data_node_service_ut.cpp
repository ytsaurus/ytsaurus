#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/data_node_service.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/master_connector.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/io/huge_page_manager.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/test_framework/test_connection.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/random.h>

#include <yt/yt/library/fusion/service_directory.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NCellMasterClient;
using namespace NClusterNode;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNative;
using namespace NObjectClient;
using namespace NRpc;
using namespace NConcurrency;

using NYT::NBus::EMultiplexingBand;
using NNative::TConnectionDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GenerateRandomBlockIdsWithOrder(int min, int max, int count, TRandomGenerator& generator)
{
    std::vector<int> blockIds;
    blockIds.reserve(count);
    for (int i = 0; i < count; ++i) {
        blockIds.push_back(generator.Generate<unsigned>() % (max - min + 1) + min);
    }
    std::sort(blockIds.begin(), blockIds.end());
    blockIds.erase(std::unique(blockIds.begin(), blockIds.end()), blockIds.end());
    return blockIds;
}

std::vector<TChecksum> BlocksToChecksums(const std::vector<TBlock>& blocks)
{
    std::vector<TChecksum> checksums;
    checksums.reserve(blocks.size());
    for (const auto& block : blocks) {
        YT_VERIFY(block.Size() > 0);
        checksums.push_back(block.GetOrComputeChecksum());
    }
    return checksums;
}

std::string GenerateRandomString(int size, TRandomGenerator& generator)
{
    std::string result;
    result.resize(size);
    for (auto& c : result) {
        c = 'a' + (generator.Generate<unsigned>() % 26);
    }
    return result;
}

std::vector<TBlock> CreateBlocks(int count, int blockSize, TRandomGenerator& generator)
{
    std::vector<TBlock> blocks;
    blocks.reserve(count);

    for (int index = 0; index < count; ++index) {
        blocks.emplace_back(TSharedRef::FromString(GenerateRandomString(blockSize, generator)));
    }

    return blocks;
}

int CalculateCummulativeBlockSize(const std::vector<TBlock>& blocks)
{
    int size = 0;
    for (const auto& block : blocks) {
        size += block.Size();
    }
    return size;
}

std::vector<std::vector<bool>> GeneratePairWiseCases(int paramCount) {
    std::vector<std::vector<bool>> result;
    std::vector<bool> testCase(paramCount, false);

    for (int i = 0; i < paramCount; ++i) {
        for (int j = i + 1; j < paramCount; ++j) {
            testCase[i] = false;
            testCase[j] = true;
            result.push_back(testCase);
            testCase[i] = true;
            testCase[j] = false;
            result.push_back(testCase);
            testCase[i] = true;
            testCase[j] = true;
            result.push_back(testCase);
            testCase[i] = false;
            testCase[j] = false;
            result.push_back(testCase);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TDataNodeBootstrapMock
    : public IBootstrap
    , public TBootstrapBase
{
public:
    TDataNodeBootstrapMock(IBootstrap* bootstrap, IMasterConnectorPtr masterConnector)
        : TBootstrapBase(bootstrap)
        , Bootstrap_(std::move(bootstrap))
        , MasterConnector_(std::move(masterConnector))
    { }

    void Initialize() override
    {
        Bootstrap_->Initialize();
    }

    void Run() override
    {
        Bootstrap_->Run();
    }

    const TChunkStorePtr& GetChunkStore() const override
    {
        return Bootstrap_->GetChunkStore();
    }

    const IAllyReplicaManagerPtr& GetAllyReplicaManager() const override
    {
        return Bootstrap_->GetAllyReplicaManager();
    }

    const TLocationManagerPtr& GetLocationManager() const override
    {
        return Bootstrap_->GetLocationManager();
    }

    const NDataNode::IChunkMetaManagerPtr& GetChunkMetaManager() const override
    {
        return Bootstrap_->GetChunkMetaManager();
    }

    const TSessionManagerPtr& GetSessionManager() const override
    {
        return Bootstrap_->GetSessionManager();
    }

    const IJobControllerPtr& GetJobController() const override
    {
        return Bootstrap_->GetJobController();
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const TMediumDirectoryManagerPtr& GetMediumDirectoryManager() const override
    {
        return Bootstrap_->GetMediumDirectoryManager();
    }

    const TMediumUpdaterPtr& GetMediumUpdater() const override
    {
        return Bootstrap_->GetMediumUpdater();
    }

    const NConcurrency::IThroughputThrottlerPtr& GetThrottler(EDataNodeThrottlerKind kind) const override
    {
        return Bootstrap_->GetThrottler(kind);
    }

    const NConcurrency::IThroughputThrottlerPtr& GetInThrottler(const TWorkloadDescriptor& descriptor) const override
    {
        return Bootstrap_->GetInThrottler(descriptor);
    }

    const NConcurrency::IThroughputThrottlerPtr& GetOutThrottler(const TWorkloadDescriptor& descriptor) const override
    {
        return Bootstrap_->GetOutThrottler(descriptor);
    }

    const IJournalDispatcherPtr& GetJournalDispatcher() const override
    {
        return Bootstrap_->GetJournalDispatcher();
    }

    const IInvokerPtr& GetStorageLookupInvoker() const override
    {
        return Bootstrap_->GetStorageLookupInvoker();
    }

    const IInvokerPtr& GetMasterJobInvoker() const override
    {
        return Bootstrap_->GetMasterJobInvoker();
    }

    const TP2PBlockCachePtr& GetP2PBlockCache() const override
    {
        return Bootstrap_->GetP2PBlockCache();
    }

    const TP2PSnooperPtr& GetP2PSnooper() const override
    {
        return Bootstrap_->GetP2PSnooper();
    }

    const TTableSchemaCachePtr& GetTableSchemaCache() const override
    {
        return Bootstrap_->GetTableSchemaCache();
    }

    const NQueryClient::IRowComparerProviderPtr& GetRowComparerProvider() const override
    {
        return Bootstrap_->GetRowComparerProvider();
    }

    const NDataNode::IBlobReaderCachePtr& GetBlobReaderCache() const override
    {
        return Bootstrap_->GetBlobReaderCache();
    }

    const IIOThroughputMeterPtr& GetIOThroughputMeter() const override
    {
        return Bootstrap_->GetIOThroughputMeter();
    }

    const TLocationHealthCheckerPtr& GetLocationHealthChecker() const override
    {
        return Bootstrap_->GetLocationHealthChecker();
    }

    const NRpc::IOverloadControllerPtr& GetOverloadController() const override
    {
        return Bootstrap_->GetOverloadController();
    }

    void SetLocationIndexesInHeartbeatsEnabled(bool value) override
    {
        MasterConnector_->SetLocationIndexesInHeartbeatsEnabled(value);
    }

    void SetPerLocationFullHeartbeatsEnabled(bool value) override
    {
        MasterConnector_->SetPerLocationFullHeartbeatsEnabled(value);
    }

private:
    IBootstrap* const Bootstrap_;
    const IMasterConnectorPtr MasterConnector_;
};

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectorMock
    : public IMasterConnector
{
    MOCK_METHOD(void, Initialize, (), (override));

    MOCK_METHOD(void, ScheduleHeartbeat, (), (override));

    MOCK_METHOD(void, ScheduleJobHeartbeat, (const std::string& jobTrackerAddress), (override));

    MOCK_METHOD(bool, IsOnline, (), (const, override));

    MOCK_METHOD(void, SetLocationIndexesInHeartbeatsEnabled, (bool value), (override));

    MOCK_METHOD(void, SetPerLocationFullHeartbeatsEnabled, (bool value), (override));
};

////////////////////////////////////////////////////////////////////////////////

struct TCellDirectoryMock
    : public ICellDirectory
{
    DEFINE_SIGNAL_OVERRIDE(TCellReconfigurationSignature, CellDirectoryChanged);

    MOCK_METHOD(void, Update, (const NCellMasterClient::NProto::TCellDirectory& protoDirectory, bool duplicate), (override));
    MOCK_METHOD(void, UpdateDefault, (), (override));

    MOCK_METHOD(TCellId, GetPrimaryMasterCellId, (), (override));
    MOCK_METHOD(TCellTag, GetPrimaryMasterCellTag, (), (override));
    MOCK_METHOD(TCellTagList, GetSecondaryMasterCellTags, (), (override));
    MOCK_METHOD(THashSet<TCellId>, GetSecondaryMasterCellIds, (), (override));

    MOCK_METHOD(IChannelPtr, FindMasterChannel, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetMasterChannelOrThrow, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetMasterChannelOrThrow, (EMasterChannelKind, TCellId), (override));

    MOCK_METHOD(TCellTagList, GetMasterCellTagsWithRole, (EMasterCellRole), (override));

    MOCK_METHOD(TCellId, GetRandomMasterCellWithRoleOrThrow, (EMasterCellRole), (override));

    MOCK_METHOD(bool, IsClientSideCacheEnabled, (), (const, override));
    MOCK_METHOD(bool, IsMasterCacheEnabled, (), (const, override));

    MOCK_METHOD(IChannelPtr, FindNakedMasterChannel, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetNakedMasterChannelOrThrow, (EMasterChannelKind, TCellTag), (override));

    MOCK_METHOD(TSecondaryMasterConnectionConfigs, GetSecondaryMasterConnectionConfigs, (), (override));

    MOCK_METHOD(
        void,
        ReconfigureMasterCellDirectory,
        (const NCellMasterClient::TSecondaryMasterConnectionConfigs& secondaryMasterConnectionConfigs),
        (override));

    MOCK_METHOD(
        bool,
        ClusterMasterCompositionChanged,
        (const TSecondaryMasterConnectionConfigs& oldSecondaryMasterConnectionConfigs, const TSecondaryMasterConnectionConfigs& newSecondaryMasterConnectionConfigs),
        (override));
};

////////////////////////////////////////////////////////////////////////////////

class TTestNodeTrackerService
    : public TServiceBase
{
public:
    TTestNodeTrackerService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            NNodeTrackerClient::TNodeTrackerServiceProxy::GetDescriptor(),
            NLogging::TLogger("TTestNodeTrackerService"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterNode)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat)
            .SetHeavy(true));
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, RegisterNode)
    {
        response->set_node_id(NodeCounter_++);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NNodeTrackerClient::NProto, Heartbeat)
    {
        context->Reply();
    }

    std::atomic<int> NodeCounter_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TIOEngineConfig)

struct TIOEngineConfig
    : public NYTree::TYsonStruct
{
    int ReadThreadCount;
    int WriteThreadCount;
    NIO::EDirectIOPolicy UseDirectIOForReads;
    i64 MinRequestSizeToUseHugePages;

    REGISTER_YSON_STRUCT(TIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("read_thread_count", &TThis::ReadThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("write_thread_count", &TThis::WriteThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);
        registrar.Parameter("min_request_size_to_use_huge_pages", &TThis::MinRequestSizeToUseHugePages)
            .GreaterThanOrEqual(0)
            .Default(2_MB);

        registrar.Parameter("use_direct_io_for_reads", &TThis::UseDirectIOForReads)
            .Default(NIO::EDirectIOPolicy::Never);
    }
};

DEFINE_REFCOUNTED_TYPE(TIOEngineConfig)

////////////////////////////////////////////////////////////////////////////////

class TDataNodeTest
    : public ::testing::Test
{
public:
    struct TDataNodeTestParams
    {
        NIO::EIOEngineType IOEngineType = NIO::EIOEngineType::FairShareHierarchical;
        NIO::EHugeManagerType HugePageManagerType = NIO::EHugeManagerType::Transparent;
        bool EnableHugePageManager = false;
        NIO::EDirectIOPolicy UseDirectIOForReads = NIO::EDirectIOPolicy::Never;
        i64 MinRequestSizeToUseHugePages = 2_MB;
        bool EnableSequentialIORequests = true;
        i64 CoalescedReadMaxGapSize = 10_MB;
        int ClusterConnectionThreadPoolSize = 4;
        int ReadThreadCount = 1;
        int WriteThreadCount = 1;
        i64 WriteMemoryLimit = 128_GB;
        i64 ReadMemoryLimit = 128_GB;
        i64 LegacyWriteMemoryLimit = 128_GB;
        bool ChooseLocationBasedOnIOWeight = false;
        std::vector<double> IOWeights = {1.};
        std::vector<int> SessionCountLimits = {1024};
        bool UseProbePutBlocks = false;
        bool SkipWriteThrottlingLocations = false;
        bool AlwaysThrottleLocation = false;
        bool PreallocateDiskSpace = false;
        bool UseDirectIO = false;
        bool WaitPrecedingBlocksReceived = true;
    };

    TDataNodeTest() = default;

    explicit TDataNodeTest(const TDataNodeTestParams& testParams)
        : TestParams_(testParams)
    { }

    TStoreLocationConfigPtr GenerateStoreLocationConfig(double ioWeight, int sessionCountLimit)
    {
        auto storeLocationConfig = New<TStoreLocationConfig>();
        storeLocationConfig->Path = Format("%v/%v/chunk_store", RootLocationsPath_, GenerateRandomString(5, Generator_));
        storeLocationConfig->IOEngineType = TestParams_.IOEngineType;
        auto ioEngineConfig = New<TIOEngineConfig>();
        ioEngineConfig->ReadThreadCount = TestParams_.ReadThreadCount;
        ioEngineConfig->WriteThreadCount = TestParams_.WriteThreadCount;
        ioEngineConfig->UseDirectIOForReads = TestParams_.UseDirectIOForReads;
        ioEngineConfig->MinRequestSizeToUseHugePages = TestParams_.MinRequestSizeToUseHugePages;
        storeLocationConfig->IOConfig = NYTree::ConvertToNode(ioEngineConfig);
        storeLocationConfig->IOWeight = ioWeight;
        storeLocationConfig->SessionCountLimit = sessionCountLimit;
        storeLocationConfig->Throttlers = {};
        storeLocationConfig->WriteMemoryLimit = TestParams_.WriteMemoryLimit;
        storeLocationConfig->ReadMemoryLimit = TestParams_.ReadMemoryLimit;
        storeLocationConfig->LegacyWriteMemoryLimit = TestParams_.LegacyWriteMemoryLimit;
        storeLocationConfig->CoalescedReadMaxGapSize = TestParams_.CoalescedReadMaxGapSize;

        for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
            if (!storeLocationConfig->Throttlers[kind]) {
                storeLocationConfig->Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }
        return storeLocationConfig;
    }

    TClusterNodeBootstrapConfigPtr GenerateClusterBootstrapConfig()
    {
        auto bootstrapConfig = New<TClusterNodeBootstrapConfig>();
        bootstrapConfig->Flavors = {NNodeTrackerClient::ENodeFlavor::Data};
        bootstrapConfig->ClusterConnection = New<TConnectionCompoundConfig>();
        bootstrapConfig->ClusterConnection->Static = New<TConnectionStaticConfig>();
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster = New<TMasterConnectionConfig>();
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster->Addresses = {PrimaryMasterAddress};
        bootstrapConfig->ClusterConnection->Static->PrimaryMaster->CellId = CellId;

        bootstrapConfig->HugePageManager = New<NIO::THugePageManagerConfig>();
        bootstrapConfig->HugePageManager->Type = TestParams_.HugePageManagerType;
        bootstrapConfig->HugePageManager->Enabled = TestParams_.EnableHugePageManager;
        bootstrapConfig->HugePageManager->PagesPerBlob = 2;

        bootstrapConfig->ClusterConnection->Dynamic = New<TConnectionDynamicConfig>();
        bootstrapConfig->ClusterConnection->Dynamic->ThreadPoolSize = TestParams_.ClusterConnectionThreadPoolSize;

        bootstrapConfig->DataNode = New<TDataNodeConfig>();
        bootstrapConfig->DataNode->EnableSequentialIORequests = TestParams_.EnableSequentialIORequests;

        bootstrapConfig->DataNode->MasterConnector = New<TMasterConnectorConfig>();
        bootstrapConfig->DataNode->MasterConnector->JobHeartbeatPeriod = TDuration::Seconds(1);

        bootstrapConfig->DataNode->BlockCache = New<TBlockCacheConfig>();
        auto cacheConfig = New<TSlruCacheConfig>();
        cacheConfig->Capacity = 0;
        bootstrapConfig->DataNode->BlockCache->CompressedData = cacheConfig;
        bootstrapConfig->DataNode->BlockCache->UncompressedData = cacheConfig;
        bootstrapConfig->DataNode->ChooseLocationBasedOnIOWeight = TestParams_.ChooseLocationBasedOnIOWeight;
        bootstrapConfig->DataNode->SkipWriteThrottlingLocations = TestParams_.SkipWriteThrottlingLocations;

        for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
            if (!bootstrapConfig->DataNode->Throttlers[kind]) {
                bootstrapConfig->DataNode->Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        for (const auto& [ioWeight, sessionCountLimit] : Zip(TestParams_.IOWeights, TestParams_.SessionCountLimits)) {
            bootstrapConfig->DataNode->StoreLocations.push_back(GenerateStoreLocationConfig(ioWeight, sessionCountLimit));
        }

        bootstrapConfig->ResourceLimits->TotalMemory = 256_GB;
        for (auto kind : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            bootstrapConfig->ResourceLimits->MemoryLimits[kind] = New<TMemoryLimit>();
            bootstrapConfig->ResourceLimits->MemoryLimits[kind]->Value = 128_GB;
        }

        return bootstrapConfig;
    }

    void SetUp() override
    {
        ActionQueue_ = New<NConcurrency::TActionQueue>();
        auto nodeTrackerService = New<TTestNodeTrackerService>(ActionQueue_->GetInvoker());

        auto cellDirectoryMock = New<TCellDirectoryMock>();

        auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
        nodeDirectory->AddDescriptor(
            NYT::NNodeTrackerClient::TNodeId(0),
            NNodeTrackerClient::TNodeDescriptor(std::string(PrimaryMasterAddress)));

        auto masterChannelFactory = CreateTestChannelFactory(
            THashMap<std::string, IServicePtr>{{PrimaryMasterAddress, nodeTrackerService}},
            THashMap<std::string, IServicePtr>());

        MemoryTracker_ = CreateNodeMemoryTracker(32_MB, New<TNodeMemoryTrackerConfig>(), {});

        TestConnection_ = CreateConnection(
            std::move(masterChannelFactory),
            /*networkPreferenceList*/ {"default"},
            std::move(nodeDirectory),
            /*nodeStatusDirectory*/ nullptr,
            ActionQueue_->GetInvoker(),
            MemoryTracker_);

        EXPECT_CALL(*TestConnection_, CreateNativeClient).WillRepeatedly([this] (const NApi::NNative::TClientOptions& options) -> NApi::NNative::IClientPtr
            {
                return New<TClient>(TestConnection_, options, MemoryTracker_);
            });
        EXPECT_CALL(*TestConnection_, GetClusterDirectory).WillRepeatedly(testing::DoDefault());
        EXPECT_CALL(*TestConnection_, SubscribeReconfigured).WillRepeatedly(testing::DoDefault());
        EXPECT_CALL(*TestConnection_, GetPrimaryMasterCellTag).WillRepeatedly([] () -> TCellTag {
            return TCellTag(0xf003);
        });
        EXPECT_CALL(*TestConnection_, GetMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        EXPECT_CALL(*cellDirectoryMock, GetPrimaryMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        EXPECT_CALL(*TestConnection_, GetPrimaryMasterCellId).WillRepeatedly([] () -> TCellId {
            return CellId;
        });
        CellDirectoryMock_ = cellDirectoryMock;
        EXPECT_CALL(*TestConnection_, GetMasterCellDirectory).WillRepeatedly([this] () -> const NCellMasterClient::ICellDirectoryPtr& {
            return CellDirectoryMock_;
        });
        EXPECT_CALL(*TestConnection_, GetSecondaryMasterCellTags).WillRepeatedly([] () -> TCellTagList {
            return {};
        });

        ClusterNodeBootstrap_ = NClusterNode::CreateNodeBootstrap(
            GenerateClusterBootstrapConfig(),
            NYTree::GetEphemeralNodeFactory(false)->CreateMap(),
            NFusion::CreateServiceDirectory(),
            TestConnection_);
        ClusterNodeBootstrap_->Initialize();

        MasterConnectorMock_ = New<TMasterConnectorMock>();
        EXPECT_CALL(*MasterConnectorMock_, IsOnline).WillRepeatedly(testing::Return(true));
        DataNodeBootstrap_ = New<TDataNodeBootstrapMock>(ClusterNodeBootstrap_->GetDataNodeBootstrap(), MasterConnectorMock_);

        // if (CellDirectoryMock_) {
        //     testing::Mock::AllowLeak(CellDirectoryMock_.Get());
        // }

        if (MasterConnectorMock_) {
            testing::Mock::AllowLeak(MasterConnectorMock_.Get());
        }

        if (TestConnection_) {
            testing::Mock::AllowLeak(TestConnection_.Get());
        }

        DataNodeService_ = CreateDataNodeService(DataNodeBootstrap_->GetConfig()->DataNode, DataNodeBootstrap_.Get());
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->UseProbePutBlocks = TestParams_.UseProbePutBlocks;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->TestingOptions->AlwaysThrottleLocation = TestParams_.AlwaysThrottleLocation;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->TestingOptions->DelayBeforePerformPutBlocks = TDuration::Seconds(1);
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->PreallocateDiskSpace = TestParams_.PreallocateDiskSpace;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->UseDirectIO = TestParams_.UseDirectIO;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->WaitPrecedingBlocksReceived = TestParams_.WaitPrecedingBlocksReceived;
        ChannelFactory_ = CreateTestChannelFactory(
            THashMap<std::string, IServicePtr>{{DataNodeServiceAddress, DataNodeService_}},
            THashMap<std::string, IServicePtr>());
    }

    void TearDown() override
    {
        MemoryTracker_->ClearTrackers();
        MemoryTracker_.Reset();
        WorkloadDescriptor_ = {};
        ChannelFactory_.Reset();
        DataNodeService_->Stop().Wait();
        DataNodeService_.Reset();
        DataNodeBootstrap_.Reset();
        ClusterNodeBootstrap_.Reset();
        ActionQueue_->Shutdown(true);
        ActionQueue_.Reset();
        CellDirectoryMock_.Reset();
        MasterConnectorMock_.Reset();
        TestConnection_.Reset();

        NFS::RemoveRecursive(RootLocationsPath_);
    }

    const NDataNode::IBootstrapPtr& GetDataNodeBootstrap() const {
        return DataNodeBootstrap_;
    }

    const NConcurrency::TActionQueuePtr& GetActionQueue() const
    {
        return ActionQueue_;
    }

    auto StartChunk(const TSessionId& sessionId, bool useProbePutBlocks, bool preallocateDiskSpace, bool useDirectIo)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.StartChunk();
        req->set_use_probe_put_blocks(useProbePutBlocks);
        req->set_preallocate_disk_space(preallocateDiskSpace);
        req->set_use_direct_io(useDirectIo);
        ToProto(req->mutable_session_id(), sessionId);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);

        return req->Invoke();
    }

    auto ProbePutBlocks(const TSessionId& sessionId, i64 cumulativeBlockSize)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.ProbePutBlocks();
        req->set_cumulative_block_size(cumulativeBlockSize);
        ToProto(req->mutable_session_id(), sessionId);
        return req->Invoke();
    }

    auto PutBlocks(const TSessionId& sessionId, const std::vector<TBlock>& blocks, int firstBlockIndex, i64 cumulativeBlockSize)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.PutBlocks();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_first_block_index(firstBlockIndex);
        req->set_cumulative_block_size(cumulativeBlockSize);
        req->SetTimeout(RequestTimeout_);
        SetRpcAttachedBlocks(req, blocks);

        return req->Invoke();
    }

    auto FlushBlocks(const TSessionId& sessionId, int blockIndex)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.FlushBlocks();
        req->set_block_index(blockIndex);
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto FinishChunk(const TSessionId& sessionId, int blockCount)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.FinishChunk();
        auto deferredMeta = New<TDeferredChunkMeta>();
        deferredMeta->set_type(ToProto(EChunkType::File));
        deferredMeta->set_format(ToProto(EChunkFormat::FileDefault));
        *deferredMeta->mutable_extensions() = {};
        *req->mutable_chunk_meta() = *deferredMeta;
        req->set_block_count(blockCount);
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto CancelChunk(const TSessionId& sessionId)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.CancelChunk();
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);

        return req->Invoke();
    }

    auto GetBlockSet(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndices,
        bool populateCache,
        bool fetchFromCache,
        bool fetchFromDisk)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.GetBlockSet();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->SetMultiplexingParallelism(1);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);
        ToProto(req->mutable_chunk_id(), chunkId);
        for (auto blockIndex : blockIndices) {
            req->add_block_indexes(blockIndex);
        }
        req->Header().set_timeout(ToProto(RequestTimeout_));
        req->Header().set_start_time(ToProto(TInstant::Now()));
        req->SetTimeout(RequestTimeout_);
        req->set_populate_cache(populateCache);
        req->set_fetch_from_cache(fetchFromCache);
        req->set_fetch_from_disk(fetchFromDisk);

        TGuid readSessionId{};
        readSessionId.Parts32[0] = Counter_++;
        ToProto(req->mutable_read_session_id(), readSessionId);

        return req->Invoke();
    }

    std::vector<TBlock> FillWithRandomBlocks(TSessionId sessionId, int blockCount, int blockSize, bool useProbePutBlocks = false, bool preallocateDiskSpace = false, bool useDirectIo = false)
    {
        auto blocks = CreateBlocks(blockCount, blockSize, Generator_);
        auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);
        WaitFor(StartChunk(sessionId, useProbePutBlocks, preallocateDiskSpace, useDirectIo))
            .ThrowOnError();
        if (useProbePutBlocks) {
            int approvedCumulativeBlockSize = 0;
            do {
                auto resp = WaitFor(ProbePutBlocks(sessionId, cummulativeBlockSize));
                approvedCumulativeBlockSize = resp.Value()->probe_put_blocks_state().approved_cumulative_block_size();
            } while (approvedCumulativeBlockSize < cummulativeBlockSize);
        }
        std::vector<TFuture<void>> putBlocks(blocks.size());
        std::vector<int> indices(blocks.size());
        std::iota(std::begin(indices), std::end(indices), 0);
        std::random_shuffle(std::begin(indices), std::end(indices));
        for (auto i : indices) {
            putBlocks[i] = PutBlocks(sessionId, {blocks[i]}, i, cummulativeBlockSize).AsVoid();
        }
        WaitFor(AllSucceeded(putBlocks))
            .ThrowOnError();
        WaitFor(FlushBlocks(sessionId, blockCount - 1))
            .ThrowOnError();
        WaitFor(FinishChunk(sessionId, blockCount))
            .ThrowOnError();
        return blocks;
    }

private:
    static constexpr TCellId CellId{0xa0f7b159, 0x17c04060, 0x41a0259, 0x46d5f0b7};
    static constexpr const char* DataNodeServiceAddress = "local:1045";
    static constexpr const char* PrimaryMasterAddress = "local:1081";

    const TDuration RequestTimeout_ = TDuration::Seconds(30);
    const TDataNodeTestParams TestParams_;

    TRandomGenerator Generator_{RandomNumber<ui64>()};

    TString RootLocationsPath_ = Format("/tmp/locations/%v", GenerateRandomString(5, Generator_));

    std::atomic<int> Counter_ = 0;

    INodeMemoryTrackerPtr MemoryTracker_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    NClusterNode::IBootstrapPtr ClusterNodeBootstrap_;
    NDataNode::IBootstrapPtr DataNodeBootstrap_;
    IServicePtr DataNodeService_;
    IChannelFactoryPtr ChannelFactory_;
    TWorkloadDescriptor WorkloadDescriptor_;
    NCellMasterClient::ICellDirectoryPtr CellDirectoryMock_;
    TIntrusivePtr<TMasterConnectorMock> MasterConnectorMock_;
    TIntrusivePtr<TTestConnection> TestConnection_;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetBlockSetTestCase
{
    int BlockCount = 100;
    int BlockSize = 2_KB;
    int ParallelGetBlockSetCount = 50;
    int BlocksInRequest = 25;
    bool PopulateCache = true;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
    bool EnableSequentialIORequests = true;
    bool UseProbePutBlocks = false;
    bool PreallocateDiskSpace = false;
    bool WaitPrecedingBlocksReceived = true;
    NIO::EHugeManagerType HugePageManagerType = NIO::EHugeManagerType::Transparent;
    bool EnableHugePageManager = false;
    bool UseDirectIOForReads = false;
    bool UseFairShareIOEngine = false;
    i64 MinRequestSizeToUseHugePages = 1_KB;
};

std::vector<TGetBlockSetTestCase> GenerateGetBlockSetParams()
{
    const std::vector<std::vector<bool>> testCases = GeneratePairWiseCases(9);
    std::vector<TGetBlockSetTestCase> result;
    result.reserve(testCases.size());

    for (const auto& testCase : testCases) {
        TGetBlockSetTestCase getblockSetTestCase;
        getblockSetTestCase.PopulateCache = testCase[0];
        getblockSetTestCase.FetchFromCache = testCase[1];
        getblockSetTestCase.EnableSequentialIORequests = testCase[2];
        getblockSetTestCase.UseProbePutBlocks = testCase[3];
        getblockSetTestCase.WaitPrecedingBlocksReceived = testCase[4];
        getblockSetTestCase.PreallocateDiskSpace = testCase[5];
        getblockSetTestCase.EnableHugePageManager = testCase[6];
        getblockSetTestCase.UseDirectIOForReads = testCase[7];
        getblockSetTestCase.UseFairShareIOEngine = testCase[8];
        result.push_back(getblockSetTestCase);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TGetBlockSetTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TGetBlockSetTestCase>
{
public:
    TGetBlockSetTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .IOEngineType = GetParam().UseFairShareIOEngine ? NIO::EIOEngineType::FairShareHierarchical : NIO::EIOEngineType::ThreadPool,
                .HugePageManagerType = GetParam().HugePageManagerType,
                .EnableHugePageManager = GetParam().EnableHugePageManager,
                .UseDirectIOForReads = GetParam().UseDirectIOForReads ? NIO::EDirectIOPolicy::Always : NIO::EDirectIOPolicy::Never,
                .MinRequestSizeToUseHugePages = GetParam().MinRequestSizeToUseHugePages,
                .EnableSequentialIORequests = GetParam().EnableSequentialIORequests,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .UseProbePutBlocks = GetParam().UseProbePutBlocks,
                .PreallocateDiskSpace = GetParam().PreallocateDiskSpace,
                .WaitPrecedingBlocksReceived = GetParam().WaitPrecedingBlocksReceived,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TGetBlockSetGapTestCase
{
    int BlockCount = 40;
    int BlockSize = 1_KB;
    bool PopulateCache = true;
    bool FetchFromCache = true;
    bool FetchFromDisk = true;
    bool EnableSequentialIORequests = true;
    i64 CoalescedReadMaxGapSize = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TGetBlockSetGapTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TGetBlockSetGapTestCase>
{
public:
    TGetBlockSetGapTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .EnableSequentialIORequests = GetParam().EnableSequentialIORequests,
                .CoalescedReadMaxGapSize = GetParam().CoalescedReadMaxGapSize,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TIOWeightTestCase
{
    std::vector<double> IOWeights = {1., 1.};
    std::vector<int> SessionCountLimits = {128, 128};
};

////////////////////////////////////////////////////////////////////////////////

class TIOWeightTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TIOWeightTestCase>
{
public:
    TIOWeightTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .ChooseLocationBasedOnIOWeight = true,
                .IOWeights = GetParam().IOWeights,
                .SessionCountLimits = GetParam().SessionCountLimits,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TSkipWriteThrottlingLocationsTestCase
{
    std::vector<double> IOWeights = {1., 1.};
    std::vector<int> SessionCountLimits = {128, 128};
    bool SkipWriteThrottlingLocations = false;
    bool AlwaysThrottleLocation = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSkipWriteThrottlingLocationsTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TSkipWriteThrottlingLocationsTestCase>
{
public:
    TSkipWriteThrottlingLocationsTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .ChooseLocationBasedOnIOWeight = true,
                .IOWeights = GetParam().IOWeights,
                .SessionCountLimits = GetParam().SessionCountLimits,
                .SkipWriteThrottlingLocations = GetParam().SkipWriteThrottlingLocations,
                .AlwaysThrottleLocation = GetParam().AlwaysThrottleLocation,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteTestCase
{
    int BlockCount = 40;
    int BlockSize = 1_KB;
    bool UseProbePutBlocks = false;
    bool PreallocateDiskSpace = false;
    bool UseDirectIo = false;
};

std::vector<TWriteTestCase> GenerateWriteTestParams()
{
    const std::vector<std::vector<bool>> testCases = GeneratePairWiseCases(3);
    std::vector<TWriteTestCase> result;
    result.reserve(testCases.size());

    for (const auto& testCase : testCases) {
        TWriteTestCase writeTestCase;
        writeTestCase.UseProbePutBlocks = testCase[0];
        writeTestCase.PreallocateDiskSpace = testCase[1];
        writeTestCase.UseDirectIo = testCase[2];
        result.push_back(writeTestCase);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TWriteTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TWriteTestCase>
{
public:
    TWriteTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .EnableHugePageManager = GetParam().UseDirectIo,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .UseProbePutBlocks = GetParam().UseProbePutBlocks,
                .PreallocateDiskSpace = GetParam().PreallocateDiskSpace,
                .UseDirectIO = GetParam().UseDirectIo,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TSkipWriteThrottlingLocationsTest, SkipThrottlingLocationsOnStartChunk)
{
    const auto& ioWeights = GetParam().IOWeights;
    auto alwaysThrottleLocation = GetParam().AlwaysThrottleLocation;
    auto enableSkipWriteThrottlingLocations = GetParam().SkipWriteThrottlingLocations;
    const auto& locations = GetDataNodeBootstrap()->GetChunkStore()->Locations();

    YT_VERIFY(std::ssize(ioWeights) == std::ssize(locations));

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto rspOrError = WaitFor(StartChunk(sessionId, true, false, false));

    if (alwaysThrottleLocation && enableSkipWriteThrottlingLocations) {
        EXPECT_FALSE(rspOrError.IsOK());
    } else {
        EXPECT_TRUE(rspOrError.IsOK());
    }
}

INSTANTIATE_TEST_SUITE_P(
    TSkipWriteThrottlingLocationsTest,
    TSkipWriteThrottlingLocationsTest,
    ::testing::Values(
        TSkipWriteThrottlingLocationsTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .SkipWriteThrottlingLocations = true,
            .AlwaysThrottleLocation = true,
        },
        TSkipWriteThrottlingLocationsTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .SkipWriteThrottlingLocations = false,
            .AlwaysThrottleLocation = true,
        },
        TSkipWriteThrottlingLocationsTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .SkipWriteThrottlingLocations = true,
            .AlwaysThrottleLocation = false,
        },
        TSkipWriteThrottlingLocationsTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .SkipWriteThrottlingLocations = false,
            .AlwaysThrottleLocation = false
        }
    )
);

TEST_P(TIOWeightTest, IoBasedOnIoWeight)
{
    auto& ioWeights = GetParam().IOWeights;
    auto& locations = GetDataNodeBootstrap()->GetChunkStore()->Locations();

    YT_VERIFY(std::ssize(ioWeights) == std::ssize(locations));

    std::vector<TFuture<void>> futures;
    futures.resize(256);

    for (auto& future : futures) {
        TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
        future = BIND(&TDataNodeTest::FillWithRandomBlocks, this, sessionId, 1, 1_KB, false, false, false)
            .AsyncVia(GetActionQueue()->GetInvoker())
            .Run()
            .AsVoid();
    }

    for (const auto& future : futures) {
        future.Wait();
    }

    std::vector<double> usedSpaces;
    usedSpaces.reserve(std::ssize(locations));

    for (const auto& location : locations) {
        usedSpaces.push_back(location->GetUsedSpace());
    }

    auto sortedUsedSpaces = usedSpaces;

    std::sort(sortedUsedSpaces.begin(), sortedUsedSpaces.end());

    EXPECT_EQ(sortedUsedSpaces, usedSpaces);
}

INSTANTIATE_TEST_SUITE_P(
    TIOWeightTest,
    TIOWeightTest,
    ::testing::Values(
        TIOWeightTestCase{
            .IOWeights = {0.0001, 0.001, 0.01, 0.1, 1.},
            .SessionCountLimits = {1024, 1024, 1024, 1024, 1024},
        },
        TIOWeightTestCase{
            .IOWeights = {0.2, 0.5},
            .SessionCountLimits = {256, 256},
        },
        TIOWeightTestCase{
            .IOWeights = {0.2, 0.6, 1.5},
            .SessionCountLimits = {128, 128, 128},
        },
        TIOWeightTestCase{
            .IOWeights = {1., 1.},
            .SessionCountLimits = {16, 256},
        }
    )
);

TEST_P(TGetBlockSetGapTest, GetBlocksByOneDiskIORequestTest)
{
    auto testCase = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    int blockCount = testCase.BlockCount;
    int blockSize = testCase.BlockSize;
    auto blocks = FillWithRandomBlocks(sessionId, blockCount, blockSize);

    std::vector<int> blockIndices(blockCount);
    if (testCase.CoalescedReadMaxGapSize >= blockSize) {
        blockIndices.resize(blockCount / 2);
        for (int i = 0; i < std::ssize(blockIndices); i++) {
            blockIndices[i] = i * 2;
        }
    } else {
        for (int i = 0; i < std::ssize(blockIndices); i++) {
            blockIndices[i] = i;
        }
    }
    std::vector<TBlock> fetchedBlocks(blockIndices.size());
    std::transform(blockIndices.begin(), blockIndices.end(), fetchedBlocks.begin(), [&] (int blockIndex) {
        return blocks[blockIndex];
    });
    auto rspOrError = WaitFor(GetBlockSet(sessionId.ChunkId, blockIndices, testCase.PopulateCache, testCase.FetchFromCache, testCase.FetchFromDisk));
    YT_VERIFY(rspOrError.IsOK());
    auto rsp = rspOrError.Value();
    auto chunkReaderStatistics = rsp->chunk_reader_statistics();
    EXPECT_EQ(chunkReaderStatistics.data_io_requests(), 1);
    auto gotBlocks = GetRpcAttachedBlocks(rsp);
    EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
    EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
}

INSTANTIATE_TEST_SUITE_P(
    TGetBlockSetGapTest,
    TGetBlockSetGapTest,
    ::testing::Values(
        TGetBlockSetGapTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = true,
        },
        TGetBlockSetGapTestCase{
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false,
        },
        TGetBlockSetGapTestCase{
            .BlockSize = 1_KB,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false,
            .CoalescedReadMaxGapSize = 1_KB,
        },
        TGetBlockSetGapTestCase{
            .BlockSize = 1_KB,
            .PopulateCache = false,
            .FetchFromCache = false,
            .FetchFromDisk = true,
            .EnableSequentialIORequests = false,
            .CoalescedReadMaxGapSize = 1_KB / 2,
        }
    )
);

TEST_P(TGetBlockSetTest, GetBlockSetTest)
{
    auto testCase = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    TRandomGenerator generator(42);
    int blockCount = testCase.BlockCount;
    int blkSize = testCase.BlockSize;
    auto blocks = FillWithRandomBlocks(sessionId, blockCount, blkSize, testCase.UseProbePutBlocks, testCase.PreallocateDiskSpace);

    int getBlockSetCount = testCase.ParallelGetBlockSetCount;
    std::vector<TFuture<void>> getBlockSetFutures;
    getBlockSetFutures.reserve(getBlockSetCount);
    for (int i = 0; i < getBlockSetCount; ++i) {
        auto blockIndices = GenerateRandomBlockIdsWithOrder(0, blocks.size() - 1, testCase.BlocksInRequest, generator);
        std::vector<TBlock> fetchedBlocks(blockIndices.size());
        for (int i = 0; i < std::ssize(blockIndices); ++i) {
            fetchedBlocks[i] = blocks[blockIndices[i]];
        }
        auto future = GetBlockSet(sessionId.ChunkId, blockIndices, testCase.PopulateCache, testCase.FetchFromCache, testCase.FetchFromDisk)
            .Apply(BIND([fetchedBlocks = std::move(fetchedBlocks)] (const TIntrusivePtr<TTypedClientResponse<TRspGetBlockSet>>& response) {
                if (response->disk_throttling() || response->net_throttling()) {
                    return;
                }
                auto gotBlocks = GetRpcAttachedBlocks(response);
                EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
                EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
        }));
        getBlockSetFutures.push_back(std::move(future));
    }

    auto allsucceededGetBlockSetFutures = AllSucceeded(getBlockSetFutures);
    EXPECT_TRUE(allsucceededGetBlockSetFutures.Wait(TDuration::Seconds(60)));
    auto getBlockSetFuturesResult = allsucceededGetBlockSetFutures.TryGet();
    EXPECT_TRUE(getBlockSetFuturesResult.has_value());
    EXPECT_TRUE(getBlockSetFuturesResult.has_value() && getBlockSetFuturesResult->IsOK());

    if (testCase.EnableHugePageManager) {
        if (testCase.HugePageManagerType == NIO::EHugeManagerType::Transparent && testCase.UseDirectIOForReads) {
            YT_VERIFY(GetDataNodeBootstrap()->GetHugePageManager()->GetHugePageSize() > 0);
            EXPECT_GT(GetDataNodeBootstrap()->GetHugePageManager()->GetUsedHugePageCount(), 0);
        }
    } else {
        EXPECT_EQ(GetDataNodeBootstrap()->GetHugePageManager()->GetUsedHugePageCount(), 0);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TGetBlockSetTest,
    TGetBlockSetTest,
    ::testing::ValuesIn(GenerateGetBlockSetParams())
);

TEST_F(TDataNodeTest, ProbePutBlocksCancelChunk)
{
    std::vector<TSessionId> sessionIds;

    for (int i = 0; i < 100; ++i) {
        TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
        sessionIds.push_back(sessionId);
        WaitFor(StartChunk(sessionId, true, false, false))
            .ThrowOnError();
    }

    for (const auto& sessionId: sessionIds) {
        for (int i = 0; i < 100; ++i) {
            YT_UNUSED_FUTURE(ProbePutBlocks(sessionId, 5_GB + RandomNumber<unsigned>() % 1000 * 100_MB));
        }
    }

    for (int i = 0; i < std::ssize(sessionIds) / 2; ++i) {
        WaitFor(CancelChunk(sessionIds[i]))
            .ThrowOnError();
    }

    for (int i = std::ssize(sessionIds) / 2; i < std::ssize(sessionIds); ++i) {
        for (int j = 0; j < 100; ++j) {
            YT_UNUSED_FUTURE(ProbePutBlocks(sessionIds[i],  5_GB + RandomNumber<unsigned>() % 1000 * 100_MB));
        }
    }

    for (int i = std::ssize(sessionIds) / 2; i < std::ssize(sessionIds); ++i) {
        WaitFor(CancelChunk(sessionIds[i]))
            .ThrowOnError();
    }
}

TEST_F(TDataNodeTest, PutBlocksCancelChunk)
{
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    WaitFor(StartChunk(sessionId, true, false, false))
        .ThrowOnError();

    TRandomGenerator generator{RandomNumber<ui64>()};

    auto blocks = CreateBlocks(100, 1_KB, generator);
    auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);

    auto putBlocksEnd = PutBlocks(sessionId, {blocks[0]}, 10, cummulativeBlockSize);
    auto putBlocksBeging = PutBlocks(sessionId, {blocks[0]}, 0, cummulativeBlockSize);

    WaitFor(CancelChunk(sessionId))
        .ThrowOnError();

    putBlocksEnd.Cancel(TError("Timeout"));

    EXPECT_THROW(WaitFor(putBlocksBeging).ThrowOnError(), NYT::TErrorException);
}

TEST_F(TDataNodeTest, ProbePutBlocksFinishChunk)
{
    std::vector<TSessionId> sessionIds;

    for (int i = 0; i < 100; ++i) {
        TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
        sessionIds.push_back(sessionId);
        WaitFor(StartChunk(sessionId, true, false, false))
            .ThrowOnError();
    }

    for (const auto& sessionId: sessionIds) {
        for (int i = 0; i < 100; ++i) {
            YT_UNUSED_FUTURE(ProbePutBlocks(sessionId, 5_GB + RandomNumber<unsigned>() % 1000 * 100_MB));
        }
    }

    for (int i = 0; i < std::ssize(sessionIds) / 2; ++i) {
        WaitFor(FinishChunk(sessionIds[i], 0))
            .ThrowOnError();
    }

    for (int i = std::ssize(sessionIds) / 2; i < std::ssize(sessionIds); ++i) {
        for (int j = 0; j < 100; ++j) {
            YT_UNUSED_FUTURE(ProbePutBlocks(sessionIds[i], 5_GB + RandomNumber<unsigned>() % 1000 * 100_MB));
        }
    }

    for (int i = std::ssize(sessionIds) / 2; i < std::ssize(sessionIds); ++i) {
        WaitFor(FinishChunk(sessionIds[i], 0))
            .ThrowOnError();
    }
}

TEST_P(TWriteTest, RandomWrite)
{
    auto testCase = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    int blockCount = testCase.BlockCount;
    int blockSize = testCase.BlockSize;
    auto blocks = FillWithRandomBlocks(
        sessionId,
        blockCount,
        blockSize,
        testCase.UseProbePutBlocks,
        testCase.PreallocateDiskSpace,
        testCase.UseDirectIo);

    std::vector<int> blockIndices(blockCount);
    std::iota(blockIndices.begin(), blockIndices.end(), 0);
    std::vector<TBlock> fetchedBlocks(blockCount);
    std::transform(blockIndices.begin(), blockIndices.end(), fetchedBlocks.begin(), [&] (int blockIndex) {
        return blocks[blockIndex];
    });
    auto rspOrError = WaitFor(GetBlockSet(sessionId.ChunkId, blockIndices, true, true, true));
    YT_VERIFY(rspOrError.IsOK());
    auto rsp = rspOrError.Value();
    auto chunkReaderStatistics = rsp->chunk_reader_statistics();
    EXPECT_EQ(chunkReaderStatistics.data_io_requests(), 1);
    auto gotBlocks = GetRpcAttachedBlocks(rsp);
    EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
    EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
}

INSTANTIATE_TEST_SUITE_P(
    TWriteTest,
    TWriteTest,
    ::testing::ValuesIn(GenerateWriteTestParams())
);

////////////////////////////////////////////////////////////////////////////////

}
