#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/data_node_service.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/master_connector.h>
#include <yt/yt/server/node/data_node/network_statistics.h>
#include <yt/yt/server/node/data_node/session.h>
#include <yt/yt/server/node/data_node/session_manager.h>

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
#include <yt/yt/core/concurrency/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_proxy_service.h>

#include <yt/yt/core/misc/fair_share_hierarchical_queue.h>
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

std::vector<std::vector<bool>> GeneratePairwiseCases(int paramCount)
{
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

    TNetThrottlingResult CheckNetOutThrottling(
        i64 pendingOutBytes,
        const std::string& networkName,
        const TWorkloadDescriptor& workloadDescriptor,
        bool incrementCounter = true) const override
    {
        return Bootstrap_->CheckNetOutThrottling(
            pendingOutBytes,
            networkName,
            workloadDescriptor,
            incrementCounter);
    }

    TNetThrottlingResult CheckNetInThrottling(
        const std::string& networkName,
        const TWorkloadDescriptor& workloadDescriptor,
        bool incrementCounter = true) const override
    {
        return Bootstrap_->CheckNetInThrottling(
            networkName,
            workloadDescriptor,
            incrementCounter);
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

    MOCK_METHOD(IChannelPtr, FindNonRetryingMasterChannel, (EMasterChannelKind, TCellTag), (override));
    MOCK_METHOD(IChannelPtr, GetNonRetryingMasterChannelOrThrow, (EMasterChannelKind, TCellTag), (override));

    MOCK_METHOD(TSecondaryMasterConnectionConfigs, GetSecondaryMasterConnectionConfigs, (), (override));

    MOCK_METHOD(
        void,
        ReconfigureMasterCellDirectory,
        (const NCellMasterClient::TSecondaryMasterConnectionConfigs& secondaryMasterConnectionConfigs),
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
    int FairShareThreadCount;

    int ReadThreadCount;
    int WriteThreadCount;
    NIO::EDirectIOPolicy UseDirectIOForReads;
    NIO::EDirectIOPolicy UseDirectIOForWrites;
    i64 MinRequestSizeToUseHugePages;

    // Request size in bytes.
    i64 DesiredRequestSize;
    i64 MinRequestSize;
    bool EnableSlicing;

    REGISTER_YSON_STRUCT(TIOEngineConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("fair_share_thread_count", &TThis::FairShareThreadCount)
            .GreaterThanOrEqual(1)
            .Default(1);

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
        registrar.Parameter("use_direct_io_for_writes", &TThis::UseDirectIOForWrites)
            .Default(NIO::EDirectIOPolicy::Never);

        registrar.Parameter("desired_request_size", &TThis::DesiredRequestSize)
            .GreaterThanOrEqual(4_KB)
            .Default(8_MB);
        registrar.Parameter("min_request_size", &TThis::MinRequestSize)
            .GreaterThanOrEqual(512)
            .Default(4_MB);
        registrar.Parameter("enable_slicing", &TThis::EnableSlicing)
            .Default(true);
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
        NIO::EDirectIOPolicy UseDirectIOForWrites = NIO::EDirectIOPolicy::Never;
        i64 MinRequestSizeToUseHugePages = 2_MB;
        bool EnableSequentialIORequests = true;
        i64 CoalescedReadMaxGapSize = 10_MB;
        i64 BlockCacheCapacity = 0;
        bool RejectOversizedBlockCacheItems = false;
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
        bool EnableWriteThrottlingWritableCheck = false;
        bool EnableInThrottlerQueueWritableCheck = false;
        bool PreallocateDiskSpace = false;
        bool WaitPrecedingBlocksReceived = true;
        TEnumIndexedArray<EWorkloadCategory, std::optional<double>> FairShareWorkloadCategoryWeights;
        TDuration DelayBeforePerformPutBlocks = TDuration::Seconds(2);
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
        ioEngineConfig->UseDirectIOForWrites = TestParams_.UseDirectIOForWrites;
        ioEngineConfig->MinRequestSizeToUseHugePages = TestParams_.MinRequestSizeToUseHugePages;
        storeLocationConfig->IOConfig = NYTree::ConvertToNode(ioEngineConfig);
        storeLocationConfig->IOWeight = ioWeight;
        storeLocationConfig->SessionCountLimit = sessionCountLimit;
        storeLocationConfig->Throttlers = {};
        storeLocationConfig->WriteMemoryLimit = TestParams_.WriteMemoryLimit;
        storeLocationConfig->ReadMemoryLimit = TestParams_.ReadMemoryLimit;
        storeLocationConfig->LegacyWriteMemoryLimit = TestParams_.LegacyWriteMemoryLimit;
        storeLocationConfig->CoalescedReadMaxGapSize = TestParams_.CoalescedReadMaxGapSize;
        storeLocationConfig->FairShareWorkloadCategoryWeights = TestParams_.FairShareWorkloadCategoryWeights;

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
        cacheConfig->Capacity = TestParams_.BlockCacheCapacity;
        cacheConfig->RejectOversizedItems = TestParams_.RejectOversizedBlockCacheItems;
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
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->EnableWriteThrottlingWritableCheck = TestParams_.EnableWriteThrottlingWritableCheck;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->EnableInThrottlerQueueWritableCheck = TestParams_.EnableInThrottlerQueueWritableCheck;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->TestingOptions->DelayBeforePerformPutBlocks = TestParams_.DelayBeforePerformPutBlocks;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->PreallocateDiskSpace = TestParams_.PreallocateDiskSpace;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->WaitPrecedingBlocksReceived = TestParams_.WaitPrecedingBlocksReceived;
        DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->FairShareHierarchicalScheduler->WindowSize = TDuration::Seconds(1);
        DataNodeBootstrap_->GetFairShareHierarchicalScheduler()->Reconfigure(DataNodeBootstrap_->GetDynamicConfigManager()->GetConfig()->FairShareHierarchicalScheduler);
        ChannelFactory_ = CreateTestChannelFactory(
            THashMap<std::string, IServicePtr>{{DataNodeServiceAddress, DataNodeService_}},
            THashMap<std::string, IServicePtr>());
    }

    void TearDown() override
    {
        MemoryTracker_->ClearTrackers();
        MemoryTracker_.Reset();
        ChannelFactory_.Reset();
        DataNodeService_->Stop().BlockingWait();
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

    auto StartChunk(const TSessionId& sessionId, bool useProbePutBlocks, bool preallocateDiskSpace, bool useDirectIo, TWorkloadDescriptor workloadDescriptor = {}, bool disableSendBlocks = false)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.StartChunk();
        req->set_use_probe_put_blocks(useProbePutBlocks);
        req->set_preallocate_disk_space(preallocateDiskSpace);
        req->set_use_direct_io(useDirectIo);
        req->set_disable_send_blocks(disableSendBlocks);
        ToProto(req->mutable_session_id(), sessionId);
        SetRequestWorkloadDescriptor(req, workloadDescriptor);

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

    auto PingSession(const TSessionId& sessionId)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.PingSession();
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

    auto CancelChunk(const TSessionId& sessionId, bool waitForCancelation = false)
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.CancelChunk();
        req->SetTimeout(RequestTimeout_);
        ToProto(req->mutable_session_id(), sessionId);
        req->set_wait_for_cancelation(waitForCancelation);

        return req->Invoke();
    }

    auto GetBlockSet(
        const TChunkId& chunkId,
        const std::vector<int>& blockIndices,
        bool populateCache,
        bool fetchFromCache,
        bool fetchFromDisk,
        TWorkloadDescriptor workloadDescriptor = {},
        std::optional<TDuration> requestTimeout = {})
    {
        auto channel = ChannelFactory_->CreateChannel(DataNodeServiceAddress);
        TDataNodeServiceProxy proxy(channel);

        auto req = proxy.GetBlockSet();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->SetMultiplexingParallelism(1);
        SetRequestWorkloadDescriptor(req, workloadDescriptor);
        ToProto(req->mutable_chunk_id(), chunkId);
        for (auto blockIndex : blockIndices) {
            req->add_block_indexes(blockIndex);
        }
        auto timeout = requestTimeout.value_or(RequestTimeout_);
        req->Header().set_timeout(ToProto(timeout));
        req->Header().set_start_time(ToProto(TInstant::Now()));
        req->SetTimeout(timeout);
        req->set_populate_cache(populateCache);
        req->set_fetch_from_cache(fetchFromCache);
        req->set_fetch_from_disk(fetchFromDisk);

        TGuid readSessionId{};
        readSessionId.Parts32[0] = Counter_++;
        ToProto(req->mutable_read_session_id(), readSessionId);

        return req->Invoke();
    }

    std::vector<TBlock> FillWithRandomBlocks(
        TSessionId sessionId,
        int blockCount,
        int blockSize,
        bool useProbePutBlocks = false,
        bool preallocateDiskSpace = false,
        bool useDirectIo = false)
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
    NCellMasterClient::ICellDirectoryPtr CellDirectoryMock_;
    TIntrusivePtr<TMasterConnectorMock> MasterConnectorMock_;
    TIntrusivePtr<TTestConnection> TestConnection_;
};

////////////////////////////////////////////////////////////////////////////////

struct TGetBlockSetTestCase
{
    int BlockCount = 100;
    int BlockSize = 2_KB;
    int ParallelGetBlockSetCount = 10;
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
    const auto testCases = GeneratePairwiseCases(9);
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

////////////////////////////////////////////////////////////////////////////////

std::vector<TWriteTestCase> GenerateWriteTestParams()
{
    const std::vector<std::vector<bool>> testCases = GeneratePairwiseCases(3);
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
                // TODO(depression): Enable after Direct IO issues get fixed
                .UseDirectIOForWrites = NIO::EDirectIOPolicy::Never,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .UseProbePutBlocks = GetParam().UseProbePutBlocks,
                .PreallocateDiskSpace = GetParam().PreallocateDiskSpace,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TFairShareHierarchicalTestCase
{
    std::vector<EWorkloadCategory> WorkloadCategories;
    TEnumIndexedArray<EWorkloadCategory, std::optional<double>> FairShareWorkloadCategoryWeights;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareHierarchicalTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TFairShareHierarchicalTestCase>
{
public:
    TFairShareHierarchicalTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .IOEngineType = NIO::EIOEngineType::FairShareHierarchical,
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .FairShareWorkloadCategoryWeights = GetParam().FairShareWorkloadCategoryWeights,
                .DelayBeforePerformPutBlocks = TDuration::Seconds(0),
            })
    { }

    TFuture<void> GenerateWriteWorkload(int blockCount, int blockSize, TWorkloadDescriptor workloadDescriptor)
    {
        return BIND([=, this] {
                TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
                auto blocks = CreateBlocks(blockCount, blockSize, Generator_);
                auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);
                WaitFor(StartChunk(sessionId, false, false, false, workloadDescriptor))
                    .ThrowOnError();
                WaitFor(PutBlocks(sessionId, blocks, 0, cummulativeBlockSize).AsVoid())
                    .ThrowOnError();
                WaitFor(FlushBlocks(sessionId, blockCount - 1))
                    .ThrowOnError();
                WaitFor(FinishChunk(sessionId, blockCount))
                    .ThrowOnError();
            })
            .AsyncVia(ThreadPool_->GetInvoker())
            .Run();
    }

    // Continuously writes chunks until ShouldStop_ is set. Used to guarantee all categories
    // remain active at the moment of measurement, regardless of relative bandwidth.
    TFuture<void> GenerateContinuousWriteWorkload(
        int blockCount,
        int blockSize,
        TWorkloadDescriptor workloadDescriptor,
        int sessionCount)
    {
        auto runSession = BIND([=, this] {
            TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
            WaitFor(StartChunk(sessionId, false, false, false, workloadDescriptor))
                .ThrowOnError();
            auto blocks = CreateBlocks(blockCount, blockSize, Generator_);
            int blockIndex = 0;
            auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);
            auto allCummulativeBlockSize = cummulativeBlockSize;
            while (!ShouldStop_.load()) {
                WaitFor(PutBlocks(sessionId, blocks, blockIndex, allCummulativeBlockSize).AsVoid())
                    .ThrowOnError();
                WaitFor(FlushBlocks(sessionId, blockIndex + blockCount - 1))
                    .ThrowOnError();
                blockIndex += blockCount;
                allCummulativeBlockSize += cummulativeBlockSize;
            }
            WaitFor(FinishChunk(sessionId, blockIndex))
                .ThrowOnError();
        });

        std::vector<TFuture<void>> sessions;
        sessions.reserve(sessionCount);
        for (int i = 0; i < sessionCount; ++i) {
            sessions.push_back(runSession.AsyncVia(ThreadPool_->GetInvoker()).Run());
        }
        return AllSucceeded(std::move(sessions));
    }

    TError CheckVerifyBucketTree(TEnumIndexedArray<EWorkloadCategory, std::optional<double>> workloadCategoryToWeight)
    {
        double sumWeight = 0;
        for (const auto& weight : workloadCategoryToWeight) {
            if (weight) {
                sumWeight += *weight;
            }
        }

        // Nothing to verify (e.g. the empty test case): pass trivially.
        if (sumWeight == 0) {
            return TError();
        }

        auto rootBucket = GetFairShareHierarchicalScheduler()->GetBucket({});
        if (!rootBucket) {
            return TError("Root bucket is missing");
        }

        auto rootRequestWindowSize = rootBucket->RequestWindowSize.load();
        auto rootSlotWindowSize = rootBucket->SlotWindowSize.load();
        if (rootRequestWindowSize <= 0 || rootSlotWindowSize <= 0) {
            return TError("Root windows are not filled yet")
                << TErrorAttribute("request_window_size", rootRequestWindowSize)
                << TErrorAttribute("slot_window_size", rootSlotWindowSize);
        }

        constexpr double WeightTolerance = 0.05;
        constexpr double ShareTolerance = 0.15;

        for (auto category : TEnumTraits<EWorkloadCategory>::GetDomainValues()) {
            if (!workloadCategoryToWeight[category]) {
                continue;
            }

            auto weight = *workloadCategoryToWeight[category];

            auto currentBucket = GetBucket(category);
            if (!currentBucket) {
                return TError("Bucket for category %Qlv is missing", category);
            }

            auto requestWindowLogCount = currentBucket->RequestWindowLogCount.load();
            auto summaryRequestWeight = currentBucket->SummaryRequestWeight.load();
            auto requestWindowSize = currentBucket->RequestWindowSize.load();
            auto slotWindowSize = currentBucket->SlotWindowSize.load();

            if (requestWindowLogCount <= 0) {
                return TError("Request window for category %Qlv is empty", category);
            }

            auto avgWeight = summaryRequestWeight / requestWindowLogCount;
            if (std::abs(avgWeight - weight) > WeightTolerance) {
                return TError("Average request weight mismatch for category %Qlv: expected %v, got %v",
                    category,
                    weight,
                    avgWeight);
            }

            auto expectedShare = weight / sumWeight;

            auto requestShare = 1.0 * requestWindowSize / rootRequestWindowSize;
            if (std::abs(requestShare - expectedShare) > ShareTolerance) {
                return TError("Request window share mismatch for category %Qlv: expected %v, got %v",
                    category,
                    expectedShare,
                    requestShare);
            }

            auto slotShare = 1.0 * slotWindowSize / rootSlotWindowSize;
            if (std::abs(slotShare - expectedShare) > ShareTolerance) {
                return TError("Slot window share mismatch for category %Qlv: expected %v, got %v",
                    category,
                    expectedShare,
                    slotShare);
            }
        }

        return TError();
    }

    void CheckRequestWindowSize(TWorkloadDescriptor workloadDescriptor = {})
    {
        auto bucket = GetBucket(workloadDescriptor);
        YT_VERIFY(bucket);
        EXPECT_NE(0, bucket->RequestWindowSize);
    }

    const TFairShareHierarchicalSchedulerPtr<std::string>& GetFairShareHierarchicalScheduler()
    {
        return GetDataNodeBootstrap()->GetFairShareHierarchicalScheduler();
    }

    void SetShouldStop(bool flag)
    {
        ShouldStop_ = flag;
    }

    TFairShareHierarchicalScheduler<std::string>::TFairShareHierarchicalSlotQueueBucketNodePtr GetBucket(TWorkloadDescriptor workloadDescriptor)
    {
        return GetFairShareHierarchicalScheduler()->GetBucket({ToString(workloadDescriptor.Category)});
    }

private:
    TRandomGenerator Generator_{RandomNumber<ui64>()};
    IThreadPoolPtr ThreadPool_ = CreateThreadPool(16, "WriteGenerator");
    std::atomic<bool> ShouldStop_{false};
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TFairShareHierarchicalTest, DISABLED_StressTest)
{
    int blockCount = 80;
    int blkSize = 1_MB;
    int writesPerCategory = 5;
    auto workloadCategories = GetParam().WorkloadCategories;
    auto workloadCategoriesWeights = GetParam().FairShareWorkloadCategoryWeights;

    {
        auto schedulerConfig = GetDataNodeBootstrap()->GetDynamicConfigManager()->GetConfig()->FairShareHierarchicalScheduler;
        schedulerConfig->WindowSize = TDuration::Seconds(10);
        GetFairShareHierarchicalScheduler()->Reconfigure(schedulerConfig);
    }

    SetShouldStop(false);
    std::vector<TFuture<void>> workloadWrites;
    workloadWrites.reserve(workloadCategories.size());

    for (const auto& workloadCategory : workloadCategories) {
        TWorkloadDescriptor workload(workloadCategory);
        workloadWrites.emplace_back(GenerateContinuousWriteWorkload(blockCount, blkSize, workload, writesPerCategory));
    }

    constexpr int MinWarmupLogCount = 80;
    auto warmupDeadline = TInstant::Now() + TDuration::Seconds(30);
    while (TInstant::Now() < warmupDeadline) {
        bool allWarm = std::all_of(
            workloadCategories.begin(),
            workloadCategories.end(),
            [&, this](EWorkloadCategory category) {
                TWorkloadDescriptor workload(category);
                auto bucket = GetBucket(workload);
                return bucket && bucket->RequestWindowLogCount.load() >= MinWarmupLogCount;
            });
        if (allWarm) {
            break;
        }
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    }

    TStringStream output;
    NYson::TYsonWriter writer(&output, NYson::EYsonFormat::Pretty, NYson::EYsonType::MapFragment);
    GetFairShareHierarchicalScheduler()->BuildOrchid(&writer);
    writer.Flush();
    NLogging::TLogger Logger("TFairShareHierarchicalTest");
    YT_LOG_DEBUG(
        "Orchid: %v",
        NYson::TYsonString(output.Str(), NYson::EYsonType::MapFragment));

    for (const auto& workloadCategory : workloadCategories) {
        TWorkloadDescriptor workload(workloadCategory);
        CheckRequestWindowSize(workload);
    }

    constexpr int RequiredStableHits = 3;
    auto convergeDeadline = TInstant::Now() + TDuration::Seconds(30);
    int stableHits = 0;
    TError lastMismatch;
    while (TInstant::Now() < convergeDeadline && stableHits < RequiredStableHits) {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        lastMismatch = CheckVerifyBucketTree(workloadCategoriesWeights);
        stableHits = lastMismatch.IsOK() ? stableHits + 1 : 0;
    }
    EXPECT_GE(stableHits, RequiredStableHits)
        << "Bucket tree did not converge to configured weights: " << ToString(lastMismatch);

    SetShouldStop(true);
    WaitFor(AllSucceeded(workloadWrites))
        .ThrowOnError();
}

TEST_P(TFairShareHierarchicalTest, WriteReadTest)
{
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    TRandomGenerator generator(42);
    int blockCount = 50;
    int blkSize = 1_MB;
    auto blocks = FillWithRandomBlocks(sessionId, blockCount, blkSize);

    CheckRequestWindowSize();

    int getBlockSetCount = 10;
    std::vector<TFuture<void>> getBlockSetFutures;
    getBlockSetFutures.reserve(getBlockSetCount);
    for (int i = 0; i < getBlockSetCount; ++i) {
        auto blockIndices = GenerateRandomBlockIdsWithOrder(0, blocks.size() - 1, 2, generator);
        std::vector<TBlock> fetchedBlocks(blockIndices.size());
        for (int i = 0; i < std::ssize(blockIndices); ++i) {
            fetchedBlocks[i] = blocks[blockIndices[i]];
        }
        auto future = GetBlockSet(sessionId.ChunkId, blockIndices, true, false, true)
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
    EXPECT_TRUE(allsucceededGetBlockSetFutures.BlockingWait(TDuration::Seconds(60)));
    auto getBlockSetFuturesResult = allsucceededGetBlockSetFutures.TryGet();
    EXPECT_TRUE(getBlockSetFuturesResult.has_value());
    EXPECT_TRUE(getBlockSetFuturesResult.has_value() && getBlockSetFuturesResult->IsOK());

    CheckRequestWindowSize();
}

INSTANTIATE_TEST_SUITE_P(
    TFairShareHierarchicalTest,
    TFairShareHierarchicalTest,
    ::testing::Values(
        TFairShareHierarchicalTestCase{ },
        TFairShareHierarchicalTestCase{
            .WorkloadCategories = {
                EWorkloadCategory::UserBatch,
                EWorkloadCategory::UserInteractive},
            .FairShareWorkloadCategoryWeights = {
                {EWorkloadCategory::UserBatch, 1},
                {EWorkloadCategory::UserInteractive, 2},
            }
        }
    )
);

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

////////////////////////////////////////////////////////////////////////////////

struct TWriteThrottlingWritableCheckTestCase
{
    std::vector<double> IOWeights = {1., 1.};
    std::vector<int> SessionCountLimits = {128, 128};
    bool AlwaysThrottleLocation = false;
    bool EnableWriteThrottlingWritableCheck = false;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteThrottlingWritableCheckTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<TWriteThrottlingWritableCheckTestCase>
{
public:
    TWriteThrottlingWritableCheckTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .ReadThreadCount = 4,
                .WriteThreadCount = 4,
                .ChooseLocationBasedOnIOWeight = true,
                .IOWeights = GetParam().IOWeights,
                .SessionCountLimits = GetParam().SessionCountLimits,
                .AlwaysThrottleLocation = GetParam().AlwaysThrottleLocation,
                .EnableWriteThrottlingWritableCheck = GetParam().EnableWriteThrottlingWritableCheck,
            })
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TWriteThrottlingWritableCheckTest, IOWeightZeroWhenThrottlingCheckEnabled)
{
    const auto& locations = GetDataNodeBootstrap()->GetChunkStore()->Locations();
    auto alwaysThrottle = GetParam().AlwaysThrottleLocation;
    auto enableCheck = GetParam().EnableWriteThrottlingWritableCheck;

    for (const auto& location : locations) {
        double ioWeight = location->GetIOWeight();
        if (alwaysThrottle && enableCheck) {
            EXPECT_EQ(ioWeight, 0.0);
        } else {
            EXPECT_GT(ioWeight, 0.0);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    TWriteThrottlingWritableCheckTest,
    TWriteThrottlingWritableCheckTest,
    ::testing::Values(
        TWriteThrottlingWritableCheckTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .AlwaysThrottleLocation = true,
            .EnableWriteThrottlingWritableCheck = true,
        },
        TWriteThrottlingWritableCheckTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .AlwaysThrottleLocation = true,
            .EnableWriteThrottlingWritableCheck = false,
        },
        TWriteThrottlingWritableCheckTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .AlwaysThrottleLocation = false,
            .EnableWriteThrottlingWritableCheck = true,
        },
        TWriteThrottlingWritableCheckTestCase{
            .IOWeights = {1, 1, 1, 1, 1},
            .SessionCountLimits = {128, 128, 128, 128, 128},
            .AlwaysThrottleLocation = false,
            .EnableWriteThrottlingWritableCheck = false,
        }
    )
);

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDataNodeTest, StartChunkReflectsNetInThrottlerQueueSize)
{
    auto bootstrap = GetDataNodeBootstrap();
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->EnableInThrottlerQueueWritableCheck = true;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->NetInThrottlingLimit = 0;

    TWorkloadDescriptor workloadDescriptor{EWorkloadCategory::SystemReplication};
    auto inThrottler = bootstrap->GetInThrottler(workloadDescriptor);
    auto throttleFuture = inThrottler->Throttle(2_GB);

    EXPECT_GT(inThrottler->GetQueueTotalAmount(), 0);

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto rspOrError = WaitFor(StartChunk(sessionId, false, false, false, workloadDescriptor));
    EXPECT_FALSE(rspOrError.IsOK());

    inThrottler->Release(2_GB);
    Y_UNUSED(throttleFuture);
}

TEST_F(TDataNodeTest, DISABLED_NetInThrottlingIsReportedAsWriteThrottling)
{
    auto bootstrap = GetDataNodeBootstrap();
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->NetInThrottlingLimit = 0;

    TWorkloadDescriptor workloadDescriptor{EWorkloadCategory::SystemReplication};
    auto inThrottler = bootstrap->GetInThrottler(workloadDescriptor);
    auto throttleFuture = inThrottler->Throttle(2_GB);

    EXPECT_GT(inThrottler->GetQueueTotalAmount(), 0);

    const std::string networkName = "test_network";
    auto result = bootstrap->CheckNetInThrottling(networkName, workloadDescriptor);
    EXPECT_TRUE(result.Enabled);

    NNodeTrackerClient::NProto::TClusterNodeStatistics statistics;
    bootstrap->GetNetworkStatistics().UpdateStatistics(&statistics);

    const NNodeTrackerClient::NProto::TNetworkStatistics* networkStatistics = nullptr;
    for (const auto& network : statistics.network()) {
        if (network.network() == networkName) {
            networkStatistics = &network;
            break;
        }
    }

    ASSERT_NE(networkStatistics, nullptr);
    EXPECT_FALSE(networkStatistics->throttling_reads());
    EXPECT_TRUE(networkStatistics->throttling_writes());

    inThrottler->Release(2_GB);
    WaitFor(throttleFuture)
        .ThrowOnError();
}

TEST_F(TDataNodeTest, StartChunkIgnoresNetInThrottlerQueueSizeWhenFlagDisabled)
{
    auto bootstrap = GetDataNodeBootstrap();
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->EnableInThrottlerQueueWritableCheck = false;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->NetInThrottlingLimit = 0;

    TWorkloadDescriptor workloadDescriptor{EWorkloadCategory::SystemReplication};
    auto inThrottler = bootstrap->GetInThrottler(workloadDescriptor);
    auto throttleFuture = inThrottler->Throttle(2_GB);

    EXPECT_GT(inThrottler->GetQueueTotalAmount(), 0);

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto rspOrError = WaitFor(StartChunk(sessionId, false, false, false, workloadDescriptor));
    EXPECT_TRUE(rspOrError.IsOK());

    inThrottler->Release(2_GB);
    Y_UNUSED(throttleFuture);
}

TEST_F(TDataNodeTest, ProbePutBlocksReflectsNetInThrottlerQueueSize)
{
    auto bootstrap = GetDataNodeBootstrap();
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->UseProbePutBlocks = true;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->EnableInThrottlerQueueWritableCheck = true;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->NetInThrottlingLimit = 0;

    TWorkloadDescriptor workloadDescriptor{EWorkloadCategory::SystemReplication};
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    WaitFor(StartChunk(sessionId, true, false, false, workloadDescriptor))
        .ThrowOnError();

    auto initialResponse = WaitFor(ProbePutBlocks(sessionId, 1_MB))
        .ValueOrThrow();
    EXPECT_EQ(initialResponse->probe_put_blocks_state().approved_cumulative_block_size(), 1_MB);

    auto inThrottler = bootstrap->GetInThrottler(workloadDescriptor);
    auto throttleFuture = inThrottler->Throttle(2_GB);

    EXPECT_GT(inThrottler->GetQueueTotalAmount(), 0);
    auto throttledResponse = WaitFor(ProbePutBlocks(sessionId, 2_MB))
        .ValueOrThrow();
    EXPECT_EQ(throttledResponse->probe_put_blocks_state().requested_cumulative_block_size(), 2_MB);
    EXPECT_EQ(throttledResponse->probe_put_blocks_state().approved_cumulative_block_size(), 0);

    auto throttledPingResponse = WaitFor(PingSession(sessionId))
        .ValueOrThrow();
    EXPECT_EQ(throttledPingResponse->probe_put_blocks_state().requested_cumulative_block_size(), 2_MB);
    EXPECT_EQ(throttledPingResponse->probe_put_blocks_state().approved_cumulative_block_size(), 0);

    auto session = bootstrap->GetSessionManager()->GetSessionOrThrow(sessionId.ChunkId);
    EXPECT_EQ(session->GetApprovedCumulativeBlockSize(), 2_MB);

    inThrottler->Release(2_GB);
    WaitFor(throttleFuture)
        .ThrowOnError();

    auto response = WaitFor(ProbePutBlocks(sessionId, 2_MB))
        .ValueOrThrow();
    EXPECT_EQ(response->probe_put_blocks_state().approved_cumulative_block_size(), 2_MB);
}

TEST_F(TDataNodeTest, ProbePutBlocksIgnoresNetInThrottlerQueueSizeWhenFlagDisabled)
{
    auto bootstrap = GetDataNodeBootstrap();
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->UseProbePutBlocks = true;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->EnableInThrottlerQueueWritableCheck = false;
    bootstrap->GetDynamicConfigManager()->GetConfig()->DataNode->NetInThrottlingLimit = 0;

    TWorkloadDescriptor workloadDescriptor{EWorkloadCategory::SystemReplication};
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    WaitFor(StartChunk(sessionId, true, false, false, workloadDescriptor))
        .ThrowOnError();

    auto inThrottler = bootstrap->GetInThrottler(workloadDescriptor);
    auto throttleFuture = inThrottler->Throttle(2_GB);

    EXPECT_GT(inThrottler->GetQueueTotalAmount(), 0);
    auto response = WaitFor(ProbePutBlocks(sessionId, 1_MB))
        .ValueOrThrow();
    EXPECT_EQ(response->probe_put_blocks_state().approved_cumulative_block_size(), 1_MB);

    inThrottler->Release(2_GB);
    Y_UNUSED(throttleFuture);
}

////////////////////////////////////////////////////////////////////////////////

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
        future.BlockingWait();
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

TEST_P(TGetBlockSetTest, DISABLED_GetBlockSetTest)
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
        for (int j = 0; j < std::ssize(blockIndices); ++j) {
            fetchedBlocks[j] = blocks[blockIndices[j]];
        }
        auto future = BIND([this, &testCase, sessionId, blockIndices, fetchedBlocks = std::move(fetchedBlocks)] {
                while (true) {
                    auto rsp = WaitFor(GetBlockSet(
                        sessionId.ChunkId,
                        blockIndices,
                        testCase.PopulateCache,
                        testCase.FetchFromCache,
                        testCase.FetchFromDisk))
                        .ValueOrThrow();
                    if (rsp->disk_throttling() || rsp->net_throttling()) {
                        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
                        continue;
                    }
                    auto gotBlocks = GetRpcAttachedBlocks(rsp);
                    EXPECT_EQ(gotBlocks.size(), fetchedBlocks.size());
                    EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(fetchedBlocks));
                    return;
                }
            })
            .AsyncVia(GetActionQueue()->GetInvoker())
            .Run();
        getBlockSetFutures.push_back(std::move(future));
    }

    auto allsucceededGetBlockSetFutures = AllSucceeded(getBlockSetFutures);
    EXPECT_TRUE(allsucceededGetBlockSetFutures.BlockingWait(TDuration::Seconds(120)));
    auto getBlockSetFuturesResult = allsucceededGetBlockSetFutures.TryGet();
    EXPECT_TRUE(getBlockSetFuturesResult.has_value());
    EXPECT_TRUE(getBlockSetFuturesResult.has_value() && getBlockSetFuturesResult->IsOK());

    const auto& hugePageManager = GetDataNodeBootstrap()->GetHugePageManager();
    bool expectHugePagesUsed =
        testCase.EnableHugePageManager &&
        testCase.UseDirectIOForReads;

    if (expectHugePagesUsed) {
        YT_VERIFY(hugePageManager->GetHugePageSize() > 0);
        EXPECT_GT(hugePageManager->GetUsedHugePageCount(), 0);
    } else {
        EXPECT_EQ(hugePageManager->GetUsedHugePageCount(), 0);
    }
}

INSTANTIATE_TEST_SUITE_P(
    TGetBlockSetTest,
    TGetBlockSetTest,
    ::testing::ValuesIn(GenerateGetBlockSetParams())
);

class TOversizedBlockCacheTest
    : public TDataNodeTest
{
public:
    TOversizedBlockCacheTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .BlockCacheCapacity = 1_KB,
                .RejectOversizedBlockCacheItems = true,
            })
    { }
};

TEST_F(TOversizedBlockCacheTest, DoesNotChargeBlockCacheMemory)
{
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto blocks = FillWithRandomBlocks(sessionId, /*blockCount*/ 1, /*blockSize*/ 4_KB);

    auto rspOrError = WaitFor(GetBlockSet(
        sessionId.ChunkId,
        /*blockIndices*/ std::vector<int>{0},
        /*populateCache*/ true,
        /*fetchFromCache*/ true,
        /*fetchFromDisk*/ true));
    ASSERT_TRUE(rspOrError.IsOK());

    auto gotBlocks = GetRpcAttachedBlocks(rspOrError.Value());
    ASSERT_EQ(gotBlocks.size(), 1u);
    EXPECT_EQ(BlocksToChecksums(gotBlocks), BlocksToChecksums(blocks));

    auto cacheCookie = GetDataNodeBootstrap()->GetBlockCache()->GetBlockCookie(
        TBlockId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), 0),
        EBlockType::CompressedData);
    ASSERT_TRUE(cacheCookie->IsActive());
    cacheCookie->SetBlock(blocks[0]);

    EXPECT_EQ(GetDataNodeBootstrap()->GetNodeMemoryUsageTracker()->GetUsed(EMemoryCategory::BlockCache), 0);
}

class TReadBlocksDeadlineTest
    : public TDataNodeTest
    , public ::testing::WithParamInterface<std::tuple</*failSession*/ bool, /*returnBlocks*/ bool>>
{
public:
    TReadBlocksDeadlineTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .CoalescedReadMaxGapSize = 0,
            })
    { }
};

TEST_P(TReadBlocksDeadlineTest, GetBlockSet)
{
    const auto [failSessionAtDeadline, returnBlocksIfSessionFails] = GetParam();

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto blocks = FillWithRandomBlocks(sessionId, /*blockCount*/ 16, /*blockSize*/ 4_KB);

    auto dyn = GetDataNodeBootstrap()->GetDynamicConfigManager()->GetConfig()->DataNode;
    dyn->FailSessionAtReadBlocksDeadline = failSessionAtDeadline;
    dyn->ReturnBlocksIfSessionFails = returnBlocksIfSessionFails;

    // Make the server-side read routine hit its internal deadline quickly,
    // while the RPC itself has enough time to complete.
    dyn->TestingOptions->BlockReadTimeoutFraction = 0.75;
    dyn->TestingOptions->DelayBeforeBlobChunkRead = TDuration::Seconds(2);

    auto rspOrError = WaitFor(GetBlockSet(
        sessionId.ChunkId,
        /*blockIndices*/ std::vector<int>{0, 2, 4, 6},
        /*populateCache*/ false,
        /*fetchFromCache*/ false,
        /*fetchFromDisk*/ true,
        /*workloadDescriptor*/ {},
        /*requestTimeout*/ TDuration::Seconds(4)));

    if (!failSessionAtDeadline) {
        EXPECT_TRUE(rspOrError.IsOK());
        auto gotBlocks = GetRpcAttachedBlocks(rspOrError.Value());
        for (const auto& block : gotBlocks) {
            EXPECT_TRUE(block.Size() == 0);
        }
        return;
    }

    if (!returnBlocksIfSessionFails) {
        EXPECT_FALSE(rspOrError.IsOK());
        return;
    }

    EXPECT_TRUE(rspOrError.IsOK());
    auto gotBlocks = GetRpcAttachedBlocks(rspOrError.Value());
    int notEmptyBlocks = 0;
    for (const auto& block : gotBlocks) {
        if (block) {
            ++notEmptyBlocks;
        }
    }
    EXPECT_GT(notEmptyBlocks, 0);
    EXPECT_LT(notEmptyBlocks, 4);
}

INSTANTIATE_TEST_SUITE_P(
    TReadBlocksDeadlineTest,
    TReadBlocksDeadlineTest,
    ::testing::Values(
        std::tuple(false, false),
        std::tuple(false, true),
        std::tuple(true, false),
        std::tuple(true, true))
);

class TReadBlocksDeadlineCancellationTest
    : public TDataNodeTest
{
public:
    static constexpr i64 BlockCacheCapacity = 16_KB;

    TReadBlocksDeadlineCancellationTest()
        : TDataNodeTest(
            TDataNodeTest::TDataNodeTestParams {
                .CoalescedReadMaxGapSize = 0,
                .BlockCacheCapacity = BlockCacheCapacity,
            })
    { }
};

TEST_F(TReadBlocksDeadlineCancellationTest, SessionDeadlineCancelledOnFastComplete)
{
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    auto blocks = FillWithRandomBlocks(sessionId, /*blockCount*/ 64, /*blockSize*/ 4_KB);

    auto dyn = GetDataNodeBootstrap()->GetDynamicConfigManager()->GetConfig()->DataNode;
    dyn->FailSessionAtReadBlocksDeadline = true;
    dyn->ReturnBlocksIfSessionFails = false;
    dyn->TestingOptions->BlockReadTimeoutFraction = 0.75;
    dyn->TestingOptions->DelayBeforeBlobChunkRead.reset();

    for (int blockIndex = 0; blockIndex < std::ssize(blocks); ++blockIndex) {
        auto rspOrError = WaitFor(GetBlockSet(
            sessionId.ChunkId,
            /*blockIndices*/ std::vector{blockIndex},
            /*populateCache*/ true,
            /*fetchFromCache*/ false,
            /*fetchFromDisk*/ true,
            /*workloadDescriptor*/ {},
            /*requestTimeout*/ TDuration::Seconds(30)));

        ASSERT_TRUE(rspOrError.IsOK());
        auto gotBlocks = GetRpcAttachedBlocks(rspOrError.Value());
        ASSERT_EQ(gotBlocks.size(), 1u);
        EXPECT_EQ(gotBlocks[0].GetOrComputeChecksum(), blocks[blockIndex].GetOrComputeChecksum());
    }

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

    auto blockCacheMemoryUsed = GetDataNodeBootstrap()->GetNodeMemoryUsageTracker()->GetUsed(
        EMemoryCategory::BlockCache);
    EXPECT_LE(blockCacheMemoryUsed, BlockCacheCapacity);
}

TEST_F(TReadBlocksDeadlineCancellationTest, SessionDeadlineStillFailsOnSlowRead)
{
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    FillWithRandomBlocks(sessionId, /*blockCount*/ 16, /*blockSize*/ 4_KB);

    auto dyn = GetDataNodeBootstrap()->GetDynamicConfigManager()->GetConfig()->DataNode;
    dyn->FailSessionAtReadBlocksDeadline = true;
    dyn->ReturnBlocksIfSessionFails = false;
    dyn->TestingOptions->BlockReadTimeoutFraction = 0.75;
    dyn->TestingOptions->DelayBeforeBlobChunkRead = TDuration::Seconds(2);

    auto rspOrError = WaitFor(GetBlockSet(
        sessionId.ChunkId,
        /*blockIndices*/ std::vector<int>{0, 2, 4, 6},
        /*populateCache*/ true,
        /*fetchFromCache*/ false,
        /*fetchFromDisk*/ true,
        /*workloadDescriptor*/ {},
        /*requestTimeout*/ TDuration::Seconds(4)));

    EXPECT_FALSE(rspOrError.IsOK());
}

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
    WaitFor(StartChunk(sessionId, false, false, false))
        .ThrowOnError();

    TRandomGenerator generator{RandomNumber<ui64>()};

    auto blocks = CreateBlocks(100, 1_KB, generator);
    auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);

    auto putBlocksEnd = PutBlocks(sessionId, {blocks[0]}, 10, cummulativeBlockSize);
    auto putBlocksBegin = PutBlocks(sessionId, {blocks[0]}, 0, cummulativeBlockSize);

    WaitFor(CancelChunk(sessionId))
        .ThrowOnError();

    putBlocksEnd.Cancel(TError("Timeout"));

    EXPECT_THROW_WITH_SUBSTRING(WaitFor(putBlocksBegin).ThrowOnError(), "Session is not active");
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

TEST_P(TWriteTest, DuplicateWrite)
{
    auto testCase = GetParam();
    TRandomGenerator generator{RandomNumber<ui64>()};

    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    int blockCount = testCase.BlockCount;
    int blockSize = testCase.BlockSize;

    auto blocks = CreateBlocks(blockCount, blockSize, generator);
    WaitFor(StartChunk(sessionId, /*useProbePutBlocks*/ false, testCase.PreallocateDiskSpace, testCase.UseDirectIo, /*workloadDescriptor*/ {}, /*disableSendBlocks*/ true))
        .ThrowOnError();

    auto putBlocks = [&] {
        auto cummulativeBlockSize = CalculateCummulativeBlockSize(blocks);

        std::vector<TFuture<void>> putBlocks(blocks.size() * 2);
        std::vector<int> indices(blocks.size() * 2);
        std::iota(std::begin(indices), std::end(indices), 0);
        std::random_shuffle(std::begin(indices), std::end(indices));
        for (auto i : indices) {
            auto blockIndex = i % blocks.size();
            putBlocks[i] = PutBlocks(sessionId, {blocks[blockIndex]}, blockIndex, cummulativeBlockSize).AsVoid();
        }
        WaitFor(AllSucceeded(putBlocks))
            .ThrowOnError();
    };

    putBlocks();

    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));

    putBlocks();

    WaitFor(FlushBlocks(sessionId, blockCount - 1))
        .ThrowOnError();

    WaitFor(FinishChunk(sessionId, blockCount))
        .ThrowOnError();
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
    auto rspOrError = WaitFor(GetBlockSet(sessionId.ChunkId, blockIndices, false, false, true));
    YT_VERIFY(rspOrError.IsOK());
    auto rsp = rspOrError.Value();
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

class TJournalSessionTest
    : public TDataNodeTest
{ };

TEST_F(TJournalSessionTest, PutBlocksDuplicatesAndGaps)
{
    TSessionId sessionId(MakeRandomId(EObjectType::JournalChunk, TCellTag(0xf003)), GenericMediumIndex);
    WaitFor(StartChunk(sessionId, /*useProbePutBlocks*/ false, /*preallocateDiskSpace*/ false, /*useDirectIo*/ false))
        .ThrowOnError();

    TRandomGenerator generator(42);
    auto records = CreateBlocks(/*count*/ 5, /*blockSize*/ 10, generator);
    auto cumulativeBlockSize = CalculateCummulativeBlockSize(records);

    WaitFor(PutBlocks(sessionId, records, /*firstBlockIndex*/ 0, cumulativeBlockSize).AsVoid())
        .ThrowOnError();
    WaitFor(FlushBlocks(sessionId, /*blockIndex*/ 4))
        .ThrowOnError();

    // A request starting past the changelog end is rejected with a dedicated
    // error code.
    auto gapResult = WaitFor(PutBlocks(sessionId, records, /*firstBlockIndex*/ 7, cumulativeBlockSize).AsVoid());
    EXPECT_FALSE(gapResult.IsOK());
    EXPECT_TRUE(gapResult.FindMatching(NChunkClient::EErrorCode::MissingJournalChunkRecord))
        << ToString(gapResult);

    // A duplicate ending exactly at the changelog end is skipped.
    auto exactDuplicateResult = WaitFor(PutBlocks(sessionId, records, /*firstBlockIndex*/ 0, cumulativeBlockSize).AsVoid());
    EXPECT_TRUE(exactDuplicateResult.IsOK())
        << ToString(exactDuplicateResult);

    // A stale duplicate ending strictly before the changelog end must be
    // skipped as well.
    std::vector<TBlock> stalePrefix(records.begin(), records.begin() + 3);
    auto staleDuplicateResult = WaitFor(PutBlocks(sessionId, stalePrefix, /*firstBlockIndex*/ 0, cumulativeBlockSize).AsVoid());
    EXPECT_TRUE(staleDuplicateResult.IsOK())
        << ToString(staleDuplicateResult);

    // The session remains usable afterwards.
    auto tailRecords = CreateBlocks(/*count*/ 3, /*blockSize*/ 10, generator);
    WaitFor(PutBlocks(sessionId, tailRecords, /*firstBlockIndex*/ 5, cumulativeBlockSize).AsVoid())
        .ThrowOnError();
    WaitFor(FlushBlocks(sessionId, /*blockIndex*/ 7))
        .ThrowOnError();

    WaitFor(CancelChunk(sessionId))
        .ThrowOnError();
}

TEST_F(TDataNodeTest, DuplicatePutBlocksDoNotMakeUsedSpaceNegative)
{
    const auto& locations = GetDataNodeBootstrap()->GetChunkStore()->Locations();
    ASSERT_EQ(locations.size(), 1u);
    const auto& location = locations.front();
    const auto initialUsedSpace = location->GetUsedSpace();
    ASSERT_EQ(initialUsedSpace, 0);
    TSessionId sessionId(MakeRandomId(EObjectType::Chunk, TCellTag(0xf003)), GenericMediumIndex);
    WaitFor(StartChunk(sessionId, /*useProbePutBlocks*/ false, /*preallocateDiskSpace*/ false, /*useDirectIo*/ false))
        .ThrowOnError();
    TRandomGenerator generator{RandomNumber<ui64>()};
    auto blocks = CreateBlocks(/*blockCount*/ 1, /*blockSize*/ 1_KB, generator);
    auto cumulativeBlockSize = CalculateCummulativeBlockSize(blocks);
    WaitFor(PutBlocks(sessionId, blocks, /*firstBlockIndex*/ 0, cumulativeBlockSize))
        .ThrowOnError();
    WaitFor(PutBlocks(sessionId, blocks, /*firstBlockIndex*/ 0, cumulativeBlockSize))
        .ThrowOnError();
    WaitFor(CancelChunk(sessionId, /*waitForCancelation*/ true))
        .ThrowOnError();
    EXPECT_GE(location->GetUsedSpace(), 0);
    EXPECT_EQ(location->GetUsedSpace(), initialUsedSpace);
}

////////////////////////////////////////////////////////////////////////////////

}
