#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_location.h>
#include <yt/yt/server/master/chunk_server/chunk_requisition.h>
#include <yt/yt/server/master/chunk_server/chunk_statistics.h>
#include <yt/yt/server/master/chunk_server/domestic_medium.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/host.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/rack.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NChunkServer {
namespace {

using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NNodeTrackerServer;
using namespace NSecurityServer;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateId(EObjectType type)
{
    static i64 counter = 0;
    return MakeId(type, TCellTag(0), counter++, 0);
}

class TTestingMediumMap
    : public NHydra::TReadOnlyEntityMap<TMedium>
{
public:
    void Add(TMedium* medium)
    {
        Map_.emplace(medium->GetId(), medium);
    }
};

class TTestingChunkStatisticsCalculatorCallbacks
    : public IChunkStatisticsCalculatorCallbacks
{
public:
    TTestingChunkStatisticsCalculatorCallbacks(
        TChunkRequisitionRegistry* chunkRequisitionRegistry,
        TDynamicChunkManagerConfigPtr dynamicConfig)
        : ChunkRequisitionRegistry_(chunkRequisitionRegistry)
        , DynamicConfig_(std::move(dynamicConfig))
    { }

    void AddMedium(TMedium* medium)
    {
        MediaByIndex_[medium->GetIndex()] = medium;
        Media_.Add(medium);
    }

    void SetConsistentPlacementWriteTargets(TNodeList targets)
    {
        ConsistentPlacementWriteTargets_ = std::move(targets);
    }

    void SetMaxReplicasPerDataCenter(int maxReplicasPerDataCenter)
    {
        MaxReplicasPerDataCenter_ = maxReplicasPerDataCenter;
    }

    int GetConsistentPlacementWriteTargetsCallCount() const
    {
        return ConsistentPlacementWriteTargetsCallCount_;
    }

    TMedium* FindMediumByIndex(int mediumIndex) const override
    {
        auto it = MediaByIndex_.find(mediumIndex);
        return it == MediaByIndex_.end() ? nullptr : it->second;
    }

    const NHydra::TReadOnlyEntityMap<TMedium>& GetMedia() const override
    {
        return Media_;
    }

    TChunkRequisitionRegistry* GetChunkRequisitionRegistry() const override
    {
        return ChunkRequisitionRegistry_;
    }

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig() const override
    {
        return DynamicConfig_;
    }

    NLogging::ELogLevel GetChunkLogLevel(const TChunk* /*chunk*/) const override
    {
        return NLogging::ELogLevel::Debug;
    }

    int GetMaxReplicasPerRack(int /*mediumIndex*/, const TChunk* /*chunk*/) const override
    {
        return 1;
    }

    int GetMaxReplicasPerDataCenter(
        int /*mediumIndex*/,
        const TChunk* /*chunk*/,
        const TDataCenter* /*dataCenter*/) const override
    {
        return MaxReplicasPerDataCenter_.value_or(Max<int>());
    }

    TNodeList GetConsistentPlacementWriteTargets(
        const TChunk* /*chunk*/,
        int /*mediumIndex*/) const override
    {
        ++ConsistentPlacementWriteTargetsCallCount_;
        return ConsistentPlacementWriteTargets_;
    }

private:
    TChunkRequisitionRegistry* const ChunkRequisitionRegistry_;
    const TDynamicChunkManagerConfigPtr DynamicConfig_;
    TMediumMap<TMedium*> MediaByIndex_;
    TTestingMediumMap Media_;
    TNodeList ConsistentPlacementWriteTargets_;
    std::optional<int> MaxReplicasPerDataCenter_;
    mutable int ConsistentPlacementWriteTargetsCallCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TTestingChunkStatisticsCalculatorCallbacks)

////////////////////////////////////////////////////////////////////////////////

class TChunkStatisticsCalculatorTest
    : public ::testing::Test
{
protected:
    // The default replication factor for chunks with the builtin requisition is 3.
    // Combined with the per-rack replica limit of 1 returned by the test callbacks,
    // this makes the minimal safe available replica count equal to 2.
    static constexpr int DefaultReplicationFactor = 3;

    static constexpr auto TestErasureCodec = NErasure::ECodec::IsaReedSolomon_6_3;
    static constexpr int TestErasureDataPartCount = 6;
    static constexpr int TestErasureTotalPartCount = 9;
    // For Reed-Solomon codecs any combination of up to "parity count" erasures is repairable.
    static constexpr int TestErasureGuaranteedRepairablePartCount = 3;

    struct TMediumReplication
    {
        int MediumIndex;
        int ReplicationFactor;
        bool DataPartsOnly = false;
    };

    enum class EReplicaAvailability
    {
        Available,
        Decommissioned,
        TemporarilyUnavailable,
    };

    void SetUp() override
    {
        Account_ = TPoolAllocator::New<TAccount>(GenerateId(EObjectType::Account));
        Medium_ = TPoolAllocator::New<TDomesticMedium>(GenerateId(EObjectType::DomesticMedium));

        Account_->RefObject();
        Medium_->RefObject();

        Medium_->SetName("default");
        Medium_->SetIndex(DefaultStoreMediumIndex);

        RequisitionRegistry_.EnsureBuiltinRequisitionsInitialized(TTestingTag(), Account_.get());

        Config_ = New<TChunkManagerConfig>();
        DynamicConfig_ = New<TDynamicChunkManagerConfig>();

        Callbacks_ = New<TTestingChunkStatisticsCalculatorCallbacks>(
            &RequisitionRegistry_,
            DynamicConfig_);
        Callbacks_->AddMedium(Medium_.get());

        StatisticsCalculator_ = std::make_unique<TChunkStatisticsCalculator>(
            Config_,
            Callbacks_);
    }

    std::unique_ptr<TChunk> CreateChunk(
        EChunkType chunkType = EChunkType::Table,
        NErasure::ECodec erasureCodec = NErasure::ECodec::None,
        bool sealed = false)
    {
        auto erasure = erasureCodec != NErasure::ECodec::None;
        auto objectType = chunkType == EChunkType::Journal
            ? (erasure ? EObjectType::ErasureJournalChunk : EObjectType::JournalChunk)
            : (erasure ? EObjectType::ErasureChunk : EObjectType::Chunk);
        auto chunk = TPoolAllocator::New<TChunk>(GenerateId(objectType));
        chunk->RefObject();

        TChunkMeta chunkMeta;
        chunkMeta.set_type(ToProto(chunkType));
        TMiscExt miscExt;
        SetProtoExtension<TMiscExt>(chunkMeta.mutable_extensions(), miscExt);

        TChunkInfo chunkInfo;
        chunk->Confirm(chunkInfo, chunkMeta);
        chunk->SetErasureCodec(erasureCodec);
        chunk->SetSealed(sealed);

        return chunk;
    }

    std::unique_ptr<TChunk> CreateErasureChunk(
        std::initializer_list<TMediumReplication> replication)
    {
        auto chunk = CreateChunk(EChunkType::Table, TestErasureCodec);
        SetChunkReplication(chunk.get(), replication);
        return chunk;
    }

    std::unique_ptr<TChunk> CreateErasureChunk()
    {
        return CreateErasureChunk({
            {
                .MediumIndex = DefaultStoreMediumIndex,
                .ReplicationFactor = 1,
            },
        });
    }

    TDomesticMedium* CreateMedium(int mediumIndex, bool transient = false)
    {
        auto medium = TPoolAllocator::New<TDomesticMedium>(GenerateId(EObjectType::DomesticMedium));
        medium->RefObject();
        medium->SetName(Format("medium-%v", mediumIndex));
        medium->SetIndex(mediumIndex);
        medium->SetTransient(transient);

        auto* result = medium.get();
        Callbacks_->AddMedium(result);
        AdditionalMedia_.push_back(std::move(medium));
        return result;
    }

    void SetChunkReplication(
        TChunk* chunk,
        std::initializer_list<TMediumReplication> replication)
    {
        // SetLocalRequisitionIndex expects the chunk's initial requisitions to be referenced.
        chunk->RefUsedRequisitions(&RequisitionRegistry_);

        TChunkRequisition requisition;
        for (const auto& entry : replication) {
            requisition |= TChunkRequisition(
                Account_.get(),
                entry.MediumIndex,
                TReplicationPolicy(entry.ReplicationFactor, entry.DataPartsOnly),
                /*committed*/ true);
        }

        auto requisitionIndex = RequisitionRegistry_.GetOrCreate(
            requisition,
            /*objectManager*/ nullptr);
        chunk->SetLocalRequisitionIndex(
            requisitionIndex,
            &RequisitionRegistry_,
            /*objectManager*/ nullptr);
    }

    TNode* CreateNode()
    {
        auto node = TPoolAllocator::New<TNode>(GenerateId(EObjectType::ClusterNode));
        node->RefObject();
        Nodes_.push_back(std::move(node));
        return Nodes_.back().get();
    }

    TDataCenter* CreateDataCenter()
    {
        auto dataCenter = TPoolAllocator::New<TDataCenter>(GenerateId(EObjectType::DataCenter));
        dataCenter->RefObject();
        dataCenter->SetName(Format("dc-%v", DataCenters_.size()));
        DataCenters_.push_back(std::move(dataCenter));
        return DataCenters_.back().get();
    }

    TRack* CreateRack(TDataCenter* dataCenter = nullptr)
    {
        auto rack = TPoolAllocator::New<TRack>(GenerateId(EObjectType::Rack));
        rack->RefObject();
        rack->SetName(Format("rack-%v", Racks_.size()));
        rack->SetIndex(std::ssize(Racks_) + 1);
        rack->SetDataCenter(dataCenter);
        Racks_.push_back(std::move(rack));
        return Racks_.back().get();
    }

    THost* CreateHost(TRack* rack = nullptr)
    {
        auto host = TPoolAllocator::New<THost>(GenerateId(EObjectType::Host));
        host->RefObject();
        host->SetName(Format("host-%v", Hosts_.size()));
        host->SetRack(rack);
        Hosts_.push_back(std::move(host));
        return Hosts_.back().get();
    }

    TNode* CreateNodeOnHost(THost* host)
    {
        auto* node = CreateNode();
        node->SetHost(TTestingTag(), host);
        return node;
    }

    TNode* CreateNodeInRack(TRack* rack)
    {
        return CreateNodeOnHost(CreateHost(rack));
    }

    TAugmentedStoredChunkReplicaPtr CreateReplica(
        TNode* node = nullptr,
        int mediumIndex = DefaultStoreMediumIndex,
        int replicaIndex = NChunkClient::GenericChunkReplicaIndex,
        EChunkReplicaState replicaState = EChunkReplicaState::Generic)
    {
        if (!node) {
            node = CreateNode();
        }

        auto location = TPoolAllocator::New<TChunkLocation>(GenerateId(EObjectType::ChunkLocation));
        location->RefObject();
        location->SetNode(node);
        location->Statistics().set_medium_index(mediumIndex);
        Locations_.push_back(std::move(location));
        return TAugmentedStoredChunkReplicaPtr(
            Locations_.back().get(),
            replicaIndex,
            replicaState);
    }

    TAugmentedStoredChunkReplicaPtr CreateReplica(
        EReplicaAvailability availability,
        int mediumIndex = DefaultStoreMediumIndex,
        int replicaIndex = NChunkClient::GenericChunkReplicaIndex,
        EChunkReplicaState replicaState = EChunkReplicaState::Generic)
    {
        auto* node = CreateNode();
        switch (availability) {
            case EReplicaAvailability::Available:
                break;
            case EReplicaAvailability::Decommissioned:
                YT_VERIFY(node->SetMaintenanceFlag(
                    NMaintenanceTrackerServer::EMaintenanceType::Decommission,
                    "test",
                    TInstant::Zero()));
                break;
            case EReplicaAvailability::TemporarilyUnavailable:
                YT_VERIFY(node->SetMaintenanceFlag(
                    NMaintenanceTrackerServer::EMaintenanceType::PendingRestart,
                    "test",
                    TInstant::Zero()));
                break;
        }

        return CreateReplica(node, mediumIndex, replicaIndex, replicaState);
    }

    //! Creates replicas for all erasure parts except those listed in #missingIndexes.
    TStoredChunkReplicaList CreateErasurePartReplicas(
        const std::vector<int>& missingIndexes = {},
        int mediumIndex = DefaultStoreMediumIndex)
    {
        TStoredChunkReplicaList replicas;
        for (int replicaIndex = 0; replicaIndex < TestErasureTotalPartCount; ++replicaIndex) {
            if (std::find(missingIndexes.begin(), missingIndexes.end(), replicaIndex) ==
                missingIndexes.end())
            {
                replicas.push_back(CreateReplica(nullptr, mediumIndex, replicaIndex));
            }
        }
        return replicas;
    }

    TNodeList CreateConsistentPlacementWriteTargets(int count)
    {
        TNodeList result;
        result.reserve(count);
        for (int index = 0; index < count; ++index) {
            result.push_back(CreateNode());
        }
        return result;
    }

    std::unique_ptr<TAccount> Account_;
    std::unique_ptr<TDomesticMedium> Medium_;
    std::vector<std::unique_ptr<TDomesticMedium>> AdditionalMedia_;
    std::vector<std::unique_ptr<TNode>> Nodes_;
    std::vector<std::unique_ptr<TDataCenter>> DataCenters_;
    std::vector<std::unique_ptr<TRack>> Racks_;
    std::vector<std::unique_ptr<THost>> Hosts_;
    std::vector<std::unique_ptr<TChunkLocation>> Locations_;
    TChunkRequisitionRegistry RequisitionRegistry_;
    TChunkManagerConfigPtr Config_;
    TDynamicChunkManagerConfigPtr DynamicConfig_;
    TIntrusivePtr<TTestingChunkStatisticsCalculatorCallbacks> Callbacks_;
    std::unique_ptr<TChunkStatisticsCalculator> StatisticsCalculator_;
};

////////////////////////////////////////////////////////////////////////////////
// Regular chunks: replica state grid.

TEST_F(TChunkStatisticsCalculatorTest, RegularReplicaStateGrid)
{
    struct TTestCase
    {
        const char* Name;
        int AvailableReplicaCount;
        int DecommissionedReplicaCount;
        int TemporarilyUnavailableReplicaCount;
        EChunkStatus ExpectedStatus;
        ECrossMediumChunkStatus ExpectedCrossMediumStatus;
        int ExpectedReplicationIndexCount;
        int ExpectedDecommissionedRemovalCount;
        int ExpectedBalancingRemovalIndexCount;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "Lost",
            .AvailableReplicaCount = 0,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Lost,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Lost | ECrossMediumChunkStatus::Precarious,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "Underreplicated",
            .AvailableReplicaCount = 1,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Underreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient,
            .ExpectedReplicationIndexCount = 1,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "Healthy",
            .AvailableReplicaCount = 3,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::None,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "UnexpectedOverreplication",
            .AvailableReplicaCount = 4,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 1,
        },
        {
            // Decommissioned replicas keep the chunk from being lost but do not
            // count towards the replication factor.
            .Name = "AllReplicasDecommissioned",
            .AvailableReplicaCount = 0,
            .DecommissionedReplicaCount = 3,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Underreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient,
            .ExpectedReplicationIndexCount = 1,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            // Replicas on pending-restart nodes do not save the chunk from being lost.
            .Name = "LostWithTemporarilyUnavailableReplicas",
            .AvailableReplicaCount = 0,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 2,
            .ExpectedStatus = EChunkStatus::Lost |
                EChunkStatus::Underreplicated |
                EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Lost |
                ECrossMediumChunkStatus::Precarious |
                ECrossMediumChunkStatus::Deficient,
            .ExpectedReplicationIndexCount = 1,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "SafeTemporaryUnavailability",
            .AvailableReplicaCount = 2,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "UnsafeTemporaryUnavailability",
            .AvailableReplicaCount = 1,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 2,
            .ExpectedStatus = EChunkStatus::Underreplicated | EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient,
            .ExpectedReplicationIndexCount = 1,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "DecommissionedReplicaNeedsReplacement",
            .AvailableReplicaCount = 2,
            .DecommissionedReplicaCount = 1,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Underreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient,
            .ExpectedReplicationIndexCount = 1,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "DecommissionedReplicaCanBeRemoved",
            .AvailableReplicaCount = 3,
            .DecommissionedReplicaCount = 1,
            .TemporarilyUnavailableReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Overreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 1,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            // A temporarily unavailable replica counts towards overreplication
            // but must not be advised for removal.
            .Name = "TemporarilyUnavailableOverreplication",
            .AvailableReplicaCount = 3,
            .DecommissionedReplicaCount = 0,
            .TemporarilyUnavailableReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::Overreplicated | EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            // When both kinds of removal are possible, decommissioned replicas
            // are removed instead of balancing removal.
            .Name = "DecommissionedRemovalPreferredOverBalancing",
            .AvailableReplicaCount = 4,
            .DecommissionedReplicaCount = 1,
            .TemporarilyUnavailableReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::Overreplicated |
                EChunkStatus::UnexpectedOverreplicated |
                EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
            .ExpectedReplicationIndexCount = 0,
            .ExpectedDecommissionedRemovalCount = 1,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateChunk();
        TStoredChunkReplicaList replicas;
        for (int index = 0; index < testCase.AvailableReplicaCount; ++index) {
            replicas.push_back(CreateReplica(EReplicaAvailability::Available));
        }
        for (int index = 0; index < testCase.DecommissionedReplicaCount; ++index) {
            replicas.push_back(CreateReplica(EReplicaAvailability::Decommissioned));
        }
        for (int index = 0; index < testCase.TemporarilyUnavailableReplicaCount; ++index) {
            replicas.push_back(CreateReplica(EReplicaAvailability::TemporarilyUnavailable));
        }

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        auto it = statistics.PerMediumStatistics.find(DefaultStoreMediumIndex);
        ASSERT_NE(it, statistics.PerMediumStatistics.end());
        const auto& mediumStatistics = it->second;
        EXPECT_EQ(mediumStatistics.Status, testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, testCase.ExpectedCrossMediumStatus);
        EXPECT_EQ(
            mediumStatistics.ReplicaCount[NChunkClient::GenericChunkReplicaIndex],
            testCase.AvailableReplicaCount);
        EXPECT_EQ(
            mediumStatistics.DecommissionedReplicaCount[NChunkClient::GenericChunkReplicaIndex],
            testCase.DecommissionedReplicaCount);
        EXPECT_EQ(
            mediumStatistics.TemporarilyUnavailableReplicaCount[NChunkClient::GenericChunkReplicaIndex],
            testCase.TemporarilyUnavailableReplicaCount);
        EXPECT_EQ(
            std::ssize(mediumStatistics.ReplicationIndexes),
            testCase.ExpectedReplicationIndexCount);
        EXPECT_EQ(
            std::ssize(mediumStatistics.DecommissionedRemovalReplicas),
            testCase.ExpectedDecommissionedRemovalCount);
        EXPECT_EQ(
            std::ssize(mediumStatistics.BalancingRemovalIndexes),
            testCase.ExpectedBalancingRemovalIndexCount);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Aggregated replication.

TEST_F(TChunkStatisticsCalculatorTest, AggregatedReplicationFactorIsCappedByMediumConfig)
{
    auto chunk = CreateChunk();
    Medium_->Config()->MaxReplicationFactor = 2;

    EXPECT_EQ(
        StatisticsCalculator_->GetChunkAggregatedReplicationFactor(
            chunk.get(),
            DefaultStoreMediumIndex),
        2);
}

TEST_F(TChunkStatisticsCalculatorTest, AggregatedReplicationIncludesUnexpectedMedium)
{
    constexpr int UnexpectedMediumIndex = 1;

    CreateMedium(UnexpectedMediumIndex);
    auto chunk = CreateChunk();
    Medium_->Config()->MaxReplicationFactor = 2;
    TStoredChunkReplicaList replicas{
        CreateReplica(nullptr, UnexpectedMediumIndex),
    };

    auto replication = StatisticsCalculator_->GetChunkAggregatedReplication(chunk.get(), replicas);

    EXPECT_EQ(replication.Get(DefaultStoreMediumIndex).GetReplicationFactor(), 2);
    EXPECT_TRUE(replication.Contains(UnexpectedMediumIndex));
    EXPECT_EQ(replication.Get(UnexpectedMediumIndex).GetReplicationFactor(), 0);

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);
    const auto& unexpectedMediumStatistics =
        statistics.PerMediumStatistics[UnexpectedMediumIndex];
    EXPECT_EQ(
        unexpectedMediumStatistics.Status,
        EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated);
    EXPECT_EQ(
        unexpectedMediumStatistics.BalancingRemovalIndexes,
        (TCompactVector<int, TypicalReplicaCount>{NChunkClient::GenericChunkReplicaIndex}));
    EXPECT_TRUE(Any(statistics.Status & ECrossMediumChunkStatus::MediumWiseLost));
}

////////////////////////////////////////////////////////////////////////////////
// Regular chunks: cross-medium statuses.

TEST_F(TChunkStatisticsCalculatorTest, PrecariousWhenOnlyTransientReplicasRemain)
{
    constexpr int TransientMediumIndex = 1;

    CreateMedium(TransientMediumIndex, /*transient*/ true);
    auto chunk = CreateChunk();
    SetChunkReplication(chunk.get(), {
        {
            .MediumIndex = DefaultStoreMediumIndex,
            .ReplicationFactor = 3,
        },
        {
            .MediumIndex = TransientMediumIndex,
            .ReplicationFactor = 3,
        },
    });
    TStoredChunkReplicaList replicas{
        CreateReplica(nullptr, TransientMediumIndex),
        CreateReplica(nullptr, TransientMediumIndex),
        CreateReplica(nullptr, TransientMediumIndex),
    };

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& defaultMediumStatistics =
        statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(
        defaultMediumStatistics.Status,
        EChunkStatus::Lost | EChunkStatus::Underreplicated);
    EXPECT_EQ(
        defaultMediumStatistics.ReplicationIndexes,
        (TCompactVector<int, TypicalReplicaCount>{NChunkClient::GenericChunkReplicaIndex}));
    EXPECT_EQ(
        statistics.PerMediumStatistics[TransientMediumIndex].Status,
        EChunkStatus::None);
    EXPECT_EQ(
        statistics.Status,
        ECrossMediumChunkStatus::MediumWiseLost | ECrossMediumChunkStatus::Precarious);
}

TEST_F(TChunkStatisticsCalculatorTest, NoPrecariousWhenAllMediaTransient)
{
    constexpr int TransientMediumIndex = 1;

    CreateMedium(TransientMediumIndex, /*transient*/ true);
    auto chunk = CreateChunk();
    SetChunkReplication(chunk.get(), {
        {
            .MediumIndex = TransientMediumIndex,
            .ReplicationFactor = 3,
        },
    });
    TStoredChunkReplicaList replicas{
        CreateReplica(nullptr, TransientMediumIndex),
        CreateReplica(nullptr, TransientMediumIndex),
        CreateReplica(nullptr, TransientMediumIndex),
    };

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    EXPECT_EQ(
        statistics.PerMediumStatistics[TransientMediumIndex].Status,
        EChunkStatus::None);
    EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
}

////////////////////////////////////////////////////////////////////////////////
// Journal chunks.

TEST_F(TChunkStatisticsCalculatorTest, JournalChunkStateGrid)
{
    struct TTestCase
    {
        const char* Name;
        bool ChunkSealed;
        int SealedReplicaCount;
        int UnsealedReplicaCount;
        EChunkStatus ExpectedStatus;
        ECrossMediumChunkStatus ExpectedCrossMediumStatus;
        int ExpectedBalancingRemovalIndexCount;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "SealedChunkWithoutSealedReplicas",
            .ChunkSealed = true,
            .SealedReplicaCount = 0,
            .UnsealedReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::SealedMissing,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Sealed |
                ECrossMediumChunkStatus::QuorumMissing |
                ECrossMediumChunkStatus::Deficient,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "UnsealedChunkBelowReadQuorum",
            .ChunkSealed = false,
            .SealedReplicaCount = 0,
            .UnsealedReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::None,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::QuorumMissing,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            // An unsealed replica blocks overreplication handling: replicas may
            // not be removed until everything is sealed.
            .Name = "UnsealedReplicaSuppressesOverreplication",
            .ChunkSealed = true,
            .SealedReplicaCount = 3,
            .UnsealedReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::None,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Sealed,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
        {
            .Name = "OverreplicatedWhenTotallySealed",
            .ChunkSealed = true,
            .SealedReplicaCount = 4,
            .UnsealedReplicaCount = 0,
            .ExpectedStatus = EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Sealed,
            .ExpectedBalancingRemovalIndexCount = 1,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateChunk(
            EChunkType::Journal,
            NErasure::ECodec::None,
            testCase.ChunkSealed);
        chunk->SetReadQuorum(2);

        TStoredChunkReplicaList replicas;
        for (int index = 0; index < testCase.SealedReplicaCount; ++index) {
            replicas.push_back(CreateReplica(
                nullptr,
                DefaultStoreMediumIndex,
                NChunkClient::GenericChunkReplicaIndex,
                EChunkReplicaState::Sealed));
        }
        for (int index = 0; index < testCase.UnsealedReplicaCount; ++index) {
            replicas.push_back(CreateReplica(
                nullptr,
                DefaultStoreMediumIndex,
                NChunkClient::GenericChunkReplicaIndex,
                EChunkReplicaState::Unsealed));
        }

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        EXPECT_EQ(
            statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
            testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, testCase.ExpectedCrossMediumStatus);
        EXPECT_EQ(
            std::ssize(statistics.PerMediumStatistics[DefaultStoreMediumIndex].BalancingRemovalIndexes),
            testCase.ExpectedBalancingRemovalIndexCount);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Erasure chunks: single medium.

TEST_F(TChunkStatisticsCalculatorTest, ErasureErasedPartsGrid)
{
    struct TTestCase
    {
        const char* Name;
        std::vector<int> MissingIndexes;
        EChunkStatus ExpectedStatus;
        ECrossMediumChunkStatus ExpectedCrossMediumStatus;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "Healthy",
            .MissingIndexes = {},
            .ExpectedStatus = EChunkStatus::None,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
        },
        {
            .Name = "DataMissing",
            .MissingIndexes = {0},
            .ExpectedStatus = EChunkStatus::DataMissing,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::DataMissing,
        },
        {
            .Name = "ParityMissing",
            .MissingIndexes = {TestErasureTotalPartCount - 1},
            .ExpectedStatus = EChunkStatus::ParityMissing,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::ParityMissing,
        },
        {
            .Name = "DataAndParityMissing",
            .MissingIndexes = {0, TestErasureTotalPartCount - 1},
            .ExpectedStatus = EChunkStatus::DataMissing | EChunkStatus::ParityMissing,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient |
                ECrossMediumChunkStatus::DataMissing |
                ECrossMediumChunkStatus::ParityMissing,
        },
        {
            // Exactly as many erasures as the codec can repair.
            .Name = "RepairableAtLimit",
            .MissingIndexes = {0, 1, 2},
            .ExpectedStatus = EChunkStatus::DataMissing,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::DataMissing,
        },
        {
            .Name = "Lost",
            .MissingIndexes = {0, 1, 2, 3},
            .ExpectedStatus = EChunkStatus::DataMissing | EChunkStatus::Lost,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Lost |
                ECrossMediumChunkStatus::Deficient |
                ECrossMediumChunkStatus::DataMissing,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateErasureChunk();
        auto replicas = CreateErasurePartReplicas(testCase.MissingIndexes);

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        EXPECT_EQ(
            statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
            testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, testCase.ExpectedCrossMediumStatus);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureTemporarilyUnavailablePartsGrid)
{
    // Temporary unavailability is safe as long as the number of unavailable parts
    // plus the per-rack replica limit (1 in these tests) does not exceed the
    // guaranteed repairable part count of the codec.
    struct TTestCase
    {
        const char* Name;
        int TemporarilyUnavailablePartCount;
        EChunkStatus ExpectedStatus;
        ECrossMediumChunkStatus ExpectedCrossMediumStatus;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "SinglePartIsSafe",
            .TemporarilyUnavailablePartCount = 1,
            .ExpectedStatus = EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
        },
        {
            .Name = "RepairGuaranteeBoundaryIsSafe",
            .TemporarilyUnavailablePartCount = TestErasureGuaranteedRepairablePartCount - 1,
            .ExpectedStatus = EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::None,
        },
        {
            .Name = "BeyondRepairGuaranteeIsMissing",
            .TemporarilyUnavailablePartCount = TestErasureGuaranteedRepairablePartCount,
            .ExpectedStatus = EChunkStatus::DataMissing | EChunkStatus::TemporarilyUnavailable,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::Deficient | ECrossMediumChunkStatus::DataMissing,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateErasureChunk();

        TStoredChunkReplicaList replicas;
        for (int replicaIndex = 0; replicaIndex < TestErasureTotalPartCount; ++replicaIndex) {
            if (replicaIndex < testCase.TemporarilyUnavailablePartCount) {
                replicas.push_back(CreateReplica(
                    EReplicaAvailability::TemporarilyUnavailable,
                    DefaultStoreMediumIndex,
                    replicaIndex));
            } else {
                replicas.push_back(CreateReplica(
                    nullptr,
                    DefaultStoreMediumIndex,
                    replicaIndex));
            }
        }

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        const auto& mediumStatistics =
            statistics.PerMediumStatistics[DefaultStoreMediumIndex];
        EXPECT_EQ(mediumStatistics.Status, testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, testCase.ExpectedCrossMediumStatus);
        EXPECT_EQ(mediumStatistics.TemporarilyUnavailableReplicaCount[0], 1);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureDecommissionedPartGrid)
{
    struct TTestCase
    {
        const char* Name;
        int DecommissionedPartIndex;
        EChunkStatus ExpectedStatus;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "DataPart",
            .DecommissionedPartIndex = 0,
            .ExpectedStatus = EChunkStatus::DataDecommissioned,
        },
        {
            .Name = "ParityPart",
            .DecommissionedPartIndex = TestErasureTotalPartCount - 1,
            .ExpectedStatus = EChunkStatus::ParityDecommissioned,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateErasureChunk();

        auto replicas = CreateErasurePartReplicas({testCase.DecommissionedPartIndex});
        replicas.push_back(CreateReplica(
            EReplicaAvailability::Decommissioned,
            DefaultStoreMediumIndex,
            testCase.DecommissionedPartIndex));

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        EXPECT_EQ(
            statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
            testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::Deficient);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureOverreplicatedPartRemovalGrid)
{
    struct TTestCase
    {
        const char* Name;
        EReplicaAvailability ExtraReplicaAvailability;
        EChunkStatus ExpectedStatus;
        int ExpectedDecommissionedRemovalCount;
        int ExpectedBalancingRemovalIndexCount;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "ExtraReplicaTriggersBalancingRemoval",
            .ExtraReplicaAvailability = EReplicaAvailability::Available,
            .ExpectedStatus = EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated,
            .ExpectedDecommissionedRemovalCount = 0,
            .ExpectedBalancingRemovalIndexCount = 1,
        },
        {
            .Name = "ExtraDecommissionedReplicaIsRemoved",
            .ExtraReplicaAvailability = EReplicaAvailability::Decommissioned,
            .ExpectedStatus = EChunkStatus::Overreplicated,
            .ExpectedDecommissionedRemovalCount = 1,
            .ExpectedBalancingRemovalIndexCount = 0,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateErasureChunk();

        auto replicas = CreateErasurePartReplicas();
        replicas.push_back(CreateReplica(
            testCase.ExtraReplicaAvailability,
            DefaultStoreMediumIndex,
            /*replicaIndex*/ 0));

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        const auto& mediumStatistics =
            statistics.PerMediumStatistics[DefaultStoreMediumIndex];
        EXPECT_EQ(mediumStatistics.Status, testCase.ExpectedStatus);
        EXPECT_EQ(
            std::ssize(mediumStatistics.DecommissionedRemovalReplicas),
            testCase.ExpectedDecommissionedRemovalCount);
        EXPECT_EQ(
            std::ssize(mediumStatistics.BalancingRemovalIndexes),
            testCase.ExpectedBalancingRemovalIndexCount);
        EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasurePartClashOnSameNodeGrid)
{
    struct TTestCase
    {
        const char* Name;
        bool AllowMultipleErasurePartsPerNode;
        EChunkStatus ExpectedStatus;
        TCompactVector<int, TypicalReplicaCount> ExpectedReplicationIndexes;
    };

    const std::vector<TTestCase> testCases{
        {
            // The clashing part is counted as decommissioned, and since its node is
            // not actually decommissioned, replication is advised.
            .Name = "ClashingPartNeedsReplication",
            .AllowMultipleErasurePartsPerNode = false,
            .ExpectedStatus = EChunkStatus::Underreplicated,
            .ExpectedReplicationIndexes = {1},
        },
        {
            .Name = "MultiplePartsPerNodeAllowed",
            .AllowMultipleErasurePartsPerNode = true,
            .ExpectedStatus = EChunkStatus::None,
            .ExpectedReplicationIndexes = {},
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        Config_->AllowMultipleErasurePartsPerNode = testCase.AllowMultipleErasurePartsPerNode;

        auto chunk = CreateErasureChunk();

        auto* sharedNode = CreateNode();
        TStoredChunkReplicaList replicas{
            CreateReplica(sharedNode, DefaultStoreMediumIndex, /*replicaIndex*/ 0),
            CreateReplica(sharedNode, DefaultStoreMediumIndex, /*replicaIndex*/ 1),
        };
        for (int replicaIndex = 2; replicaIndex < TestErasureTotalPartCount; ++replicaIndex) {
            replicas.push_back(CreateReplica(
                nullptr,
                DefaultStoreMediumIndex,
                replicaIndex));
        }

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        const auto& mediumStatistics =
            statistics.PerMediumStatistics[DefaultStoreMediumIndex];
        EXPECT_EQ(mediumStatistics.Status, testCase.ExpectedStatus);
        EXPECT_EQ(mediumStatistics.ReplicationIndexes, testCase.ExpectedReplicationIndexes);
        EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureDataPartsOnlyDoesNotRequireParity)
{
    auto chunk = CreateErasureChunk({
        {
            .MediumIndex = DefaultStoreMediumIndex,
            .ReplicationFactor = 1,
            .DataPartsOnly = true,
        },
    });

    TStoredChunkReplicaList replicas;
    for (int replicaIndex = 0; replicaIndex < TestErasureDataPartCount; ++replicaIndex) {
        replicas.push_back(CreateReplica(
            nullptr,
            DefaultStoreMediumIndex,
            replicaIndex));
    }

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    EXPECT_EQ(
        statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
        EChunkStatus::None);
    EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
}

////////////////////////////////////////////////////////////////////////////////
// Erasure chunks: cross-medium.

TEST_F(TChunkStatisticsCalculatorTest, ErasurePartsSplitAcrossMediaAdviseCrossMediumReplication)
{
    constexpr int SecondMediumIndex = 1;

    CreateMedium(SecondMediumIndex);
    auto chunk = CreateErasureChunk({
        {
            .MediumIndex = DefaultStoreMediumIndex,
            .ReplicationFactor = 1,
        },
        {
            .MediumIndex = SecondMediumIndex,
            .ReplicationFactor = 1,
        },
    });

    // Parts 0-5 on the default medium, parts 3-8 on the second one: each medium
    // misses a repairable set of parts that is present on the other one.
    TStoredChunkReplicaList replicas;
    for (int replicaIndex = 0; replicaIndex < TestErasureDataPartCount; ++replicaIndex) {
        replicas.push_back(CreateReplica(
            nullptr,
            DefaultStoreMediumIndex,
            replicaIndex));
    }
    for (int replicaIndex = TestErasureTotalPartCount - TestErasureDataPartCount;
        replicaIndex < TestErasureTotalPartCount;
        ++replicaIndex)
    {
        replicas.push_back(CreateReplica(
            nullptr,
            SecondMediumIndex,
            replicaIndex));
    }

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& defaultMediumStatistics =
        statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(
        defaultMediumStatistics.Status,
        EChunkStatus::ParityMissing | EChunkStatus::Underreplicated);
    EXPECT_EQ(
        defaultMediumStatistics.ReplicationIndexes,
        (TCompactVector<int, TypicalReplicaCount>{6, 7, 8}));

    const auto& secondMediumStatistics =
        statistics.PerMediumStatistics[SecondMediumIndex];
    EXPECT_EQ(
        secondMediumStatistics.Status,
        EChunkStatus::DataMissing | EChunkStatus::Underreplicated);
    EXPECT_EQ(
        secondMediumStatistics.ReplicationIndexes,
        (TCompactVector<int, TypicalReplicaCount>{0, 1, 2}));

    EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::Deficient);
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureMediumWiseLostGrid)
{
    struct TTestCase
    {
        const char* Name;
        bool SecondMediumTransient;
        ECrossMediumChunkStatus ExpectedCrossMediumStatus;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "PersistentCopyIsNotPrecarious",
            .SecondMediumTransient = false,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::MediumWiseLost,
        },
        {
            .Name = "TransientOnlyCopyIsPrecarious",
            .SecondMediumTransient = true,
            .ExpectedCrossMediumStatus = ECrossMediumChunkStatus::MediumWiseLost | ECrossMediumChunkStatus::Precarious,
        },
    };

    int nextMediumIndex = 1;
    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto secondMediumIndex = nextMediumIndex++;
        CreateMedium(secondMediumIndex, testCase.SecondMediumTransient);
        auto chunk = CreateErasureChunk({
            {
                .MediumIndex = DefaultStoreMediumIndex,
                .ReplicationFactor = 1,
            },
            {
                .MediumIndex = secondMediumIndex,
                .ReplicationFactor = 1,
            },
        });

        // All parts reside on the second medium only.
        auto replicas = CreateErasurePartReplicas(/*missingIndexes*/ {}, secondMediumIndex);

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        const auto& defaultMediumStatistics =
            statistics.PerMediumStatistics[DefaultStoreMediumIndex];
        EXPECT_EQ(
            defaultMediumStatistics.Status,
            EChunkStatus::Lost |
            EChunkStatus::DataMissing |
            EChunkStatus::ParityMissing |
            EChunkStatus::Underreplicated);
        EXPECT_EQ(
            std::ssize(defaultMediumStatistics.ReplicationIndexes),
            TestErasureTotalPartCount);
        EXPECT_EQ(
            statistics.PerMediumStatistics[secondMediumIndex].Status,
            EChunkStatus::None);
        EXPECT_EQ(statistics.Status, testCase.ExpectedCrossMediumStatus);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasureUnsealedJournalQuorum)
{
    auto chunk = CreateChunk(EChunkType::Journal, TestErasureCodec, /*sealed*/ false);
    chunk->SetReadQuorum(7);
    SetChunkReplication(chunk.get(), {
        {
            .MediumIndex = DefaultStoreMediumIndex,
            .ReplicationFactor = 1,
        },
    });

    TStoredChunkReplicaList replicas;
    for (int replicaIndex = 0; replicaIndex < TestErasureDataPartCount; ++replicaIndex) {
        replicas.push_back(CreateReplica(
            nullptr,
            DefaultStoreMediumIndex,
            replicaIndex,
            EChunkReplicaState::Sealed));
    }

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    EXPECT_EQ(
        statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
        EChunkStatus::ParityMissing);
    EXPECT_EQ(
        statistics.Status,
        ECrossMediumChunkStatus::QuorumMissing |
        ECrossMediumChunkStatus::ParityMissing |
        ECrossMediumChunkStatus::Deficient);
}

////////////////////////////////////////////////////////////////////////////////
// Placement safety: racks, data centers and hosts.

TEST_F(TChunkStatisticsCalculatorTest, RackAwarePlacementGrid)
{
    struct TTestCase
    {
        const char* Name;
        int ExtraRacklessReplicaCount;
        EChunkStatus ExpectedStatus;
        int ExpectedReplicationIndexCount;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "RackLimitViolationIsUnsafe",
            .ExtraRacklessReplicaCount = 1,
            .ExpectedStatus = EChunkStatus::UnsafelyPlaced,
            .ExpectedReplicationIndexCount = 1,
        },
        {
            // Overreplication takes precedence over unsafe placement.
            .Name = "OverreplicationSuppressesUnsafePlacement",
            .ExtraRacklessReplicaCount = 2,
            .ExpectedStatus = EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated,
            .ExpectedReplicationIndexCount = 0,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        auto chunk = CreateChunk();

        auto* rack = CreateRack();
        TStoredChunkReplicaList replicas{
            CreateReplica(CreateNodeInRack(rack)),
            CreateReplica(CreateNodeInRack(rack)),
        };
        for (int index = 0; index < testCase.ExtraRacklessReplicaCount; ++index) {
            replicas.push_back(CreateReplica());
        }

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        const auto& mediumStatistics =
            statistics.PerMediumStatistics[DefaultStoreMediumIndex];
        EXPECT_EQ(mediumStatistics.Status, testCase.ExpectedStatus);
        EXPECT_EQ(
            std::ssize(mediumStatistics.ReplicationIndexes),
            testCase.ExpectedReplicationIndexCount);
        EXPECT_EQ(
            static_cast<bool>(mediumStatistics.UnsafelyPlacedReplica),
            Any(testCase.ExpectedStatus & EChunkStatus::UnsafelyPlaced));
        EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, DataCenterLimitViolationIsUnsafe)
{
    Callbacks_->SetMaxReplicasPerDataCenter(1);

    auto chunk = CreateChunk();

    // Two replicas in distinct racks of the same data center do not violate
    // the per-rack limit but do violate the per-data-center one.
    auto* dataCenter = CreateDataCenter();
    TStoredChunkReplicaList replicas{
        CreateReplica(CreateNodeInRack(CreateRack(dataCenter))),
        CreateReplica(CreateNodeInRack(CreateRack(dataCenter))),
        CreateReplica(),
    };

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics =
        statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(mediumStatistics.Status, EChunkStatus::UnsafelyPlaced);
    EXPECT_TRUE(mediumStatistics.UnsafelyPlacedReplica);
    EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
}

TEST_F(TChunkStatisticsCalculatorTest, HostAwarePlacementGrid)
{
    struct TTestCase
    {
        const char* Name;
        bool UseHostAwareReplicator;
        EChunkStatus ExpectedStatus;
    };

    const std::vector<TTestCase> testCases{
        {
            .Name = "SameHostReplicasAreUnsafe",
            .UseHostAwareReplicator = true,
            .ExpectedStatus = EChunkStatus::UnsafelyPlaced,
        },
        {
            .Name = "SameHostReplicasAreIgnoredWhenDisabled",
            .UseHostAwareReplicator = false,
            .ExpectedStatus = EChunkStatus::None,
        },
    };

    for (const auto& testCase : testCases) {
        SCOPED_TRACE(testCase.Name);

        DynamicConfig_->UseHostAwareReplicator = testCase.UseHostAwareReplicator;

        auto chunk = CreateChunk();

        auto* sharedHost = CreateHost();
        TStoredChunkReplicaList replicas{
            CreateReplica(CreateNodeOnHost(sharedHost)),
            CreateReplica(CreateNodeOnHost(sharedHost)),
            CreateReplica(),
        };

        auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

        EXPECT_EQ(
            statistics.PerMediumStatistics[DefaultStoreMediumIndex].Status,
            testCase.ExpectedStatus);
        EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
    }
}

TEST_F(TChunkStatisticsCalculatorTest, ErasurePartsInSameRackAreUnsafelyPlaced)
{
    constexpr int FirstClashingPartIndex = 3;
    constexpr int SecondClashingPartIndex = 4;

    auto chunk = CreateErasureChunk();

    auto* rack = CreateRack();
    TStoredChunkReplicaList replicas;
    for (int replicaIndex = 0; replicaIndex < TestErasureTotalPartCount; ++replicaIndex) {
        auto inRack =
            replicaIndex == FirstClashingPartIndex ||
            replicaIndex == SecondClashingPartIndex;
        replicas.push_back(CreateReplica(
            inRack ? CreateNodeInRack(rack) : nullptr,
            DefaultStoreMediumIndex,
            replicaIndex));
    }

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics =
        statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(mediumStatistics.Status, EChunkStatus::UnsafelyPlaced);
    EXPECT_EQ(
        mediumStatistics.ReplicationIndexes,
        (TCompactVector<int, TypicalReplicaCount>{SecondClashingPartIndex}));
    ASSERT_TRUE(mediumStatistics.UnsafelyPlacedReplica);
    EXPECT_EQ(mediumStatistics.UnsafelyPlacedReplica.GetReplicaIndex(), SecondClashingPartIndex);
    EXPECT_EQ(statistics.Status, ECrossMediumChunkStatus::None);
}

////////////////////////////////////////////////////////////////////////////////
// Consistent replica placement.

TEST_F(TChunkStatisticsCalculatorTest, SkipsConsistentPlacementWithoutHash)
{
    auto chunk = CreateChunk();
    DynamicConfig_->ConsistentReplicaPlacement->Enable = true;
    Callbacks_->SetConsistentPlacementWriteTargets(
        CreateConsistentPlacementWriteTargets(DefaultReplicationFactor));

    TStoredChunkReplicaList replicas{CreateReplica()};
    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_TRUE(mediumStatistics.MissingReplicas.empty());
    EXPECT_TRUE(None(mediumStatistics.Status & EChunkStatus::InconsistentlyPlaced));
    EXPECT_EQ(Callbacks_->GetConsistentPlacementWriteTargetsCallCount(), 0);
}

TEST_F(TChunkStatisticsCalculatorTest, SkipsConsistentPlacementWhenDisabled)
{
    auto chunk = CreateChunk();
    chunk->SetConsistentReplicaPlacementHash(1);
    DynamicConfig_->ConsistentReplicaPlacement->Enable = false;
    Callbacks_->SetConsistentPlacementWriteTargets(
        CreateConsistentPlacementWriteTargets(DefaultReplicationFactor));

    TStoredChunkReplicaList replicas{CreateReplica()};
    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_TRUE(mediumStatistics.MissingReplicas.empty());
    EXPECT_TRUE(None(mediumStatistics.Status & EChunkStatus::InconsistentlyPlaced));
    EXPECT_EQ(Callbacks_->GetConsistentPlacementWriteTargetsCallCount(), 0);
}

TEST_F(TChunkStatisticsCalculatorTest, SkipsConsistentPlacementWithoutWriteTargets)
{
    auto chunk = CreateChunk();
    chunk->SetConsistentReplicaPlacementHash(1);
    DynamicConfig_->ConsistentReplicaPlacement->Enable = true;

    TStoredChunkReplicaList replicas{CreateReplica()};
    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_TRUE(mediumStatistics.MissingReplicas.empty());
    EXPECT_TRUE(None(mediumStatistics.Status & EChunkStatus::InconsistentlyPlaced));
    EXPECT_EQ(Callbacks_->GetConsistentPlacementWriteTargetsCallCount(), 1);
}

TEST_F(TChunkStatisticsCalculatorTest, ReportsMissingConsistentPlacementReplicas)
{
    auto chunk = CreateChunk();
    chunk->SetConsistentReplicaPlacementHash(1);
    DynamicConfig_->ConsistentReplicaPlacement->Enable = true;

    auto targets = CreateConsistentPlacementWriteTargets(DefaultReplicationFactor);
    Callbacks_->SetConsistentPlacementWriteTargets(targets);

    TStoredChunkReplicaList replicas{CreateReplica(targets.front())};
    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    ASSERT_EQ(std::ssize(mediumStatistics.MissingReplicas), DefaultReplicationFactor - 1);
    EXPECT_EQ(mediumStatistics.MissingReplicas.front().GetPtr(), targets[1]);
    EXPECT_TRUE(None(mediumStatistics.Status & EChunkStatus::InconsistentlyPlaced));
}

TEST_F(TChunkStatisticsCalculatorTest, ReportsInconsistentPlacement)
{
    auto chunk = CreateChunk();
    chunk->SetConsistentReplicaPlacementHash(1);
    DynamicConfig_->ConsistentReplicaPlacement->Enable = true;

    Callbacks_->SetConsistentPlacementWriteTargets(
        CreateConsistentPlacementWriteTargets(DefaultReplicationFactor));

    TStoredChunkReplicaList replicas{CreateReplica()};
    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(std::ssize(mediumStatistics.MissingReplicas), DefaultReplicationFactor);
    EXPECT_EQ(
        mediumStatistics.Status,
        EChunkStatus::Underreplicated | EChunkStatus::InconsistentlyPlaced);
    EXPECT_EQ(
        mediumStatistics.ReplicationIndexes,
        (TCompactVector<int, TypicalReplicaCount>{NChunkClient::GenericChunkReplicaIndex}));
}

TEST_F(TChunkStatisticsCalculatorTest, RemovesInconsistentlyPlacedReplicaWhenOverreplicated)
{
    auto chunk = CreateChunk();
    chunk->SetConsistentReplicaPlacementHash(1);
    DynamicConfig_->ConsistentReplicaPlacement->Enable = true;

    auto targets = CreateConsistentPlacementWriteTargets(DefaultReplicationFactor);
    Callbacks_->SetConsistentPlacementWriteTargets(targets);

    TStoredChunkReplicaList replicas;
    for (auto* target : targets) {
        replicas.push_back(CreateReplica(target));
    }
    replicas.push_back(CreateReplica());
    auto* strangerLocation = Locations_.back().get();

    auto statistics = StatisticsCalculator_->ComputeChunkStatistics(chunk.get(), replicas);

    const auto& mediumStatistics = statistics.PerMediumStatistics[DefaultStoreMediumIndex];
    EXPECT_EQ(
        mediumStatistics.Status,
        EChunkStatus::Overreplicated | EChunkStatus::UnexpectedOverreplicated);
    ASSERT_EQ(std::ssize(mediumStatistics.DecommissionedRemovalReplicas), 1);
    EXPECT_EQ(mediumStatistics.DecommissionedRemovalReplicas.front().GetPtr(), strangerLocation);
    EXPECT_TRUE(mediumStatistics.BalancingRemovalIndexes.empty());
    EXPECT_TRUE(mediumStatistics.MissingReplicas.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
