#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_scraper.h>
#include <yt/yt/ytlib/chunk_client/throttler_manager.h>

#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <library/cpp/yt/misc/range_helpers.h>

#include <util/generic/guid.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NYTree;

using namespace testing;

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<T> RemoveAt(std::vector<T> vector, int position)
{
    vector.at(position) = std::move(vector.back());
    vector.pop_back();
    return std::move(vector);
}

class TChunkScraperBaseTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
    TActionQueuePtr ScraperActionQueue_ = New<TActionQueue>("ScraperActionQueue");

    TPromise<void> TestFinishedPromise_ = NewPromise<void>();
    TFuture<void> TestFinishedFuture_ = TestFinishedPromise_;

    NLogging::TLogger Logger = CppTestsLogger();

    void TearDown() override
    {
        ASSERT_TRUE(TestFinishedFuture_.IsSet());
    }

    [[nodiscard]] TChunkScraperPtr MakeScraper(
        TChunkBatchLocatedHandler callback,
        int batchSize = 1,
        TChunkScraperAvailabilityPolicy availabilityPolicy = MetadataAvailablePolicy,
        bool prioritizeUnavailable = true,
        std::optional<int> throttlerLimit = {})
    {
        auto scraperConfig = New<TChunkScraperConfig>();
        scraperConfig->MaxChunksPerRequest = batchSize;
        scraperConfig->PrioritizeUnavailableChunks = prioritizeUnavailable;

        return New<TChunkScraper>(
            std::move(scraperConfig),
            ScraperActionQueue_->GetInvoker(),
            ScraperActionQueue_->GetInvoker(),
            New<TThrottlerManager>(TThroughputThrottlerConfig::Create(throttlerLimit)),
            NativeClient_,
            NativeClient_->GetNativeConnection()->GetNodeDirectory(),
            std::move(callback),
            availabilityPolicy,
            Logger);
    }

    void StartScraperWithChunks(const TChunkScraperPtr& scraper, std::vector<TChunkId> chunkIds)
    {
        ScraperActionQueue_->GetInvoker()->Invoke(BIND([scraper, chunkIds = std::move(chunkIds)] {
            for (auto chunkId : chunkIds) {
                scraper->Add(chunkId);
            }
            scraper->Start();
        }));
    }

    template <class THandler>
    static TChunkBatchLocatedHandler MakeEpochAwareHandler(THandler&& handler)
    {
        return BIND([
            handler = std::forward<THandler>(handler),
            epoch = 0
        ] (const std::vector<TScrapedChunkInfo>& info) mutable {
            return handler(info, epoch++);
        });
    }

    template <class THandler>
    static TChunkBatchLocatedHandler MakeLimitedEpochAwareHandler(THandler&& handler, int epochsLimit)
    {
        return MakeEpochAwareHandler([
            handler = std::forward<THandler>(handler),
            epochsLimit
        ] (const std::vector<TScrapedChunkInfo>& info, int epoch) mutable {
            if (epoch >= epochsLimit) {
                return;
            }
            handler(info, epoch);
        });
    }

    template <class... TEpochHandlers>
    static TChunkBatchLocatedHandler CombineEpochHandlers(TEpochHandlers&&... handlers)
    {
        return MakeLimitedEpochAwareHandler(
            [
                handlers = std::array{BIND(std::forward<TEpochHandlers>(handlers))...}
            ] (const std::vector<TScrapedChunkInfo>& info, int epoch) mutable {
                std::move(handlers[epoch])(info);
            },
            sizeof...(handlers));
    }

    template <class THandler>
    TChunkBatchLocatedHandler MakeFinishingEpochAwareHandler(THandler&& handler, int finishAfter)
    {
        return MakeLimitedEpochAwareHandler(
            [
                handler = std::forward<THandler>(handler),
                promise = std::move(TestFinishedPromise_),
                finishAfter
            ] (const std::vector<TScrapedChunkInfo>& info, int epoch) mutable {
                auto finallyFinishTest = Finally([&] {
                    if (epoch + 1 == finishAfter) {
                        promise.Set();
                    }
                });
                handler(info, epoch);
            },
            finishAfter);
    }

    static NCypressClient::TNodeId CreateFile(const NApi::TCreateNodeOptions& options = {})
    {
        return WaitFor(Client_->CreateNode("//tmp/" + CreateGuidAsString(), EObjectType::File, options))
            .ValueOrThrow();
    }

    static void WriteSampleData(NCypressClient::TNodeId fileId)
    {
        auto writer = Client_->CreateFileWriter(Format("#%v", fileId));
        WaitFor(writer->Open())
            .ThrowOnError();
        WaitFor(writer->Write(TSharedRef::FromString("Hello, World"))).ThrowOnError();
        WaitFor(writer->Close())
            .ThrowOnError();
    }

    static TChunkId CreateChunk(int replicationFactor = 1)
    {
        NApi::TCreateNodeOptions options;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("replication_factor", replicationFactor);

        auto fileId = CreateFile(options);
        WriteSampleData(fileId);
        auto chunkIds = ConvertTo<std::vector<TChunkId>>(
            WaitFor(Client_->GetNode(Format("#%v/@chunk_ids", fileId)))
                .ValueOrThrow());
        YT_VERIFY(chunkIds.size() == 1u);
        return chunkIds.front();
    }

    static std::vector<TChunkId> CreateChunks(int chunksCount = 10, int replicationFactor = 1)
    {
        std::vector<TChunkId> result;
        result.reserve(chunksCount);
        std::generate_n(std::back_inserter(result), chunksCount, [=] { return CreateChunk(replicationFactor); });
        return result;
    }

    static std::vector<std::string> GetChunkReplicaNodes(TChunkId chunkId)
    {
        return ConvertTo<std::vector<std::string>>(
            WaitFor(Client_->GetNode(Format("#%v/@stored_replicas", chunkId)))
                .ValueOrThrow());
    }

    static void UnbanDataNode(const std::string& dataNode)
    {
        WaitFor(Client_->RemoveMaintenance(EMaintenanceComponent::ClusterNode, dataNode, {}))
            .ThrowOnError();
        WaitUntil(
            [&] {
                return ConvertTo<ENodeState>(
                    WaitFor(Client_->GetNode(Format("//sys/cluster_nodes/%v/@state", dataNode)))
                        .ValueOrThrow()) == NNodeTrackerClient::ENodeState::Online;
            },
            "Node is offline");
    }

    static std::vector<std::string> GetDataNodes()
    {
        return ConvertTo<std::vector<std::string>>(
            WaitFor(Client_->ListNode(Format("//sys/data_nodes")))
                .ValueOrThrow());
    }

    [[nodiscard]] static auto BanDataNode(const std::string& dataNode)
    {
        WaitFor(Client_->AddMaintenance(EMaintenanceComponent::ClusterNode, dataNode, EMaintenanceType::Ban, "test"))
            .ThrowOnError();
        WaitUntil(
            [&] {
                return ConvertTo<ENodeState>(
                    WaitFor(Client_->GetNode(Format("//sys/cluster_nodes/%v/@state", dataNode)))
                        .ValueOrThrow()) == ENodeState::Offline;
            },
            "Node is online");
        return Finally([dataNode] {
            UnbanDataNode(dataNode);
        });
    }

    [[nodiscard]] static auto BanDataNodes(const std::vector<std::string>& dataNodes)
    {
        return TransformRangeTo<std::vector<decltype(BanDataNode(std::declval<std::string>()))>>(
            dataNodes,
            &BanDataNode);
    }

    [[nodiscard]] static auto BanAllNodes()
    {
        return BanDataNodes(GetDataNodes());
    }

    static std::vector<TChunkId> ExtractChunkIds(const std::vector<TScrapedChunkInfo>& info)
    {
        return TransformRangeTo<std::vector<TChunkId>>(info, &TScrapedChunkInfo::ChunkId);
    }

    static auto ChunkIdsAre(const std::vector<TChunkId>& expectedChunkIds) {
        return ResultOf("ChunkIds", &ExtractChunkIds, ContainerEq(expectedChunkIds));
    }

    static auto ChunkIdIs(TChunkId chunkId)
    {
        return Field("ChunkId", &TScrapedChunkInfo::ChunkId, chunkId);
    }

    static auto ReplicasAre(auto&& matcher)
    {
        return Field("Replicas", &TScrapedChunkInfo::Replicas, std::forward<decltype(matcher)>(matcher));
    }

    static auto AvailabilityIs(EChunkAvailability availability)
    {
        return Field("Availability", &TScrapedChunkInfo::Availability, availability);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkScraperBaseTest, RemovePreservesOrder)
{
    auto chunkIds = CreateChunks();

    TChunkScraperPtr scraper = MakeScraper(
        CombineEpochHandlers(
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                scraper->Remove(chunkIds.back());
                chunkIds.pop_back();
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                scraper->Remove(chunkIds.front());
                chunkIds.erase(chunkIds.begin());
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                scraper->Remove(chunkIds[1]);
                chunkIds.erase(std::next(chunkIds.begin()));
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                for (auto chunkId : chunkIds) {
                    scraper->Remove(chunkId);
                }
                TestFinishedPromise_.Set();
            },
            [] (const std::vector<TScrapedChunkInfo>& info) {
                ADD_FAILURE() << "Scraper should stop when there are no more chunks to locate, but located something: " << PrintToString(info);
            }),
        chunkIds.size());

    StartScraperWithChunks(scraper, chunkIds);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TChunkScraperBaseTest, RemovedChunksLocatedMissing)
{
    auto fileId = CreateFile();
    WriteSampleData(fileId);
    auto chunkIds = ConvertTo<std::vector<TChunkId>>(
        WaitFor(Client_->GetNode(Format("#%v/@chunk_ids", fileId)))
            .ValueOrThrow());
    ASSERT_THAT(chunkIds, SizeIs(1));
    auto chunkId = chunkIds.front();

    WaitFor(Client_->RemoveNode(Format("#%v", fileId)))
        .ThrowOnError();
    WaitUntil(
        [&] {
            return !WaitFor(Client_->NodeExists(Format("#%v", chunkId)))
                .ValueOrThrow();
        },
        "Chunk still exists");

    auto scraper = MakeScraper(
        CombineEpochHandlers(
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ElementsAre(AllOf(ChunkIdIs(chunkId), ReplicasAre(IsEmpty()), AvailabilityIs(EChunkAvailability::Missing))));
                TestFinishedPromise_.Set();
            },
            [] (const std::vector<TScrapedChunkInfo>& info) {
                ADD_FAILURE() << "Scraper should stop when there are no more chunks to locate, but located something: " << PrintToString(info);
            }));

    StartScraperWithChunks(scraper, {chunkId});
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TChunkScraperBaseTest, UnbannedChunkLocatedAvailable)
{
    auto chunkIds = CreateChunks();

    auto locatedUnavailablePromise = NewPromise<void>();
    auto locatedUnavailableFuture = locatedUnavailablePromise.ToFuture();

    auto nodesUnbannedPromise = NewPromise<void>();
    auto nodesUnbannedFuture = nodesUnbannedPromise.ToFuture();

    auto scraper = MakeScraper(CombineEpochHandlers(
        [&] (const std::vector<TScrapedChunkInfo>& info) {
            EXPECT_THAT(info, ChunkIdsAre(chunkIds));
            EXPECT_THAT(info, Each(testing::AllOf(ReplicasAre(IsEmpty()), AvailabilityIs(EChunkAvailability::Unavailable))));
            locatedUnavailablePromise.Set();
            WaitFor(nodesUnbannedFuture)
                .ThrowOnError();
        },
        [&] (const std::vector<TScrapedChunkInfo>& info) {
            EXPECT_THAT(info, ChunkIdsAre(chunkIds));
            EXPECT_THAT(info, Each(testing::AllOf(ReplicasAre(SizeIs(1)), AvailabilityIs(EChunkAvailability::Available))));
            TestFinishedPromise_.Set();
        }),
        chunkIds.size());

    {
        auto banGuard = BanAllNodes();
        StartScraperWithChunks(scraper, chunkIds);
        WaitFor(locatedUnavailableFuture)
            .ThrowOnError();
    }

    nodesUnbannedPromise.Set();
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TChunkScraperBaseTest, OnBecameUnavailableWorks)
{
    const auto chunkIds = CreateChunks(5);

    TChunkScraperPtr scraper = MakeScraper(
        CombineEpochHandlers(
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(
                    info,
                    ElementsAre(AllOf(
                        ChunkIdIs(chunkIds.front()),
                        AvailabilityIs(EChunkAvailability::Available),
                        ReplicasAre(SizeIs(1)))));

                scraper->OnChunkBecameUnavailable(chunkIds.back());
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(
                    info,
                    ElementsAre(AllOf(
                        ChunkIdIs(chunkIds.back()),
                        AvailabilityIs(EChunkAvailability::Available),
                        ReplicasAre(SizeIs(1)))));
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(
                    info,
                    ElementsAre(AllOf(
                        ChunkIdIs(chunkIds[1]),
                        AvailabilityIs(EChunkAvailability::Available),
                        ReplicasAre(SizeIs(1)))));

                TestFinishedPromise_.Set();
            }));

    StartScraperWithChunks(scraper, chunkIds);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TChunkScraperBaseTest, WithNontrivialThrottler)
{
    std::atomic<int> batchesCounter = 0;
    auto scraper = MakeScraper(
        BIND([&] (const std::vector<TScrapedChunkInfo>& /*info*/) {
            if (!TestFinishedFuture_.IsSet()) {
                ++batchesCounter;
            }
        }),
        /*batchSize*/ 1,
        MetadataAvailablePolicy,
        /*prioritizeUnavailable*/ true,
        /*throttlerLimit*/ 10);

    StartScraperWithChunks(scraper, CreateChunks());
    Sleep(TDuration::Seconds(3));
    TestFinishedPromise_.Set();
    EXPECT_THAT(batchesCounter.load(), testing::AllOf(Ge(20), Le(40))) << "expected value is 30";
}

TEST_F(TChunkScraperBaseTest, DrainQueueAndAddChunks)
{
    auto chunkIds = CreateChunks();

    auto allChunksRemoved = NewPromise<void>();

    TChunkScraperPtr scraper = MakeScraper(
        CombineEpochHandlers(
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                for (auto chunkId : chunkIds) {
                    scraper->Remove(chunkId);
                }
                allChunksRemoved.Set();
            },
            [&] (const std::vector<TScrapedChunkInfo>& info) {
                EXPECT_THAT(info, SizeIs(Ge(1)));
                for (auto chunkId : chunkIds) {
                    scraper->Remove(chunkId);
                }
                TestFinishedPromise_.Set();
            },
            [] (const std::vector<TScrapedChunkInfo>& info) {
                ADD_FAILURE() << "Scraper should stop when there are no more chunks to locate, but located something: " << PrintToString(info);
            }),
        chunkIds.size());

    StartScraperWithChunks(scraper, chunkIds);

    WaitFor(allChunksRemoved.ToFuture())
        .ThrowOnError();

    ScraperActionQueue_->GetInvoker()->Invoke(BIND([scraper, chunkIds = std::move(chunkIds)] {
        for (auto chunkId : chunkIds) {
            scraper->Add(chunkId);
        }
    }));

    WaitFor(TestFinishedFuture_.WithTimeout(TDuration::Seconds(10)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

struct TWithOneBatchParams
{
    bool BanNodes;
    bool PrioritizeUnavailable;
    int BatchSize;
};

static void PrintTo(const TWithOneBatchParams& params, std::ostream* out)
{
    *out << Format(
        "(BanNodes: %v, PrioritizeUnavailable: %v, BatchSize: %v)",
        params.BanNodes,
        params.PrioritizeUnavailable,
        params.BatchSize);
}

class TWithOneBatchTest
    : public TChunkScraperBaseTest
    , public WithParamInterface<TWithOneBatchParams>
{ };

TEST_P(TWithOneBatchTest, OneBatch)
{
    auto chunkIds = CreateChunks(GetParam().BatchSize);
    auto banGuard = GetParam().BanNodes ? BanAllNodes() : BanDataNodes({});

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int /*epoch*/) {
                EXPECT_THAT(info, ChunkIdsAre(chunkIds));
                EXPECT_THAT(info, Each(AvailabilityIs(GetParam().BanNodes ? EChunkAvailability::Unavailable : EChunkAvailability::Available)));
                EXPECT_THAT(info, Each(ReplicasAre(SizeIs(GetParam().BanNodes ? 0 : 1))));
            },
            /*finishAfter*/ 2),
        GetParam().BatchSize);

    StartScraperWithChunks(scraper, chunkIds);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

INSTANTIATE_TEST_SUITE_P(
    TTestChunkScraper,
    TWithOneBatchTest,
    ConvertGenerator(
        Combine(
            Bool(),
            Bool(),
            Values(1, 3)),
        [] (std::tuple<bool, bool, int> args) {
            auto [banNodes, prioritizeUnavailable, batchSize] = args;
            return TWithOneBatchParams{banNodes, prioritizeUnavailable, batchSize};
        }));

////////////////////////////////////////////////////////////////////////////////

class TWithBannedNodeTest
    : public TChunkScraperBaseTest
{
public:
    TWithBannedNodeTest()
    {
        for (auto chunkId : ChunkIds_) {
            if (GetOrCrash(Replicas_, chunkId) == BannedNode_) {
                UnavailableChunks_.push_back(chunkId);
            } else {
                AvailableChunks_.push_back(chunkId);
            }
        }
    }

    void StartScraper(const TChunkScraperPtr& scraper)
    {
        StartScraperWithChunks(scraper, ChunkIds_);
    }

protected:
    const std::vector<TChunkId> ChunkIds_ = CreateChunks(10);
    const std::string BannedNode_ = GetChunkReplicaNodes(ChunkIds_.front()).front();

    const THashMap<TChunkId, std::string> Replicas_ = TransformRangeTo<THashMap<TChunkId, std::string>>(
        ChunkIds_,
        [] (TChunkId chunkId) {
            auto replicas = GetChunkReplicaNodes(chunkId);
            YT_VERIFY(replicas.size() == 1u);
            return std::pair{chunkId, replicas.front()};
        });

    const decltype(BanDataNode(BannedNode_)) BannedNodeGuard_ = BanDataNode(BannedNode_);

    std::vector<TChunkId> UnavailableChunks_;
    std::vector<TChunkId> AvailableChunks_;
};

TEST_F(TWithBannedNodeTest, UnavailableFirstOneBatch)
{
    auto scraper = MakeScraper(
        CombineEpochHandlers(
            [&] (const std::vector<TScrapedChunkInfo>& firstEpochInfo) {
                EXPECT_THAT(firstEpochInfo, ChunkIdsAre(ChunkIds_));
                for (const auto& chunkInfo : firstEpochInfo) {
                    auto [expectedAvailability, expectedReplicasSize] =
                        Contains(UnavailableChunks_, chunkInfo.ChunkId)
                            ? std::tuple{EChunkAvailability::Unavailable, 0}
                            : std::tuple{EChunkAvailability::Available, 1};
                    EXPECT_THAT(chunkInfo, ReplicasAre(SizeIs(expectedReplicasSize)));
                    EXPECT_THAT(chunkInfo, AvailabilityIs(expectedAvailability));
                }
            },
            [&] (const std::vector<TScrapedChunkInfo>& secondEpochInfo) {
                EXPECT_THAT(secondEpochInfo, ChunkIdsAre(ConcatVectors(UnavailableChunks_, AvailableChunks_)));
                TestFinishedPromise_.Set();
            }),
        ChunkIds_.size());

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TWithBannedNodeTest, OneChunkPerBatch)
{
    ASSERT_THAT(UnavailableChunks_, testing::Contains(ChunkIds_.front()));

    auto scraper = MakeScraper(MakeFinishingEpochAwareHandler(
        [&] (const std::vector<TScrapedChunkInfo>& info, int /*epoch*/) {
            EXPECT_THAT(
                info,
                ElementsAre(AllOf(
                        ChunkIdIs(ChunkIds_.front()),
                        ReplicasAre(IsEmpty()),
                        AvailabilityIs(EChunkAvailability::Unavailable))));
        },
        ChunkIds_.size()));

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

class TOneUnavailableChunkTest
    : public TChunkScraperBaseTest
{
protected:
    const std::vector<std::string> DataNodes_ = GetDataNodes();

    const TChunkId UnavailableChunk_ = [&] {
        auto bannedGuard = BanDataNodes(RemoveAt(DataNodes_, 0));
        return CreateChunks(1).front();
    }();

    const decltype(BanDataNode(std::declval<std::string>())) BannedGuard_ = BanDataNode(DataNodes_.front());
    const std::vector<TChunkId> AvailableChunks_ = CreateChunks();
};

TEST_F(TOneUnavailableChunkTest, AddAvailableFirst)
{
    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(
                    info,
                    ElementsAre(
                        AllOf(ChunkIdIs(UnavailableChunk_), ReplicasAre(IsEmpty()), AvailabilityIs(EChunkAvailability::Unavailable)),
                        AllOf(
                            ChunkIdIs(AvailableChunks_[epoch % AvailableChunks_.size()]),
                            ReplicasAre(SizeIs(1)),
                            AvailabilityIs(EChunkAvailability::Available))));
            },
            AvailableChunks_.size() * 2),
        /*batchSize*/ 2);

    StartScraperWithChunks(scraper, ConcatVectors(std::vector{UnavailableChunk_}, AvailableChunks_));
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TOneUnavailableChunkTest, AddUnavailableLast)
{
    constexpr int BatchSize = 2;
    ASSERT_THAT(AvailableChunks_.size() % BatchSize, 0);
    const int epochsTillUnavailableReached = AvailableChunks_.size() / BatchSize;

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                if (epoch < epochsTillUnavailableReached) {
                    EXPECT_THAT(
                        info,
                        ElementsAre(
                            ChunkIdIs(AvailableChunks_[epoch * BatchSize]),
                            ChunkIdIs(AvailableChunks_[epoch * BatchSize + 1])));
                    EXPECT_THAT(
                        info,
                        Each(testing::AllOf(ReplicasAre(SizeIs(1)), AvailabilityIs(EChunkAvailability::Available))));
                    return;
                }

                EXPECT_THAT(
                    info,
                    ElementsAre(
                        AllOf(ChunkIdIs(UnavailableChunk_), ReplicasAre(IsEmpty()), AvailabilityIs(EChunkAvailability::Unavailable)),
                        AllOf(
                            ChunkIdIs(AvailableChunks_[(epoch - epochsTillUnavailableReached) % AvailableChunks_.size()]),
                            ReplicasAre(SizeIs(1)),
                            AvailabilityIs(EChunkAvailability::Available))));
            },
            AvailableChunks_.size() * 2),
        BatchSize);

    StartScraperWithChunks(scraper, ConcatVectors(AvailableChunks_, std::vector{UnavailableChunk_}));
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

class TThreeChunkGroupsTest
    : public TChunkScraperBaseTest
{
protected:
    static constexpr int GroupSize = 5;

    const std::vector<std::string> DataNodes_ = GetDataNodes();

    std::vector<TChunkId> CreateChunkGroup(int nodeIndex)
    {
        auto banGuard = BanDataNodes(RemoveAt(DataNodes_, nodeIndex));
        return CreateChunks(GroupSize);
    }

    const std::array<std::vector<TChunkId>, 3> ChunkGroups_ = {CreateChunkGroup(0), CreateChunkGroup(1), CreateChunkGroup(2)};

    void StartScraper(const TChunkScraperPtr& scraper)
    {
        StartScraperWithChunks(scraper, RangeTo<std::vector<TChunkId>>(ChunkGroups_ | std::views::join));
    }
};

TEST_F(TThreeChunkGroupsTest, AllAvailable)
{
    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(info, ChunkIdsAre(ChunkGroups_[epoch % ChunkGroups_.size()]));
                EXPECT_THAT(info, Each(testing::AllOf(AvailabilityIs(EChunkAvailability::Available), ReplicasAre(SizeIs(1)))));
            },
            /*finishAfter*/ 6),
        GroupSize);

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TThreeChunkGroupsTest, LargeBatchFirstGroupUnavailable)
{
    auto guard = BanDataNode(DataNodes_[0]);

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(info, ChunkIdsAre(ConcatVectors(ChunkGroups_[0], ChunkGroups_[epoch % 2 + 1])));
            },
            /*finishAfter*/ 4),
        2 * GroupSize);

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TThreeChunkGroupsTest, SmallBatchSecondGroupUnavailable)
{
    auto guard = BanDataNode(DataNodes_[1]);

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(info, ChunkIdsAre(ChunkGroups_[epoch == 0 ? 0 : 1]));
                EXPECT_THAT(
                    info,
                    Each(Conditional(
                        epoch == 0,
                        testing::AllOf(AvailabilityIs(EChunkAvailability::Available), ReplicasAre(SizeIs(1))),
                        testing::AllOf(AvailabilityIs(EChunkAvailability::Unavailable), ReplicasAre(IsEmpty())))));
            },
            /*finishAfter*/ 3),
        GroupSize);

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TThreeChunkGroupsTest, LargeBatchFirstAndThirdGroupsUnavailable)
{
    auto guard = BanDataNodes({DataNodes_[0], DataNodes_[2]});

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(info, ChunkIdsAre(ConcatVectors(ChunkGroups_[0], ChunkGroups_[epoch == 0 ? 1 : 2])));
            },
            /*finishAfter*/ 4),
        2 * GroupSize);

    StartScraper(scraper);
    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

TEST_F(TThreeChunkGroupsTest, SmallBatchFirstAndThirdGroupsUnavailable)
{
    auto guard = BanDataNodes({DataNodes_[0], DataNodes_[2]});

    auto scraper = MakeScraper(
        MakeFinishingEpochAwareHandler(
            [&] (const std::vector<TScrapedChunkInfo>& info, int epoch) {
                EXPECT_THAT(info, ChunkIdsAre(ChunkGroups_[epoch % 2 == 0 ? 0 : 2]));
                EXPECT_THAT(
                    info,
                    Each(testing::AllOf(AvailabilityIs(EChunkAvailability::Unavailable), ReplicasAre(IsEmpty()))));
            },
            /*finishAfter*/ 4
        ),
        GroupSize);

    ScraperActionQueue_->GetInvoker()->Invoke(BIND([&] {
        for (auto chunkId : ChunkGroups_ | std::views::join) {
            scraper->Add(chunkId);
        }
        for (auto chunkId : std::views::join(std::array{ChunkGroups_[0], ChunkGroups_[2]})) {
            scraper->OnChunkBecameUnavailable(chunkId);
        }
        scraper->Start();
    }));

    WaitFor(TestFinishedFuture_)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
