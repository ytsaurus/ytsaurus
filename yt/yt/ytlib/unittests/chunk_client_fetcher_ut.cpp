#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/fetcher.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/random.h>

namespace NYT::NChunkClient {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("ChunkClientFetcherUnitTest");

////////////////////////////////////////////////////////////////////////////////

TFetcherConfigPtr CreateFetcherConfig(
    int maxChunksPerNodeFetch,
    const std::optional<TDuration>& nodeDirectorySynchronizationTimeout)
{
    auto result = New<TFetcherConfig>();
    result->MaxChunksPerNodeFetch = maxChunksPerNodeFetch;
    if (nodeDirectorySynchronizationTimeout) {
        result->NodeDirectorySynchronizationTimeout = *nodeDirectorySynchronizationTimeout;
    }
    return result;
}

DECLARE_REFCOUNTED_CLASS(TTestFetcher)

DEFINE_ENUM(EChunkResponse,
    (OK)
    (Timeout)
    (Failure)
);

DEFINE_ENUM(ENodeResponse,
    (OK)
    (Failure)
);

struct TNodeResponseScenario
{
    std::vector<ENodeResponse> NodeResponses;
    THashMap<int, std::vector<EChunkResponse>> ChunkResponses;
};

template <class T>
void TryPopBack(std::vector<T>& v)
{
    if (!v.empty()) {
        v.pop_back();
    }
}

using TResponseProfile = THashMap<TNodeId, TNodeResponseScenario>;

class TFakeChunkScraper
    : public IFetcherChunkScraper
{
    TFuture<void> ScrapeChunks(const THashSet<TInputChunkPtr>& /*chunkSpecs*/) override
    {
        return VoidFuture;
    }

    i64 GetUnavailableChunkCount() const override
    {
        return 0;
    }
};

class TTestFetcher
    : public TFetcherBase
{
public:
    TTestFetcher(
        int maxChunksPerNodeFetch,
        TNodeDirectoryPtr nodeDirectory,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger,
        bool createChunkScraper,
        const std::optional<TDuration>& nodeDirectorySynchronizationTimeout)
        : TFetcherBase(
        CreateFetcherConfig(maxChunksPerNodeFetch, nodeDirectorySynchronizationTimeout),
            std::move(nodeDirectory),
            std::move(invoker),
            (createChunkScraper ? New<TFakeChunkScraper>() : nullptr),
            nullptr,
            logger)
    { }

    void ProcessDynamicStore(int /*chunkIndex*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> FetchFromNode(
        TNodeId nodeId,
        std::vector<int> chunkIndexes) override
    {
        auto& scenario = ResponseProfile_[nodeId];

        auto nodeFinally = Finally([&] {
            TryPopBack(scenario.NodeResponses);
        });

        if (!scenario.NodeResponses.empty() && scenario.NodeResponses.back() != ENodeResponse::OK) {
            OnNodeFailed(nodeId, chunkIndexes);
            return VoidFuture;
        }

        for (const auto& chunkIndex : chunkIndexes) {
            auto& chunkResponses = scenario.ChunkResponses[chunkIndex];

            if (!chunkResponses.empty() && chunkResponses.back() != EChunkResponse::OK) {
                if (chunkResponses.back() == EChunkResponse::Failure) {
                    OnChunkFailed(nodeId, chunkIndex, TError("failure"));
                } else if (chunkResponses.back() == EChunkResponse::Timeout) {
                    OnChunkFailed(nodeId, chunkIndex, TError(NYT::EErrorCode::Timeout, "timeout"));
                } else {
                    YT_ABORT();
                }
            } else {
                EXPECT_TRUE(FetchedChunkIndexes_.insert(chunkIndex).second);
            }
            TryPopBack(chunkResponses);
        }

        return VoidFuture;
    }

    void OnFetchingCompleted() override
    {
        EXPECT_EQ(Chunks_.size(), FetchedChunkIndexes_.size());
        FetchingCompleted_ = true;
    }

    void SetResponseProfile(const TResponseProfile& responseProfile)
    {
        ResponseProfile_ = responseProfile;
    }

    void AssertFetchingCompleted()
    {
        ASSERT_TRUE(FetchingCompleted_);
    }

private:
    TResponseProfile ResponseProfile_;
    THashSet<int> FetchedChunkIndexes_;
    bool FetchingCompleted_ = false;
};

DEFINE_REFCOUNTED_TYPE(TTestFetcher)

class TFetcherBaseTest
    : public ::testing::Test
{
protected:
    TActionQueuePtr ControlQueue_;
    IInvokerPtr Invoker_;
    TNodeDirectoryPtr NodeDirectory_;

    int NodeCount_ = 0;
    int ChunkCount_ = 0;
    std::vector<TInputChunkPtr> Chunks_;

    TResponseProfile ResponseProfile_;

    TTestFetcherPtr Fetcher_;

    TFetcherBaseTest()
        : ControlQueue_(New<TActionQueue>())
        , Invoker_(ControlQueue_->GetInvoker())
    { }

    void SetUpFetcher(
        int maxChunksPerNodeFetch,
        int nodeCount,
        int chunkCount,
        std::vector<std::vector<int>> chunkToReplicas,
        bool createChunkScraper = true,
        const std::optional<TDuration>& nodeDirectorySynchronizationTimeout = {})
    {
        NodeCount_ = nodeCount;
        if (!NodeDirectory_) {
            NodeDirectory_ = New<TNodeDirectory>();
            for (int nodeId = 1; nodeId <= NodeCount_; ++nodeId) {
                NodeDirectory_->AddDescriptor(TNodeId(nodeId), TNodeDescriptor{Format("node-%v", nodeId)});
            }
        }

        Fetcher_ = New<TTestFetcher>(maxChunksPerNodeFetch, NodeDirectory_, Invoker_, Logger, createChunkScraper, nodeDirectorySynchronizationTimeout);

        YT_VERIFY(chunkCount == std::ssize(chunkToReplicas));
        ChunkCount_ = chunkCount;
        for (int chunkIndex = 0; chunkIndex < ChunkCount_; ++chunkIndex) {
            Chunks_.push_back(CreateChunk(chunkIndex, chunkToReplicas[chunkIndex]));
            Fetcher_->AddChunk(Chunks_.back());
        }
    }

    void SetNodeResponse(int nodeIndex, const TNodeResponseScenario& nodeResponseScenario)
    {
        YT_VERIFY(1 <= nodeIndex && nodeIndex <= NodeCount_);
        for (const auto& [chunkIndex, value] : nodeResponseScenario.ChunkResponses) {
            YT_VERIFY(0 <= chunkIndex && chunkIndex < ChunkCount_);
        }
        ResponseProfile_[TNodeId(nodeIndex)] = nodeResponseScenario;
    }

    static TInputChunkPtr CreateChunk(int id, const std::vector<int>& replicaNodes)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(TChunkId(id, 0));

        TChunkReplicaWithMediumList replicas;
        for (int index = 0; index < std::ssize(replicaNodes); ++index) {
            replicas.emplace_back(TNodeId(replicaNodes[index]), index, GenericMediumIndex);
        }
        inputChunk->SetReplicaList(replicas);

        return inputChunk;
    }

    TTestFetcherPtr GetFetcher()
    {
        Fetcher_->SetResponseProfile(ResponseProfile_);
        return Fetcher_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFetcherBaseTest, Simple)
{
    SetUpFetcher(/*maxChunksPerNodeFetch*/ 2, /*nodeCount*/ 5, /*chunkCount*/ 6, /*chunkToReplicas*/ {
        {1, 2, 4}, // 0
        {3, 5, 2}, // 1
        {4, 3, 1}, // 2
        {1, 5, 3}, // 3
        {5, 2, 4}, // 4
        {3, 1, 4}, // 5
    });
    SetNodeResponse(1, {.NodeResponses = {ENodeResponse::Failure}});
    SetNodeResponse(3, {.ChunkResponses = {
        {1, {EChunkResponse::Timeout}},
        {3, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});
    SetNodeResponse(4, {.ChunkResponses = {
        {0, {EChunkResponse::Failure}},
        {4, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {5, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});

    WaitFor(GetFetcher()->Fetch())
        .ThrowOnError();
    GetFetcher()->AssertFetchingCompleted();
}

TEST_F(TFetcherBaseTest, AllTimeoutsCauseDeadNode)
{
    SetUpFetcher(/*maxChunksPerNodeFetch*/ 2, /*nodeCount*/ 1, /*chunkCount*/ 2, /*chunkToReplicas*/ {
        {1}, // 0
        {1}, // 1
    }, /*createChunkScraper*/ false);
    SetNodeResponse(1, {.ChunkResponses = {
        {0, {EChunkResponse::OK}},
        {1, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});

    EXPECT_THROW_WITH_ERROR_CODE(
        WaitFor(GetFetcher()->Fetch()).ThrowOnError(),
        NYT::NChunkClient::EErrorCode::ChunkUnavailable);
}

TEST_F(TFetcherBaseTest, StaleNodeDirectory)
{
    auto nodeDirectory = New<TNodeDirectory>();
    // Skip some nodes (all replicas of chunk 3, see below).
    for (int nodeId = 1; nodeId <= NodeCount_; ++nodeId) {
        if (nodeId == 1 || nodeId == 3 || nodeId == 5) {
            continue;
        }

        nodeDirectory->AddDescriptor(TNodeId(nodeId), TNodeDescriptor{Format("node-%v", nodeId)});
    }
    NodeDirectory_ = nodeDirectory;

    SetUpFetcher(/*maxChunksPerNodeFetch*/ 2, /*nodeCount*/ 5, /*chunkCount*/ 6, /*chunkToReplicas*/ {
        {1, 2, 4}, // 0
        {3, 5, 2}, // 1
        {4, 3, 1}, // 2
        {1, 5, 3}, // 3
        {5, 2, 4}, // 4
        {3, 1, 4}, // 5
    });

    SetNodeResponse(1, {.NodeResponses = {ENodeResponse::Failure}});
    SetNodeResponse(3, {.ChunkResponses = {
        {1, {EChunkResponse::Timeout}},
        {3, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});
    SetNodeResponse(4, {.ChunkResponses = {
        {0, {EChunkResponse::Failure}},
        {4, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {5, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});

    TDelayedExecutor::Submit(BIND([this] {
        auto completeNodeDirectory = New<TNodeDirectory>();
        for (int nodeId = 1; nodeId <= NodeCount_; ++nodeId) {
            completeNodeDirectory->AddDescriptor(TNodeId(nodeId), TNodeDescriptor{Format("node-%v", nodeId)});
        }
        NodeDirectory_->MergeFrom(completeNodeDirectory);
    }), TDuration::Seconds(5));

    WaitFor(GetFetcher()->Fetch())
        .ThrowOnError();
    GetFetcher()->AssertFetchingCompleted();
}

TEST_F(TFetcherBaseTest, StaleNodeDirectoryFails)
{
    auto nodeDirectory = New<TNodeDirectory>();
    // Skip some nodes (all replicas of chunk 3, see below).
    for (int nodeId = 1; nodeId <= NodeCount_; ++nodeId) {
        if (nodeId == 1 || nodeId == 3 || nodeId == 5) {
            continue;
        }

        nodeDirectory->AddDescriptor(TNodeId(nodeId), TNodeDescriptor{Format("node-%v", nodeId)});
    }
    NodeDirectory_ = nodeDirectory;

    SetUpFetcher(/*maxChunksPerNodeFetch*/ 2, /*nodeCount*/ 5, /*chunkCount*/ 6, /*chunkToReplicas*/ {
        {1, 2, 4}, // 0
        {3, 5, 2}, // 1
        {4, 3, 1}, // 2
        {1, 5, 3}, // 3
        {5, 2, 4}, // 4
        {3, 1, 4}, // 5
    }, /*createChunkScraper*/ true, /*nodeDirectorySynchronizationTimeout*/ TDuration::Seconds(5));

    EXPECT_THROW_WITH_ERROR_CODE(WaitFor(GetFetcher()->Fetch()).ThrowOnError(), NYT::EErrorCode::Timeout);
}

TEST_F(TFetcherBaseTest, ASingleNodeSavesTheDay)
{
    SetUpFetcher(/*maxChunksPerNodeFetch*/ 2, /*nodeCount*/ 3, /*chunkCount*/ 6, /*chunkToReplicas*/ {
        {3, 2, 1}, // 0
        {1, 3},    // 1
        {2, 3, 1}, // 2
        {3, 1, 2}, // 3
        {1, 2, 3}, // 4
        {2, 1},    // 5
    });
    SetNodeResponse(1, {.ChunkResponses = {
        {0, {EChunkResponse::Timeout}},
        {1, {EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {2, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {3, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {4, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
        {5, {EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout, EChunkResponse::Timeout}},
    }});
    SetNodeResponse(2, {.ChunkResponses = {
        {0, {EChunkResponse::Failure}},
        {1, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {2, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {3, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {4, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {5, {EChunkResponse::Timeout, EChunkResponse::Failure}},
    }});
    SetNodeResponse(3, {.ChunkResponses = {
        {0, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {1, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {2, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {3, {EChunkResponse::Failure}},
        {4, {EChunkResponse::Timeout, EChunkResponse::Failure}},
        {5, {EChunkResponse::Timeout, EChunkResponse::Failure}},
    }});

    WaitFor(GetFetcher()->Fetch())
        .ThrowOnError();
    GetFetcher()->AssertFetchingCompleted();
}

ui64 GenRandom(ui64 upperBound) {
    static TRandomGenerator generator;
    return generator.Generate<ui64>() % upperBound;
}

std::vector<int> Get3RandomNumbers(int max)
{
    YT_VERIFY(max >= 3);
    auto rand1 = static_cast<int>(GenRandom(max));
    auto rand2 = static_cast<int>(GenRandom(max - 1));
    auto rand3 = static_cast<int>(GenRandom(max - 2));
    if (rand2 >= rand1) {
        ++rand2;
    }
    if (rand3 >= std::min(rand1, rand2)) {
        ++rand3;
    }
    if (rand3 >= std::max(rand1, rand2)) {
        ++rand3;
    }
    // Check for bugs in the code above :)
    YT_VERIFY(rand1 != rand2 && rand1 != rand3 && rand2 != rand3);

    return {rand1, rand2, rand3};
}

TNodeResponseScenario GenerateRandomNodeResponseScenario(
    int chunkCount,
    int nodeFailureRatioDenominator,
    int chunkResponseRatioDenominator)
{
    TNodeResponseScenario result;
    bool doNodeFailures = GenRandom(nodeFailureRatioDenominator) == 0;
    for (int responseIndex = 0; responseIndex < 3; ++responseIndex) {
        result.NodeResponses.push_back(static_cast<ENodeResponse>(GenRandom(1 + doNodeFailures)));
    }
    for (int chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex) {
        if (GenRandom(chunkResponseRatioDenominator) > 0) {
            continue;
        }
        std::vector<EChunkResponse> chunkResponses;
        for (int responseIndex = 0; responseIndex < 5; ++responseIndex) {
            chunkResponses.push_back(static_cast<EChunkResponse>(GenRandom(3)));
        }
        result.ChunkResponses[chunkIndex] = chunkResponses;
    }
    return result;
}

TEST_F(TFetcherBaseTest, Random)
{
    TRandomGenerator gen(1543);

    auto nodeCount = 1000;
    auto chunkCount = 250000;

    std::vector<std::vector<int>> chunkToReplicas(chunkCount);
    for (int chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex) {
        auto replicas = Get3RandomNumbers(nodeCount);
        for (auto& replica : replicas) {
            // Node ids must start from 1 :(
            ++replica;
        }
        chunkToReplicas[chunkIndex] = replicas;
    }

    SetUpFetcher(/*maxChunksPerNodeFetch*/ 30, nodeCount, chunkCount, chunkToReplicas);
    for (int nodeId = 1; nodeId <= nodeCount; ++nodeId) {
        SetNodeResponse(nodeId, GenerateRandomNodeResponseScenario(
            chunkCount,
            /*nodeFailureRatioDenominator*/ 7,
            /*chunkResponseRatioDenominator*/ 107));
    }

    WaitFor(GetFetcher()->Fetch())
        .ThrowOnError();
    GetFetcher()->AssertFetchingCompleted();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
