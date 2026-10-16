#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/chunked_modification.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TChunk = std::vector<int>;

TError ConflictError()
{
    return TError(NTabletClient::EErrorCode::TransactionLockConflict, "Row lock conflict");
}

//! A tablet in the middle of a smooth movement — the one failure that is waited out rather than
//! split, since it has nothing to do with the contents of the chunk.
TError MovingTabletError()
{
    return TError(NTabletClient::EErrorCode::TabletServantIsNotActive, "Tablet servant is not active");
}

TError DeadTabletError()
{
    return TError("Tablet cell has no assigned peers");
}

//! Records everything the modifier did: which chunks it tried to commit, which of them it
//! considers landed, and how long it waited between rounds.
class TRecorder
{
public:
    std::vector<TChunk> Attempts;
    std::vector<TChunk> Committed;
    std::vector<TDuration> Delays;

    TChunkCommittedHandler<int> Handler()
    {
        return [this] (const TChunk& chunk) {
            Committed.push_back(chunk);
        };
    }

    TChunkRetryDelayer Delayer()
    {
        return [this] (TDuration backoff) {
            Delays.push_back(backoff);
        };
    }

    //! Wraps |commit| so that every attempted chunk is recorded first.
    TChunkCommitter<int> Committer(std::function<TError(const TChunk&)> commit)
    {
        return [this, commit = std::move(commit)] (const TChunk& chunk) {
            Attempts.push_back(chunk);
            return commit(chunk);
        };
    }

    //! Every item that landed, in commit order — the items must appear exactly once each.
    std::vector<int> CommittedItems() const
    {
        std::vector<int> items;
        for (const auto& chunk : Committed) {
            items.insert(items.end(), chunk.begin(), chunk.end());
        }
        return items;
    }
};

std::vector<TError> Modify(
    TRecorder& recorder,
    const std::vector<int>& items,
    ssize_t itemsPerChunk,
    bool splitOnConflict,
    std::function<TError(const TChunk&)> commit)
{
    return ModifyInChunks<int>(
        "test",
        items,
        itemsPerChunk,
        splitOnConflict,
        recorder.Committer(std::move(commit)),
        recorder.Handler(),
        recorder.Delayer());
}

TError Ok(const TChunk& /*chunk*/)
{
    return {};
}

bool Contains(const TChunk& chunk, int item)
{
    return std::find(chunk.begin(), chunk.end(), item) != chunk.end();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedModificationTest, NothingToModify)
{
    TRecorder recorder;

    auto failures = Modify(recorder, {}, /*itemsPerChunk*/ 10, /*splitOnConflict*/ false, &Ok);

    EXPECT_TRUE(failures.empty());
    EXPECT_TRUE(recorder.Attempts.empty());
    EXPECT_TRUE(recorder.Committed.empty());
}

TEST(TChunkedModificationTest, ItemsAreCutIntoChunksOfTheGivenSize)
{
    TRecorder recorder;

    auto failures = Modify(recorder, {1, 2, 3, 4, 5}, /*itemsPerChunk*/ 2, /*splitOnConflict*/ false, &Ok);

    EXPECT_TRUE(failures.empty());
    // The last chunk carries the remainder; the order of the items is the order they came in.
    EXPECT_EQ(recorder.Attempts, (std::vector<TChunk>{{1, 2}, {3, 4}, {5}}));
    EXPECT_EQ(recorder.Committed, recorder.Attempts);
    EXPECT_TRUE(recorder.Delays.empty());
}

TEST(TChunkedModificationTest, ChunkWiderThanTheInputStaysOne)
{
    TRecorder recorder;

    Modify(recorder, {1, 2, 3}, /*itemsPerChunk*/ 100, /*splitOnConflict*/ false, &Ok);

    EXPECT_EQ(recorder.Attempts, (std::vector<TChunk>{{1, 2, 3}}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedModificationTest, AnErrorThatIsNeitherAConflictNorAMoveIsReportedUntried)
{
    // A tablet that is genuinely down stays down for longer than an iteration, so retrying it
    // here would only burn the rounds the retriable failures need.
    TRecorder recorder;

    auto failures = Modify(recorder, {1, 2, 3, 4}, /*itemsPerChunk*/ 2, /*splitOnConflict*/ true, [] (const TChunk& chunk) {
        return Contains(chunk, 3) ? DeadTabletError() : TError();
    });

    ASSERT_EQ(std::ssize(failures), 1);
    EXPECT_THAT(failures[0].GetMessage(), testing::HasSubstr("no assigned peers"));
    // Each chunk was tried exactly once, and the healthy one still landed.
    EXPECT_EQ(recorder.Attempts, (std::vector<TChunk>{{1, 2}, {3, 4}}));
    EXPECT_EQ(recorder.Committed, (std::vector<TChunk>{{1, 2}}));
}

TEST(TChunkedModificationTest, ConflictIsRetriedWholeWhenSplittingIsNotAllowed)
{
    // A grant conflicts with a worker that has not been shut out yet, so the same conflict can
    // repeat on any subset — splitting would only multiply the transactions.
    TRecorder recorder;
    int conflicts = 2;

    auto failures = Modify(recorder, {1, 2, 3}, /*itemsPerChunk*/ 3, /*splitOnConflict*/ false, [&] (const TChunk& /*chunk*/) {
        return conflicts-- > 0 ? ConflictError() : TError();
    });

    EXPECT_TRUE(failures.empty());
    EXPECT_EQ(recorder.Attempts, (std::vector<TChunk>{{1, 2, 3}, {1, 2, 3}, {1, 2, 3}}));
    EXPECT_EQ(recorder.Committed, (std::vector<TChunk>{{1, 2, 3}}));
    // A conflict resolves by retrying, not by waiting.
    EXPECT_TRUE(recorder.Delays.empty());
}

TEST(TChunkedModificationTest, ConflictHalvesTheChunkDownToTheGuiltyItem)
{
    // The revocation phases can conflict at most once per partition, which is what makes halving
    // converge: here item 7 conflicts three times, so it has to end up alone before it lands.
    TRecorder recorder;
    int conflictsLeft = 3;

    auto failures = Modify(recorder, {1, 2, 3, 4, 5, 6, 7, 8}, /*itemsPerChunk*/ 8, /*splitOnConflict*/ true, [&] (const TChunk& chunk) {
        if (Contains(chunk, 7) && conflictsLeft > 0) {
            --conflictsLeft;
            return ConflictError();
        }
        return TError();
    });

    std::vector<TChunk> expectedAttempts{
        {1, 2, 3, 4, 5, 6, 7, 8},
        {1, 2, 3, 4},
        {5, 6, 7, 8},
        {5, 6},
        {7, 8},
        {7},
        {8},
    };

    EXPECT_TRUE(failures.empty());
    EXPECT_EQ(recorder.Attempts, expectedAttempts);
    // Every item landed, and none of them twice: a chunk that committed is never retried.
    auto committed = recorder.CommittedItems();
    std::sort(committed.begin(), committed.end());
    EXPECT_EQ(committed, (std::vector<int>{1, 2, 3, 4, 5, 6, 7, 8}));
}

TEST(TChunkedModificationTest, AnItemThatKeepsConflictingIsReportedAlone)
{
    TRecorder recorder;

    auto failures = Modify(recorder, {1, 2}, /*itemsPerChunk*/ 2, /*splitOnConflict*/ true, [] (const TChunk& chunk) {
        return Contains(chunk, 2) ? ConflictError() : TError();
    });

    // The healthy item is isolated by the first split and lands; the guilty one exhausts the
    // rounds on its own and is what the caller is told about.
    ASSERT_EQ(std::ssize(failures), 1);
    EXPECT_EQ(recorder.CommittedItems(), (std::vector<int>{1}));
    EXPECT_EQ(recorder.Attempts.back(), (TChunk{2}));
}

TEST(TChunkedModificationTest, RoundsAreBounded)
{
    TRecorder recorder;

    auto failures = Modify(recorder, {1}, /*itemsPerChunk*/ 1, /*splitOnConflict*/ true, [] (const TChunk& /*chunk*/) {
        return ConflictError();
    });

    ASSERT_EQ(std::ssize(failures), 1);
    EXPECT_EQ(std::ssize(recorder.Attempts), MaxChunkedModificationRounds);
    EXPECT_TRUE(recorder.Committed.empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedModificationTest, AMovingTabletIsWaitedOutRatherThanSplit)
{
    // The move rejects the chunk whatever it contains, so splitting it would be pointless; the
    // rounds have to outlast the move instead.
    TRecorder recorder;
    int rejections = 3;

    auto failures = Modify(recorder, {1, 2, 3, 4}, /*itemsPerChunk*/ 4, /*splitOnConflict*/ true, [&] (const TChunk& /*chunk*/) {
        return rejections-- > 0 ? MovingTabletError() : TError();
    });

    EXPECT_TRUE(failures.empty());
    EXPECT_EQ(recorder.Attempts, (std::vector<TChunk>{{1, 2, 3, 4}, {1, 2, 3, 4}, {1, 2, 3, 4}, {1, 2, 3, 4}}));
    // The backoff grows with the round, so a move that outlives a round is not chased.
    EXPECT_EQ(recorder.Delays, (std::vector<TDuration>{
            ChunkedModificationTransientRetryBackoff,
            2 * ChunkedModificationTransientRetryBackoff,
            3 * ChunkedModificationTransientRetryBackoff,
                               }));
}

TEST(TChunkedModificationTest, TheBackoffOfAMovingTabletIsCapped)
{
    TRecorder recorder;

    auto failures = Modify(recorder, {1}, /*itemsPerChunk*/ 1, /*splitOnConflict*/ false, [] (const TChunk& /*chunk*/) {
        return MovingTabletError();
    });

    ASSERT_EQ(std::ssize(failures), 1);
    // One wait per round except the last, which gives up instead of retrying.
    ASSERT_EQ(std::ssize(recorder.Delays), MaxChunkedModificationRounds - 1);
    EXPECT_EQ(recorder.Delays.front(), ChunkedModificationTransientRetryBackoff);
    EXPECT_EQ(recorder.Delays.back(), MaxChunkedModificationTransientRetryBackoff);
    for (auto delay : recorder.Delays) {
        EXPECT_LE(delay, MaxChunkedModificationTransientRetryBackoff);
    }
}

TEST(TChunkedModificationTest, OnlyAMovingTabletMakesTheRoundWait)
{
    // A round that failed on conflicts alone retries immediately; one that also hit a move waits.
    TRecorder recorder;
    int conflicts = 1;

    Modify(recorder, {1, 2}, /*itemsPerChunk*/ 1, /*splitOnConflict*/ false, [&] (const TChunk& chunk) {
        if (Contains(chunk, 1) && conflicts-- > 0) {
            return ConflictError();
        }
        return TError();
    });

    EXPECT_TRUE(recorder.Delays.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
