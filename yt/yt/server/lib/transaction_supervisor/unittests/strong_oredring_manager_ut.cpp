#include <yt/yt/server/lib/transaction_supervisor/strong_ordering_manager.h>

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/core/profiling/timing.h>
#include <yt/yt/core/test_framework/framework.h>

#include <util/random/random.h>

namespace NYT::NTransactionSupervisor {
namespace {

using namespace NApi;
using namespace NHydra;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

NLogging::TLogger Logger("Test");

std::vector<std::string> DefaultTags = {"tag"};

TMutationContext MutationContextMock{TTestingTag{}};

TClusterTag SelfClockClusterTagMock = NObjectClient::TCellTag(10);

////////////////////////////////////////////////////////////////////////////////

class TStrongOrderingManagerTestBase
    : public ::testing::Test
{
public:
    struct TTestTransaction
    {
        // Data for strong ordering manager.
        TTransactionId Id;
        bool IsCoordinated;
        TTimestamp PrepareTimestamp;
        TTimestamp CommitTimestamp;
        std::vector<std::string> Tags;

        // Data to setup test environment.
        TTimestamp DelayedPrepareTimestamp = NullTimestamp;
        //NB: If nullopt - do not simulate ReadyToCommit call.
        std::optional<TTimestamp> DelayedReadyToCommitTimestamp = std::nullopt;
        TTimestamp DelayedCommitTimestamp = NullTimestamp;
        bool ShouldAbortInsteadOfCommit = false;
    };

    void SetUp() override
    {
        SetCurrentMutationContext(&MutationContextMock);
    }

    void TearDown() override
    {
        SetCurrentMutationContext(nullptr);
        Manager_.Clear();
    }

    void Prepare(TTestTransaction transaction)
    {
        YT_LOG_DEBUG("Calling Prepare (TransactionId: %v, PrepareTimestamp: %v, IsCoordinated: %v)",
            transaction.Id,
            transaction.PrepareTimestamp,
            transaction.IsCoordinated);

        Manager_.OnCommitPrepare(transaction.Id, transaction.PrepareTimestamp, transaction.IsCoordinated, transaction.Tags);
    };

    [[nodiscard]] std::vector<TCommitInfo> ReadyToCommit(TTestTransaction transaction)
    {
        YT_LOG_DEBUG("Calling ReadyToCommit (TransactionId: %v, CommitTimestamp: %v, IsCoordinated: %v)",
            transaction.Id,
            transaction.CommitTimestamp,
            transaction.IsCoordinated);

        return Manager_.OnCommitReadyToCommit(transaction.Id, transaction.CommitTimestamp, SelfClockClusterTagMock);
    }

    [[nodiscard]] std::vector<TCommitInfo> Commit(TTestTransaction transaction)
    {
        YT_LOG_DEBUG("Calling Commit (TransactionId: %v, CommitTimestamp: %v, IsCoordinated: %v)",
            transaction.Id,
            transaction.CommitTimestamp,
            transaction.IsCoordinated);

        return Manager_.OnCommitCommit(
            transaction.Id,
            transaction.CommitTimestamp,
            SelfClockClusterTagMock,
            transaction.IsCoordinated,
            transaction.IsCoordinated ? ECommitState::ReadyToCommit : ECommitState::Commit);
    };

    [[nodiscard]] std::vector<TCommitInfo> Abort(TTestTransaction transaction)
    {
        YT_LOG_DEBUG("Calling Abort (TransactionId: %v, CommitTimestamp: %v, IsCoordinated: %v)",
            transaction.Id,
            transaction.CommitTimestamp,
            transaction.IsCoordinated);

        return Manager_.OnCommitAbort(transaction.Id, transaction.CommitTimestamp);
    };

protected:
    TTransactionId GenerateTransactionId()
    {
        return TGuid::FromString(Format("1-2-a0002-%x", TransactionsCount_++));
    }

    TStrongOrderingManager Manager_{Logger};
    int TransactionsCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TStrongOrderingManagerTestSingleTransaction
    : public TStrongOrderingManagerTestBase
    , public ::testing::WithParamInterface<std::tuple<
        /*isCoordinated*/ bool,
        /*runReadyToCommit*/ bool
    >>
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_P(TStrongOrderingManagerTestSingleTransaction, Commit)
{
    SetCurrentMutationContext(&MutationContextMock);

    auto [isCoordinated, runReadyToCommit] = GetParam();
    TTestTransaction transaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = isCoordinated,
        .PrepareTimestamp = 1,
        .CommitTimestamp = 5,
        .Tags = DefaultTags,
    };

    Prepare(transaction);
    if (!isCoordinated && runReadyToCommit) {
        auto transactionsToCommit = ReadyToCommit(transaction);
        EXPECT_TRUE(transactionsToCommit.empty());
    }

    auto transactionsToCommit = Commit(transaction);
    ASSERT_EQ(std::ssize(transactionsToCommit), 1);
    EXPECT_EQ(transactionsToCommit[0].TransactionId, transaction.Id);
}

TEST_P(TStrongOrderingManagerTestSingleTransaction, Abort)
{
    SetCurrentMutationContext(&MutationContextMock);

    auto [isCoordinated, runReadyToCommit] = GetParam();
    TTestTransaction transaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = isCoordinated,
        .PrepareTimestamp = 1,
        .CommitTimestamp = 5,
        .Tags = DefaultTags,
    };

    Prepare(transaction);
    if (!isCoordinated && runReadyToCommit) {
        auto transactionsToCommit = ReadyToCommit(transaction);
        EXPECT_TRUE(transactionsToCommit.empty());
    }

    auto transactionsToCommit = Abort(transaction);
    EXPECT_TRUE(transactionsToCommit.empty());
}

INSTANTIATE_TEST_SUITE_P(
    TStrongOrderingManagerTestSingleTransaction,
    TStrongOrderingManagerTestSingleTransaction,
    ::testing::Values(
        std::tuple<bool, bool>(/*isCoordinated*/ true, /*runReadyToCommit*/ false),
        std::tuple<bool, bool>(/*isCoordinated*/ false, /*runReadyToCommit*/ false),
        std::tuple<bool, bool>(/*isCoordinated*/ false, /*runReadyToCommit*/ true)
    ));

////////////////////////////////////////////////////////////////////////////////

class TStrongOrderingManagerTest
    : public TStrongOrderingManagerTestBase
{};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStrongOrderingManagerTest, TwoOverlappingTransactions)
{
    SetCurrentMutationContext(&MutationContextMock);

    TTestTransaction firstTransaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = true, // This should not matter.
        .PrepareTimestamp = 3,
        .CommitTimestamp = 6,
        .Tags = DefaultTags,
    };

    TTestTransaction secondTransaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = true, // This should not matter.
        .PrepareTimestamp = 2,
        .CommitTimestamp = 10,
        .Tags = DefaultTags,
    };

    Prepare(firstTransaction);
    Prepare(secondTransaction);

    auto transactionsToCommit = Commit(firstTransaction);
    EXPECT_TRUE(transactionsToCommit.empty());

    transactionsToCommit = Commit(secondTransaction);
    ASSERT_EQ(std::ssize(transactionsToCommit), 2);
    EXPECT_EQ(transactionsToCommit[0].TransactionId, firstTransaction.Id);
    EXPECT_EQ(transactionsToCommit[1].TransactionId, secondTransaction.Id);
}

TEST_F(TStrongOrderingManagerTest, CommitOrderOptimized)
{
    TTestTransaction firstTransaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = false, // It's important for the test setup.
        .PrepareTimestamp = 3,
        .CommitTimestamp = 6,
        .Tags = DefaultTags,
    };

    TTestTransaction secondTransaction{
        .Id = GenerateTransactionId(),
        .IsCoordinated = false, // This should not matter.
        .PrepareTimestamp = 2,
        .CommitTimestamp = 10,
        .Tags = DefaultTags,
    };

    Prepare(firstTransaction);

    auto transactionsToCommit = ReadyToCommit(firstTransaction);
    EXPECT_TRUE(transactionsToCommit.empty());

    Prepare(secondTransaction);

    // Since commit timestamp of the first transaction was already known seen
    // before registering the second transactions' prepare timestamp, the second
    // transactions' commit timestamp cannot be less than commit timestamp of the
    // first transaction. Therefore the first transaction can be committed.
    transactionsToCommit = Commit(firstTransaction);
    ASSERT_EQ(std::ssize(transactionsToCommit), 1);
    EXPECT_EQ(transactionsToCommit[0].TransactionId, firstTransaction.Id);

    transactionsToCommit = Commit(secondTransaction);
    ASSERT_EQ(std::ssize(transactionsToCommit), 1);
    EXPECT_EQ(transactionsToCommit[0].TransactionId, secondTransaction.Id);
}

////////////////////////////////////////////////////////////////////////////////

class TStrongOrderingManagerStressTest
    : public TStrongOrderingManagerTestBase
{
public:
    using TBase = TStrongOrderingManagerTestBase;

    void SetUp() override
    {
        TBase::SetUp();

        auto seed = static_cast<int>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
        auto* testName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
        Cerr << Format("Running test %v with seed %v", testName, seed) << Endl;

        Rng_ = std::mt19937(seed);
    }

protected:
    std::mt19937 Rng_;

    struct TStressTestConfig
    {
        int TransactionCount = 1000;

        int MinTagsPerTransaction = 2;
        int MaxTagsPerTransaction = 3;
        int TagCount = 5;

        // An average amount of simultaneously intersecting transactions is AverageCommitDuration / AveragePrepareFrequency.
        int AveragePrepareFrequency = 20;
        int AverageCommitDuration = 500;

        double CoordinationProbability = 0.5;

        // Probability that ReadyToCommit is received for non-coordinated transaction.
        double ReadyToCommitProbability = 0.7;

        double AbortProbability = 0.2;

        // Delay is added to timestamps to simulate network interactions.
        // This is only applicable to non-coordinated transactions.
        TTimestamp MaxPrepareDelay = 100;
        TTimestamp MaxCommitDelay = 100;

        bool EnableDebugLogging = false;

        std::string Print() const
        {
            return Format(
                "TransactionCount: %v, MinTagsPerTransaction: %v, MaxTagsPerTransaction: %v, TagCount: %v, "
                "AveragePrepareFrequency: %v, AverageCommitDuration: %v, CoordinationProbability: %v, "
                "ReadyToCommitProbability: %v, AbortProbability: %v, MaxPrepareDelay: %v, MaxCommitDelay: %v, "
                "EnableDebugLogging: %v",
                TransactionCount,
                MinTagsPerTransaction,
                MaxTagsPerTransaction,
                TagCount,
                AveragePrepareFrequency,
                AverageCommitDuration,
                CoordinationProbability,
                ReadyToCommitProbability,
                AbortProbability,
                MaxPrepareDelay,
                MaxCommitDelay,
                EnableDebugLogging);
        }
    };

    void ValidateConfig(const TStressTestConfig& config)
    {
        ASSERT_LE(config.MinTagsPerTransaction, config.MaxTagsPerTransaction)
            << "Inconsistent tags per transaction settings";
        ASSERT_LE(config.MaxTagsPerTransaction, config.TagCount)
            << "Inconsistent tags per transaction settings";

        ASSERT_GT(config.TransactionCount, 0)
            << "Transaction count should be greater than 0";

        ASSERT_LE(0, config.ReadyToCommitProbability)
            << "ReadyToCommit probability should be greater than or equal to 0";
        ASSERT_LE(config.ReadyToCommitProbability, 1)
            << "ReadyToCommit probability should be less than or equal to 1";

        ASSERT_LE(0, config.AbortProbability)
            << "Abort probability should be greater than or equal to 0";
        ASSERT_LE(config.AbortProbability, 1)
            << "Abort probability should be less than or equal to 1";
    }

    std::vector<std::string> GenerateTagPool(int tagCount)
    {
        std::vector<std::string> tags;
        tags.reserve(tagCount);
        for (int i = 0; i < tagCount; ++i) {
            tags.push_back("tag_" + std::to_string(i));
        }
        return tags;
    }

    std::vector<std::string> AssignRandomTags(
        const std::vector<std::string>& tagPool,
        int minTagsPerTransaction,
        int maxTagsPerTransaction)
    {
        std::uniform_int_distribution<int> tagCountDistribution(minTagsPerTransaction, maxTagsPerTransaction);
        int tagCount = tagCountDistribution(Rng_);

        std::vector<std::string> shuffled = tagPool;
        std::shuffle(shuffled.begin(), shuffled.end(), Rng_);
        shuffled.resize(tagCount);

        return shuffled;
    }

    std::vector<TTestTransaction> GenerateTransactions(
        const TStressTestConfig& config)
    {
        std::vector<TTestTransaction> transactions(config.TransactionCount);

        // Generate ID, IsCoordinated value and strong ordering tags.
        std::bernoulli_distribution coordinatorDistribution(config.CoordinationProbability);
        std::vector<std::string> tagPool = GenerateTagPool(config.TagCount);
        for (int i = 0; i < config.TransactionCount; ++i) {
            auto& transaction = transactions[i];
            transaction.Id = GenerateTransactionId();
            transaction.IsCoordinated = coordinatorDistribution(Rng_);
            transaction.Tags = AssignRandomTags(tagPool, config.MinTagsPerTransaction, config.MaxTagsPerTransaction);
        }

        // Generate prepare timestamps and delays.
        auto maxPrepareTimestamp = config.AveragePrepareFrequency * config.TransactionCount;
        std::uniform_int_distribution<TTimestamp> timestampDistribution(0, maxPrepareTimestamp);
        std::uniform_int_distribution<TTimestamp> prepareDelayDistribution(1, config.MaxPrepareDelay);
        for (auto& transaction : transactions) {
            transaction.PrepareTimestamp = timestampDistribution(Rng_);

            // Prepare delays are generated early, because any other action has to be observed
            // strictly after a tranasction was prepared.
            if (transaction.IsCoordinated) {
                // Transactions coordinated by this cell have no delay.
                transaction.DelayedPrepareTimestamp = transaction.PrepareTimestamp;
            } else {
                transaction.DelayedPrepareTimestamp = transaction.PrepareTimestamp + prepareDelayDistribution(Rng_);
            }
        }

        // Generate commit timestamps.
        std::unordered_set<TTimestamp> usedCommitTimestamps;
        auto maxCommitTimestampOffset = config.AverageCommitDuration * 2;
        for (auto& transaction : transactions) {
            std::uniform_int_distribution<TTimestamp> commitOffsetDistribution(1, maxCommitTimestampOffset);

            int collisionCount = 1;
            auto candidate = transaction.DelayedPrepareTimestamp + commitOffsetDistribution(Rng_);
            while (usedCommitTimestamps.contains(candidate)) {
                if (collisionCount % 100 == 0) {
                    commitOffsetDistribution = std::uniform_int_distribution<TTimestamp>(
                        maxCommitTimestampOffset,
                        maxCommitTimestampOffset * 2 * (collisionCount / 100));
                }

                candidate = transaction.DelayedPrepareTimestamp + commitOffsetDistribution(Rng_);
                ++collisionCount;
            }

            usedCommitTimestamps.insert(candidate);
            transaction.CommitTimestamp = candidate;
        }

        // Sanity check.
        for (const auto& transaction : transactions) {
            YT_ASSERT(transaction.PrepareTimestamp < transaction.CommitTimestamp);
            YT_ASSERT(transaction.DelayedPrepareTimestamp < transaction.CommitTimestamp);
        }

        // Now generate everything else.
        std::bernoulli_distribution abortDistribution(config.AbortProbability);
        std::bernoulli_distribution readyToCommitDistribution(config.ReadyToCommitProbability);
        std::uniform_int_distribution<TTimestamp> commitDelayDistribution(1, config.MaxCommitDelay);

        for (auto& transaction : transactions) {
            transaction.ShouldAbortInsteadOfCommit = abortDistribution(Rng_);

            // Transactions coordinated by this cell have no delay and do not have explicit
            // ReadyToCommit stage.
            if (transaction.IsCoordinated) {
                transaction.DelayedReadyToCommitTimestamp = std::nullopt;
                transaction.DelayedCommitTimestamp = transaction.CommitTimestamp;
                continue;
            }

            auto firstTimestamp = transaction.CommitTimestamp + commitDelayDistribution(Rng_);
            auto secondTimestamp = transaction.CommitTimestamp + commitDelayDistribution(Rng_);

            // Avoid messy "same timestamp" situations.
            while (firstTimestamp == secondTimestamp) {
                // Adding 1 in case MaxCommitDelay is 0, to avoid an infinite loop.
                secondTimestamp = transaction.CommitTimestamp + commitDelayDistribution(Rng_) + 1;
            }

            if (!readyToCommitDistribution(Rng_)) {
                transaction.DelayedReadyToCommitTimestamp = std::nullopt;
                transaction.DelayedCommitTimestamp = firstTimestamp;
            } else {
                transaction.DelayedReadyToCommitTimestamp = std::min(firstTimestamp, secondTimestamp);
                transaction.DelayedCommitTimestamp = std::max(firstTimestamp, secondTimestamp);
            }
        }

        return transactions;
    }

    // Handy when debugging.
    void LogTransactionInfo(const std::vector<TTestTransaction>& transactions)
    {
        for (const auto& transaction : transactions) {
            YT_LOG_DEBUG(
                "Transaction information (Id: %v, IsCoordinated: %v, PrepareTimestamp: %v "
                "CommitTimestamp: %v, Tags: %v, DelayedPrepareTimestamp: %v, "
                "DelayedReadyToCommitTimestamp: %v, DelayedCommitTimestamp: %v, ShouldAbortInsteadOfCommit: %v)",
                transaction.Id,
                transaction.IsCoordinated,
                transaction.PrepareTimestamp,
                transaction.CommitTimestamp,
                transaction.Tags,
                transaction.DelayedPrepareTimestamp,
                transaction.DelayedReadyToCommitTimestamp,
                transaction.DelayedCommitTimestamp,
                transaction.ShouldAbortInsteadOfCommit);
        }
    }

    struct TAction
    {
        // Only really needed to sort actions.
        TTimestamp TriggerTimestamp;

        // Index into the transaction list.
        int TransactionIndex;

        // DEFINE_ENUM doesn't work inside class definitions.
        enum class EActionType
        {
            Prepare = 0,
            ReadyToCommit = 1,
            Commit = 2,
            Abort = 3,
        };
        EActionType ActionType;

        bool operator<(const TAction& other) const
        {
            return TriggerTimestamp < other.TriggerTimestamp;
        }
    };

    std::vector<TAction> GenerateAcions(const std::vector<TTestTransaction>& transactions)
    {
        std::vector<TAction> actions;
        actions.reserve(transactions.size() * 3);
        for (int i = 0; i < std::ssize(transactions); ++i) {
            const auto& transaction = transactions[i];

            TAction prepareAction{
                .TriggerTimestamp = transaction.DelayedPrepareTimestamp,
                .TransactionIndex = i,
                .ActionType = TAction::EActionType::Prepare,
            };
            actions.push_back(std::move(prepareAction));

            if (!transaction.IsCoordinated && transaction.DelayedReadyToCommitTimestamp) {
                TAction ReadyToCommitAction{
                    .TriggerTimestamp = *transaction.DelayedReadyToCommitTimestamp,
                    .TransactionIndex = i,
                    .ActionType = TAction::EActionType::ReadyToCommit,
                };
                actions.push_back(std::move(ReadyToCommitAction));
            }

            TAction FinalAction{
                .TriggerTimestamp = transaction.DelayedCommitTimestamp,
                .TransactionIndex = i,
                .ActionType = transaction.ShouldAbortInsteadOfCommit
                    ? TAction::EActionType::Abort
                    : TAction::EActionType::Commit,
            };
            actions.push_back(std::move(FinalAction));
        }

        // This should ensure that actions are ordered by TriggerTimestamp,
        // but actions sharing Trigger timestamp are randomly ordered.
        std::sort(actions.begin(), actions.end());

        return actions;
    }

    using TPerTagCommitHistory = THashMap<std::string, std::vector<TCommitInfo>>;

    TPerTagCommitHistory Simulate(
        const std::vector<TAction>& actions,
        const std::vector<TTestTransaction>& transactions)
    {
        THashMap<TTransactionId, std::vector<std::string>> transactionIdToTags;
        for (const auto& transaction : transactions) {
            transactionIdToTags[transaction.Id] = transaction.Tags;
        }

        TPerTagCommitHistory history;

        auto saveCommitInfo = [&](const std::vector<TCommitInfo>& commits) {
            for (const auto& commit : commits) {
                const auto& tags = GetOrCrash(transactionIdToTags, commit.TransactionId);
                for (const auto& tag : tags) {
                    history[tag].push_back(commit);
                }
            }
        };

        for (const auto& action : actions) {
            const auto& transaction = transactions[action.TransactionIndex];

            switch (action.ActionType) {
                case TAction::EActionType::Prepare:
                    Prepare(transaction);
                    break;

                case TAction::EActionType::ReadyToCommit:
                    saveCommitInfo(ReadyToCommit(transaction));
                    break;

                case TAction::EActionType::Commit:
                    saveCommitInfo(Commit(transaction));
                    break;

                case TAction::EActionType::Abort:
                    saveCommitInfo(Abort(transaction));
                    break;
            }
        }

        return history;
    }

    void ValidateStrongOrdering(
        const TPerTagCommitHistory& commitHistory,
        const std::vector<TTestTransaction>& transactions)
    {
        THashMap<TTransactionId, TTimestamp> transactionToCommitTimestamp;
        for (const auto& transaction : transactions) {
            transactionToCommitTimestamp[transaction.Id] = transaction.CommitTimestamp;
        }

        for (const auto& [tag, commits] : commitHistory) {
            TTransactionId previousTransactionId = NullTransactionId;
            TTimestamp previousCommitTimestamp = NullTimestamp;
            for (const auto& commit : commits) {
                auto it = transactionToCommitTimestamp.find(commit.TransactionId);
                ASSERT_NE(it, transactionToCommitTimestamp.end())
                    << Format("Unknown transaction in commit history (TransactionId: %v, Tag: %v)",
                        commit.TransactionId,
                        tag);

                auto currentCommitTimestamp = it->second;
                EXPECT_GT(currentCommitTimestamp, previousCommitTimestamp)
                    << Format("Strong ordering violated (Tag: %v, PreviousCommitTimestamp: %v, "
                        "PreviousTransactionId: %v, CurrentCommitTimestamp: %v, OffendingTransactionId: %v)",
                        tag,
                        previousCommitTimestamp,
                        previousTransactionId,
                        currentCommitTimestamp,
                        it->first);

                previousTransactionId = it->first;
                previousCommitTimestamp = currentCommitTimestamp;
            }
        }
    }

    // Every committed transaction must appear in the history
    // for each of its tags exactly once.
    void ValidateAllCommittedTransactionsReported(
        const TPerTagCommitHistory& commitHistory,
        const std::vector<TTestTransaction>& transactions)
    {
        THashMap<TTransactionId, THashSet<std::string>> committedTransactionIdToTags;
        for (const auto& transaction : transactions) {
            if (transaction.ShouldAbortInsteadOfCommit) {
                continue;
            }

            committedTransactionIdToTags[transaction.Id].insert(transaction.Tags.begin(), transaction.Tags.end());
        }

        for (const auto& [tag, commits] : commitHistory) {
            for (const auto& commit : commits) {
                auto it = committedTransactionIdToTags.find(commit.TransactionId);
                ASSERT_NE(it, committedTransactionIdToTags.end())
                    << Format("Commit mentions a transaction, that should not be committed "
                        "(TransactionId: %v, CommitTimestamp: %v, Tag: %v)",
                        commit.TransactionId,
                        commit.CommitTimestamp,
                        tag);

                ASSERT_EQ(it->second.erase(tag), 1ul)
                    << Format("Transaction was either mentioned as being committed more than once, "
                        "or the simulator reported wrong tags for it (TransactionId: %v, Tag: %v)",
                        commit.TransactionId,
                        tag);
            }
        }

        for (const auto& [transactionId, tags] : committedTransactionIdToTags) {
            ASSERT_TRUE(tags.empty())
                << Format("Transaction was not reported as committed in some of its tags (TransactionId: %v, UnreportedTags: %v)",
                    transactionId,
                    tags);
        }
    }

    // This is an overkill probably. But I am too sleepy to think about it.
    void ValidateNoAbortedTransactionsInCommitInfos(
        const TPerTagCommitHistory& commitHistory,
        const std::vector<TTestTransaction>& transactions)
    {
        THashSet<TTransactionId> abortedTransactionIds;
        for (const auto& transaction : transactions) {
            if (transaction.ShouldAbortInsteadOfCommit) {
                abortedTransactionIds.insert(transaction.Id);
            }
        }

        for (const auto& [tag, commits] : commitHistory) {
            for (const auto& commit : commits) {
                EXPECT_FALSE(abortedTransactionIds.contains(commit.TransactionId))
                    << Format("Transaction was seen as committed instead of being aborted in some shard "
                        "(TransactionId: %v, ShardTag: %v)",
                        commit.TransactionId,
                        tag);
            }
        }
    }

    void RunSimulation(const TStressTestConfig& config)
    {
        ValidateConfig(config);

        NProfiling::TWallTimer timer;
        auto transactions = GenerateTransactions(config);
        if (config.EnableDebugLogging) {
            LogTransactionInfo(transactions);
        }
        auto timeAfterTransactionGeneration = timer.GetElapsedTime().MilliSeconds();

        auto actions = GenerateAcions(transactions);
        auto timeAfterActionGeneration = timer.GetElapsedTime().MilliSeconds();

        auto commitHistroy = Simulate(actions, transactions);
        auto timeAfterSimulation = timer.GetElapsedTime().MilliSeconds();

        ValidateStrongOrdering(commitHistroy, transactions);
        ValidateAllCommittedTransactionsReported(commitHistroy, transactions);
        ValidateNoAbortedTransactionsInCommitInfos(commitHistroy, transactions);
        auto timeAfterValidation = timer.GetElapsedTime().MilliSeconds();

        if (config.EnableDebugLogging) {
            Cerr << Format(
                "Test time breakdown:\n"
                "Total time - %vms\n"
                "Transaction generation took %vms\n"
                "Action generation took %vms\n"
                "Simulation took %vms\n"
                "Validation took %vms\n",
                timeAfterValidation,
                timeAfterTransactionGeneration,
                timeAfterActionGeneration - timeAfterTransactionGeneration,
                timeAfterSimulation - timeAfterActionGeneration,
                timeAfterValidation - timeAfterSimulation) << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TStrongOrderingManagerStressTest, Small)
{
    TStressTestConfig config;
    config.TransactionCount = 100;
    RunSimulation(config);
}

TEST_F(TStrongOrderingManagerStressTest, SingleSharedTag)
{
    TStressTestConfig config;
    config.TagCount = 1;
    config.MinTagsPerTransaction = 1;
    config.MaxTagsPerTransaction = 1;

    RunSimulation(config);
}

TEST_F(TStrongOrderingManagerStressTest, ManyTags)
{
    TStressTestConfig config;
    config.TransactionCount = 10'000;

    config.MinTagsPerTransaction = 1;
    config.MaxTagsPerTransaction = 100;
    config.TagCount = 100;

    RunSimulation(config);
}

TEST_F(TStrongOrderingManagerStressTest, LongTransactions)
{
    TStressTestConfig config;
    config.AverageCommitDuration = 5000;

    RunSimulation(config);
}

TEST_F(TStrongOrderingManagerStressTest, Large)
{
    TStressTestConfig config;
    config.TransactionCount = 1'000'000;
    config.AverageCommitDuration = 1000;

    config.MinTagsPerTransaction = 3;
    config.MaxTagsPerTransaction = 5;
    config.TagCount = 10;

    RunSimulation(config);
}

// Jesus take the wheel!
TEST_F(TStrongOrderingManagerStressTest, RandomEverything)
{
    TStressTestConfig config;
    std::normal_distribution<double> transactionCountDistribution(100'000, 25'000);
    config.TransactionCount = static_cast<int>(transactionCountDistribution(Rng_));

    std::uniform_int_distribution<int> tagCountDirstribution(1, 100);
    config.TagCount = tagCountDirstribution(Rng_);

    std::uniform_int_distribution<int> minTagsPerTransactionDirstribution(1, config.TagCount);
    config.MinTagsPerTransaction = minTagsPerTransactionDirstribution(Rng_);

    std::uniform_int_distribution<int> maxTagsPerTransactionDirstribution(config.MinTagsPerTransaction, config.TagCount);
    config.MaxTagsPerTransaction = maxTagsPerTransactionDirstribution(Rng_);

    std::uniform_int_distribution<int> averagePrepareFrequencyDirstribution(0, 200);
    config.AveragePrepareFrequency = averagePrepareFrequencyDirstribution(Rng_);

    std::uniform_int_distribution<int> averageCommitDurationDirstribution(1, 5000);
    config.AverageCommitDuration = averageCommitDurationDirstribution(Rng_);

    std::uniform_real_distribution<double> probabilityDistribution(0, 1);
    config.CoordinationProbability = probabilityDistribution(Rng_);
    config.ReadyToCommitProbability = probabilityDistribution(Rng_);
    config.AbortProbability = probabilityDistribution(Rng_);

    std::uniform_int_distribution<int> maxDelayDirstribution(0, 1000);
    config.MaxPrepareDelay = maxDelayDirstribution(Rng_);
    config.MaxCommitDelay = maxDelayDirstribution(Rng_);

    Cerr << Format("Generated random values for random stress test (%v)", config.Print()) << Endl;

    RunSimulation(config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTransactionSupervisor
