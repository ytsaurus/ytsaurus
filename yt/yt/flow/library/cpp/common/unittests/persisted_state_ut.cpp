#include <random>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/flow/library/cpp/common/persisted_state.h>
#include <yt/yt/flow/library/cpp/common/persisted_state_control.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestSerializer
{
public:
    std::string Encode(const std::optional<int>& value) const
    {
        return value.has_value() ? std::to_string(value.value()) : "";
    }

    std::string Encode(const std::optional<std::string>& value) const
    {
        return value.has_value() ? "!" + value.value() : "";
    }

    std::optional<int> Decode(const std::string& serial, std::type_identity<int>) const
    {
        return serial.empty() ? std::optional<int>{std::nullopt} : std::optional<int>{std::stoi(serial)};
    }

    std::optional<std::string> Decode(const std::string& serial, std::type_identity<std::string>) const
    {
        return serial.empty() ? std::optional<std::string>{std::nullopt} : std::optional<std::string>{serial.substr(1)};
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestDatabaseRow
{
    TSequenceId SequenceId;
    EStorageRowFlags Flags;
    TPersistedStateName Name;
    std::string KeyLeft;
    std::string KeyRight;
    std::string Value;
    bool operator==(const TTestDatabaseRow&) const = default;
};

struct TTestDatabase
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    std::map<TSequenceId, TTestDatabaseRow> Data;
    THashSet<TSequenceId> UndefinedRecords;
    bool DebugDisconnect = false;
    bool DebugInjectFake1 = false;
    bool DebugInjectFake2 = false;
    ssize_t ExecuteActionsBeforeFailure = std::numeric_limits<ssize_t>::max();
    ssize_t ExecuteActionsBeforeCommittedFailure = std::numeric_limits<ssize_t>::max();
    ssize_t InsertionCount = 0;
    ssize_t DeletionCount = 0;
    double RandomExecuteFailProbability = 0;

    TTestDatabase Clone()
    {
        auto guard = Guard(Lock);
        return TTestDatabase{.Data = Data, .UndefinedRecords = UndefinedRecords};
    }
};

class TStorageHandler
    : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = TPersistedStateStorageRow<std::string>;

    TStorageHandler(TTestDatabase& database)
        : Database_{database}
    {
        MaxSelectSize_ = 2;
        MaxExecuteSize_ = 10;
    }

    ssize_t DebugAlterMaxExecuteSize(ssize_t newSize)
    {
        ssize_t old = MaxExecuteSize_;
        MaxExecuteSize_ = newSize;
        return old;
    }

    void Select(TSequenceId lastSequenceId, std::vector<TStorageRow>& result) override
    {
        THROW_ERROR_EXCEPTION_IF(Database_.DebugDisconnect, "Debug disconnect");
        auto it = Database_.Data.upper_bound(lastSequenceId);
        if (Database_.DebugInjectFake1) {
            result.emplace_back(TStorageRow{
                FakeSequenceId,
                EStorageRowFlags::Commit,
                "test",
                "",
                "0",
                ""});
            Database_.DebugInjectFake1 = false;
        }
        for (ssize_t i = 0; i < MaxSelectSize_ && it != Database_.Data.end(); ++i, ++it) {
            result.emplace_back(TStorageRow{
                it->second.SequenceId,
                it->second.Flags,
                it->second.Name,
                it->second.KeyLeft,
                it->second.KeyRight,
                it->second.Value});
        }
        if (Database_.DebugInjectFake2) {
            result.emplace_back(TStorageRow{
                FakeSequenceId,
                EStorageRowFlags::Commit,
                "test",
                "0",
                "",
                ""});
            Database_.DebugInjectFake2 = false;
        }
    }

    void Execute(
        std::vector<TStorageRow>&& insertRows,
        const std::vector<TSequenceId>& deleteRows,
        bool,
        const std::vector<TPersistedStateCommitContext*>& contexts) override
    {
        EXPECT_EQ(std::ssize(contexts), 0);
        EXPECT_LE(std::ssize(insertRows) + std::ssize(deleteRows), MaxExecuteSize_);

        auto guard = Guard(Database_.Lock);

        bool randomFail = RandomNumber<double>() < Database_.RandomExecuteFailProbability;
        if (randomFail || Database_.DebugDisconnect || Database_.ExecuteActionsBeforeFailure <= 0) {
            for (auto& row : insertRows) {
                // We are foring to emulate DB error. Actually DB error does not mean that the transaction was not committed.
                // So it should be correct to allow cleanup attempts that tries to delete those records.
                Database_.UndefinedRecords.insert(row.SequenceId);
            }
        }
        THROW_ERROR_EXCEPTION_IF(randomFail, "Random execute failure");
        THROW_ERROR_EXCEPTION_IF(Database_.DebugDisconnect, "Debug disconnect");
        THROW_ERROR_EXCEPTION_IF(Database_.ExecuteActionsBeforeFailure <= 0, "Countdown failure");
        Database_.ExecuteActionsBeforeFailure--;
        for (auto sequenceId : deleteRows) {
            EXPECT_TRUE(Database_.Data.contains(sequenceId) || Database_.UndefinedRecords.contains(sequenceId));
            Database_.Data.erase(sequenceId);
            Database_.UndefinedRecords.erase(sequenceId);
        }
        for (auto& row : insertRows) {
            EXPECT_FALSE(Database_.Data.contains(row.SequenceId));
            Database_.Data.emplace(
                row.SequenceId,
                TTestDatabaseRow{row.SequenceId,
                    row.Flags,
                    row.Name,
                    row.KeyLeft,
                    row.KeyRight,
                    row.Value});
        }
        Database_.InsertionCount += std::ssize(insertRows);
        Database_.DeletionCount += std::ssize(deleteRows);
        if (Database_.ExecuteActionsBeforeCommittedFailure <= 0) {
            for (auto sequenceId : deleteRows) {
                // We are foring to emulate network error after DB commit error.
                // It should be correct to allow cleanup attempts that tries to delete again those records.
                Database_.UndefinedRecords.insert(sequenceId);
            }
        }
        THROW_ERROR_EXCEPTION_IF(Database_.ExecuteActionsBeforeCommittedFailure <= 0, "Countdown failure");
        Database_.ExecuteActionsBeforeCommittedFailure--;
    }

private:
    TTestDatabase& Database_;
};

template <class T>
std::set<T> SortedConst(std::vector<T>&& unsorted)
{
    return {std::make_move_iterator(unsorted.begin()), std::make_move_iterator(unsorted.end())};
}

template <class T, class U>
std::set<std::pair<std::remove_const_t<T>, std::remove_const_t<U>>> SortedConst(std::vector<std::pair<T, U>>&& unsorted)
{
    return {std::make_move_iterator(unsorted.begin()), std::make_move_iterator(unsorted.end())};
}

//! Check contents of the state, using different methods.
template <class TKey, class TValue>
void CheckStateContents(const TPersistedStatePtr<TKey, TValue>& state, const std::unordered_map<TKey, TValue>& mustBe, const std::vector<TKey>& checkKeys)
{
    EXPECT_EQ(SortedConst(GetKeys(*state)), SortedConst(GetKeys(mustBe)));
    EXPECT_EQ(SortedConst(GetValues(*state)), SortedConst(GetValues(mustBe)));
    EXPECT_EQ(SortedConst(GetItems(*state)), SortedConst(GetItems(mustBe)));
    for (const auto& [key, value] : mustBe) {
        EXPECT_TRUE(state->contains(key));
        EXPECT_EQ(state->at(key), value);
        EXPECT_EQ(*state->FindPtr(key), value);
        EXPECT_NE(state->find(key), state->end());
        EXPECT_EQ(state->find(key)->first, key);
        EXPECT_EQ(state->find(key)->second, value);
    }
    std::unordered_set<TKey> foundKeys;
    for (auto it = state->begin(); it != state->end(); ++it) {
        const auto& key = it->first;
        const auto& value = it->second;
        EXPECT_EQ(value, mustBe.at(key));
        foundKeys.insert(key);
    }
    EXPECT_EQ(foundKeys.size(), mustBe.size());
    foundKeys.clear();
    for (const auto& [key, value] : *state) {
        EXPECT_EQ(value, mustBe.at(key));
        foundKeys.insert(key);
    }
    EXPECT_EQ(foundKeys.size(), mustBe.size());
    for (const auto& key : checkKeys) {
        if (!mustBe.contains(key)) {
            EXPECT_FALSE(state->contains(key));
            EXPECT_EQ(state->FindPtr(key), nullptr);
            EXPECT_EQ(state->find(key), state->end());
        }
    }

    EXPECT_EQ(state->RecordCount(), static_cast<int>(mustBe.size() * 2 + 1));
}

//! Make replica to follow the leader until reached.
template <class TKey, class TValue, class TDefaultSerializer>
void Follow(
    const TPersistedStateControlPtr<TKey, TValue, TDefaultSerializer>& replica,
    const TPersistedStateControlPtr<TKey, TValue, TDefaultSerializer>& leader,
    ssize_t BatchSize)
{
    while (replica->GetSequenceId() != leader->GetSequenceId()) {
        replica->Apply(leader->Follow(replica->GetSequenceId(), BatchSize), nullptr);
    }
}

//! Smoke test. Just several action.
TEST(TPersistedStateTest, Basics)
{
    TTestDatabase database;
    auto leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(database));
    auto leaderState1 = leader->CreateState<int, std::string>("test1");
    auto leaderState2 = leader->CreateState<std::string, int>("test2");
    leader->Recover();

    auto replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
    auto replicaState1 = replica->CreateState<int, std::string>("test1");
    auto replicaState2 = replica->CreateState<std::string, int>("test2");

    auto checkVersions = [&] () {
        EXPECT_EQ(leaderState1->GetControlSequenceId(), leader->GetSequenceId());
        EXPECT_EQ(leaderState2->GetControlSequenceId(), leader->GetSequenceId());
        EXPECT_EQ(replicaState1->GetControlSequenceId(), replica->GetSequenceId());
        EXPECT_EQ(replicaState2->GetControlSequenceId(), replica->GetSequenceId());
    };

    EXPECT_EQ(leaderState1->RecordCount(), 1);
    EXPECT_EQ(leaderState2->RecordCount(), 1);
    EXPECT_EQ(replicaState1->RecordCount(), 1);
    EXPECT_EQ(replicaState2->RecordCount(), 1);

    // Add one value to the first table.
    leaderState1->insert_or_assign(0, "test0");
    EXPECT_EQ(database.Data.size(), 3u);
    CheckStateContents(leaderState1, {{0, "test0"}}, {});
    CheckStateContents(replicaState1, {}, {0});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 1);
    CheckStateContents(replicaState1, {{0, "test0"}}, {});
    checkVersions();

    // Add one value to the second table.
    leaderState2->insert_or_assign("aaa", 111);
    EXPECT_EQ(database.Data.size(), 6u);
    CheckStateContents(leaderState2, {{"aaa", 111}}, {});
    CheckStateContents(replicaState2, {}, {"aaa"});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 100500);
    CheckStateContents(replicaState2, {{"aaa", 111}}, {});
    checkVersions();

    // Add another value to the first table.
    {
        auto [it, is_inserted] = leaderState1->insert_or_assign(1, "test1");
        EXPECT_TRUE(is_inserted);
        EXPECT_EQ(it->first, 1);
        EXPECT_EQ(it->second, "test1");
    }
    EXPECT_EQ(database.Data.size(), 8u);
    CheckStateContents(leaderState1, {{0, "test0"}, {1, "test1"}}, {});
    CheckStateContents(replicaState1, {{0, "test0"}}, {1});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}}, {});
    checkVersions();

    // Add one more value to the first table.
    {
        auto [it, is_inserted] = leaderState1->emplace(2, "test2");
        EXPECT_TRUE(is_inserted);
        EXPECT_EQ(it->first, 2);
        EXPECT_EQ(it->second, "test2");
    }
    EXPECT_EQ(database.Data.size(), 10u);
    CheckStateContents(leaderState1, {{0, "test0"}, {1, "test1"}, {2, "test2"}}, {});
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}}, {});

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}, {2, "test2"}}, {});

    // Erase the last inserted value.
    leaderState1->erase(2);
    EXPECT_EQ(database.Data.size(), 8u);
    CheckStateContents(leaderState1, {{0, "test0"}, {1, "test1"}}, {2});
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}, {2, "test2"}}, {});

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}}, {2});

    // Can't update value with emplace.
    {
        auto [it, is_inserted] = leaderState1->emplace(0, "test00");
        EXPECT_FALSE(is_inserted);
        EXPECT_EQ(it->first, 0);
        EXPECT_EQ(it->second, "test0");
    }

    // Update value.
    {
        auto [it, is_inserted] = leaderState1->insert_or_assign(0, "test00");
        EXPECT_FALSE(is_inserted);
        EXPECT_EQ(it->first, 0);
        EXPECT_EQ(it->second, "test00");
    }
    EXPECT_EQ(database.Data.size(), 8u);
    CheckStateContents(leaderState1, {{0, "test00"}, {1, "test1"}}, {});
    CheckStateContents(replicaState1, {{0, "test0"}, {1, "test1"}}, {});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {{0, "test00"}, {1, "test1"}}, {});
    checkVersions();

    // Erase value.
    leaderState1->erase(0);
    EXPECT_EQ(database.Data.size(), 6u);
    CheckStateContents(leaderState1, {{1, "test1"}}, {0});
    CheckStateContents(replicaState1, {{0, "test00"}, {1, "test1"}}, {});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {{1, "test1"}}, {0});
    checkVersions();

    // Erase the last value.
    leaderState1->erase(1);
    EXPECT_EQ(database.Data.size(), 4u);
    CheckStateContents(leaderState1, {}, {0, 1});
    CheckStateContents(replicaState1, {{1, "test1"}}, {0});
    checkVersions();

    // Ensure the replica follows.
    Follow(replica, leader, 2);
    CheckStateContents(replicaState1, {}, {0, 1});
    checkVersions();

    // Recreate and reconnect to database.
    leaderState1->insert_or_assign(0, "test");

    leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(database));
    leaderState1 = leader->CreateState<int, std::string>("test1");
    leaderState2 = leader->CreateState<std::string, int>("test2");
    leader->Recover();
    CheckStateContents(leaderState1, {{0, "test"}}, {1});
    CheckStateContents(leaderState2, {{"aaa", 111}}, {});
    checkVersions();

    replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
    replicaState1 = replica->CreateState<int, std::string>("test1");
    replicaState2 = replica->CreateState<std::string, int>("test2");
    Follow(replica, leader, 2);

    CheckStateContents(replicaState1, {{0, "test"}}, {1});
    CheckStateContents(replicaState2, {{"aaa", 111}}, {});
    checkVersions();

    leaderState1->erase(0);
    leaderState2->insert_or_assign("bbb", 222);
    CheckStateContents(leaderState1, {}, {0, 1});
    CheckStateContents(leaderState2, {{"aaa", 111}, {"bbb", 222}}, {});
    checkVersions();

    Follow(replica, leader, 1);
    CheckStateContents(replicaState1, {}, {0, 1});
    CheckStateContents(replicaState2, {{"aaa", 111}, {"bbb", 222}}, {});
    checkVersions();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestActionType,
    (Insert)
    (Update)
    (Erase)
);

struct TTestAction
{
    ETestActionType Type;
    int Key;
    int Value;
};

//! Massive test. A batch of random actions. Check that a replica is synced from any delay point.
TEST(TPersistedStateTest, Massive)
{
    constexpr int numberOfActions = 200;
    constexpr int numberOfGroupsOfActions = 20;
    constexpr int numberOfKeys = 10;
    constexpr int numberOfValues = 100;
    constexpr int numberOfApplyRows = 7;

    // Generate random action list.
    std::random_device rd;
    auto seed = rd();
    Cerr << "TPersistedStateTest: Massive: Random seed: " << seed << "\n";
    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> genType(0, TEnumTraits<ETestActionType>::GetDomainSize() - 1);
    std::uniform_int_distribution<int> genKey(0, numberOfKeys - 1);
    std::uniform_int_distribution<int> genValue(0, numberOfValues - 1);
    std::vector<TTestAction> testActions(numberOfActions);
    std::unordered_map<int, int> referenceMap;
    for (auto& action : testActions) {
        while (true) {
            action.Type = static_cast<ETestActionType>(genType(gen));
            action.Key = genKey(gen);
            switch (action.Type) {
                case ETestActionType::Insert: {
                    if (referenceMap.contains(action.Key)) {
                        continue;
                    }
                    action.Value = genValue(gen);
                    referenceMap.insert_or_assign(action.Key, action.Value);
                    break;
                }
                case ETestActionType::Update: {
                    if (!referenceMap.contains(action.Key)) {
                        continue;
                    }
                    action.Value = genValue(gen);
                    referenceMap.insert_or_assign(action.Key, action.Value);
                    break;
                }
                case ETestActionType::Erase: {
                    if (!referenceMap.contains(action.Key)) {
                        continue;
                    }
                    referenceMap.erase(action.Key);
                    break;
                }
            };
            break;
        }
    }

    std::vector<int> allKeys;
    allKeys.reserve(numberOfKeys);
    for (int i = 0; i < numberOfKeys; i++) {
        allKeys.push_back(i);
    }

    for (int i = 1; i <= numberOfGroupsOfActions; i++) {
        for (int j = 0; j <= i; j++) {
            TTestDatabase database;
            auto leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(database));
            auto leaderState = leader->CreateState<int, int>("test");
            auto replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
            auto replicaState = replica->CreateState<int, int>("test");
            leader->Recover();
            referenceMap.clear();

            auto apply = [&] (int k) {
                const auto& action = testActions[k];
                switch (action.Type) {
                    case ETestActionType::Insert: {
                        referenceMap.insert_or_assign(action.Key, action.Value);
                        auto [it, is_inserted] = leaderState->insert_or_assign(action.Key, action.Value);
                        EXPECT_TRUE(is_inserted);
                        break;
                    }
                    case ETestActionType::Update: {
                        referenceMap.insert_or_assign(action.Key, action.Value);
                        auto [it, is_inserted] = leaderState->insert_or_assign(action.Key, action.Value);
                        EXPECT_FALSE(is_inserted);
                        break;
                    }
                    case ETestActionType::Erase: {
                        referenceMap.erase(action.Key);
                        auto erased = leaderState->erase(action.Key);
                        EXPECT_EQ(erased, 1u);
                        break;
                    }
                };
            };

            // Execute some actions.
            int k_init = j * numberOfActions / numberOfGroupsOfActions;
            for (int k = 0; k < k_init; k++) {
                apply(k);
            }
            // And sync replica.
            Follow(replica, leader, numberOfApplyRows);
            CheckStateContents(leaderState, referenceMap, allKeys);
            CheckStateContents(replicaState, referenceMap, allKeys);

            // Execute more actions.
            int k_final = i * numberOfActions / numberOfGroupsOfActions;
            for (int k = k_init; k < k_final; k++) {
                apply(k);
            }
            // And sync replica again.
            Follow(replica, leader, numberOfApplyRows);
            CheckStateContents(leaderState, referenceMap, allKeys);
            CheckStateContents(replicaState, referenceMap, allKeys);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Test various possible exceptions.
TEST(TPersistedStateTest, Misuse)
{
    TTestDatabase database;
    auto dbHandler = New<TStorageHandler>(database);
    auto leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(dbHandler);
    auto leaderState = leader->CreateState<int, int>("test");
    EXPECT_THROW_WITH_SUBSTRING((leader->CreateState<int, int>("test")), "State with name 'test' already registered");
    EXPECT_THROW_WITH_SUBSTRING(leaderState->insert_or_assign(1, 1), "Can't modify state: is either not leader or not recovered yet");
    EXPECT_THROW_WITH_SUBSTRING(leaderState->erase(1), "Can't modify state: is either not leader or not recovered yet");
    leader->Recover();
    EXPECT_THROW_WITH_SUBSTRING(leader->Recover(), "Can't recover twice");
    EXPECT_THROW_WITH_SUBSTRING((leader->CreateState<int, int>("test2")), "States must be created before recover");
    leaderState->insert_or_assign(1, 1);
    EXPECT_THROW_WITH_SUBSTRING(leaderState->at(3), "TPersistedState::at: out of range");

    auto replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
    auto replicaState = replica->CreateState<int, int>("test");
    EXPECT_THROW_WITH_SUBSTRING(replica->Recover(), "Can't recover because is not leader");
    EXPECT_THROW_WITH_SUBSTRING(replicaState->insert_or_assign(1, 1), "Can't modify state: is either not leader or not recovered yet");
    EXPECT_THROW_WITH_SUBSTRING(replicaState->erase(1), "Can't modify state: is either not leader or not recovered yet");

    leader = nullptr;
    EXPECT_THROW_WITH_SUBSTRING(leaderState->insert_or_assign(1, 1), "Persisted state control has been destroyed");
    EXPECT_THROW_WITH_SUBSTRING(leaderState->erase(1), "Persisted state control has been destroyed");
}

////////////////////////////////////////////////////////////////////////////////

//! Test rollback in case of exception in database connector.
TEST(TPersistedStateTest, ExceptionSafety)
{
    TTestDatabase database;
    auto leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(database));
    auto leaderState = leader->CreateState<int, int>("test");
    auto replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
    auto replicaState = replica->CreateState<int, int>("test");
    leader->Recover();
    leaderState->insert_or_assign(2, 2);
    leaderState->insert_or_assign(4, 4);
    Follow(replica, leader, 1);
    CheckStateContents(leaderState, {{2, 2}, {4, 4}}, {1, 3, 5});
    CheckStateContents(replicaState, {{2, 2}, {4, 4}}, {1, 3, 5});

    auto currentState = leader->Follow(TSequenceId{0}, 100500);
    database.DebugDisconnect = true;

    for (int key : {1, 2, 3, 4, 5}) {
        EXPECT_THROW_WITH_SUBSTRING(leaderState->insert_or_assign(key, key), "Debug disconnect");
        CheckStateContents(leaderState, {{2, 2}, {4, 4}}, {1, 3, 5});
        EXPECT_EQ(leader->Follow(TSequenceId{0}, 100500), currentState);
    }
    for (int key : {2, 4}) {
        EXPECT_THROW_WITH_SUBSTRING(leaderState->erase(key), "Debug disconnect");
        CheckStateContents(leaderState, {{2, 2}, {4, 4}}, {1, 3, 5});
        EXPECT_EQ(leader->Follow(TSequenceId{0}, 100500), currentState);
    }

    Follow(replica, leader, 1);
    CheckStateContents(leaderState, {{2, 2}, {4, 4}}, {1, 3, 5});
    CheckStateContents(replicaState, {{2, 2}, {4, 4}}, {1, 3, 5});

    database.DebugDisconnect = false;
    leaderState->insert_or_assign(3, 3);
    leaderState->erase(2);
    CheckStateContents(leaderState, {{3, 3}, {4, 4}}, {1, 2, 5});
    Follow(replica, leader, 1);
    CheckStateContents(replicaState, {{3, 3}, {4, 4}}, {1, 2, 5});
}

////////////////////////////////////////////////////////////////////////////////

//! Test rollback in case of exception in database connector.
TEST(TPersistedStateTest, Subscriptions)
{
    TTestDatabase database;
    auto leader = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(database));
    auto leaderState1 = leader->CreateState<int, std::string>("test1");
    auto leaderState2 = leader->CreateState<int, std::string>("test2");
    leader->Recover();

    using TOnInsert = TPersistedState<int, std::string>::TOnInsert;
    using TOnErase = TPersistedState<int, std::string>::TOnErase;

    int hits = 0;
    auto addHit = [&] (auto) {
        hits++;
    };
    leaderState1->Subscribe(TOnInsert{addHit}, TOnErase{addHit});

    std::vector<int> inserts;
    std::vector<int> erases;
    auto addInserts = [&] (int x) {
        inserts.push_back(x);
    };
    auto addErases = [&] (int x) {
        erases.push_back(x);
    };

    auto s1 = leaderState1->Subscribe(TOnInsert{addInserts}, TOnErase{[&] (const int& x) {
        erases.push_back(x);
    }});

    leaderState1->insert_or_assign(1, "aa");
    leaderState1->insert_or_assign(2, "bb");
    leaderState1->insert_or_assign(1, "aaa");
    leaderState1->erase(2);
    leaderState2->insert_or_assign(3, "cc");

    EXPECT_EQ(erases, std::vector({1, 2}));
    EXPECT_EQ(inserts, std::vector({1, 2, 1}));
    EXPECT_EQ(hits, 5);

    auto replica = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>();
    auto replicaState1 = replica->CreateState<int, std::string>("test1");
    auto replicaState2 = replica->CreateState<int, std::string>("test2");

    erases.clear();
    inserts.clear();
    auto s2 = replicaState1->Subscribe(
        TOnInsert{[&] (const int& x) {
            inserts.push_back(x);
        }},
        TOnErase{addErases});

    Follow(replica, leader, 1);

    EXPECT_EQ(erases, std::vector<int>({}));
    EXPECT_EQ(inserts, std::vector({1}));
    EXPECT_EQ(hits, 5);

    leaderState1->Unsubscribe(s1);

    leaderState1->insert_or_assign(2, "bbb");
    leaderState1->erase(1);

    EXPECT_EQ(erases, std::vector<int>({}));
    EXPECT_EQ(inserts, std::vector({1}));

    CheckStateContents(replicaState1, {{1, "aaa"}}, {2});
    Follow(replica, leader, 1);
    CheckStateContents(replicaState1, {{2, "bbb"}}, {1});

    EXPECT_EQ(erases, std::vector({1}));
    EXPECT_EQ(inserts, std::vector({1, 2}));

    replicaState1->Unsubscribe(s2);
    leaderState1->erase(2);

    // Get value in trigger.
    s1 = leaderState1->Subscribe(
        TOnInsert{[&] (const int& k) {
            EXPECT_EQ(leaderState1->contains(k), 1);
            EXPECT_EQ(leaderState1->at(k), "xx");
        }},
        TOnErase{[&] (const int&) {
            EXPECT_TRUE(false);
        }});
    leaderState1->insert_or_assign(1, "xx");
    leaderState1->Unsubscribe(s1);

    s1 = leaderState1->Subscribe(
        TOnInsert{[&] (const int& k) {
            EXPECT_EQ(leaderState1->contains(k), 1);
            EXPECT_EQ(leaderState1->at(k), "yy");
        }},
        TOnErase{[&] (const int& k) {
            EXPECT_EQ(leaderState1->contains(k), 1);
            // See the new value.
            EXPECT_EQ(leaderState1->at(k), "yy");
        }});
    leaderState1->insert_or_assign(1, "yy");
    leaderState1->Unsubscribe(s1);

    s1 = leaderState1->Subscribe(
        TOnInsert{[&] (const int& k) {
            EXPECT_EQ(leaderState1->contains(k), 1);
            EXPECT_EQ(leaderState1->at(k), "yy");
        }},
        TOnErase{[&] (const int& k) {
            EXPECT_EQ(leaderState1->contains(k), 1);
            // See the new value.
            EXPECT_EQ(leaderState1->at(k), "yy");
        }});
    leaderState1->insert_or_assign(1, "yy");
    leaderState1->Unsubscribe(s1);
}

////////////////////////////////////////////////////////////////////////////////

class TTester
{
public:
    using TDBKey = std::string;
    using TDBValue = std::string;
    using TSerializer = TTestSerializer;

    TTestDatabase Database;
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>(Database);

    TPersistedStateControlPtr<TDBKey, TDBValue, TSerializer> Leader = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>(StorageHandler);
    TPersistedStatePtr<int, std::string> LeaderState1 = Leader->CreateState<int, std::string>("test1");
    TPersistedStatePtr<std::string, int> LeaderState2 = Leader->CreateState<std::string, int>("test2");

    TPersistedStateControlPtr<TDBKey, TDBValue, TSerializer> Replica = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>();
    TPersistedStatePtr<int, std::string> ReplicaState1 = Replica->CreateState<int, std::string>("test1");
    TPersistedStatePtr<std::string, int> ReplicaState2 = Replica->CreateState<std::string, int>("test2");

    TPersistedStateTransactionPtr Tx1;
    TPersistedStateTransactionPtr Tx2;
    TPersistedStateTransactionPtr Tx3;
    TPersistedStateTransactionPtr TxReplica;

    TTester(
        const std::unordered_map<int, std::string>& init1 = {{1, "a"}, {2, "b"}},
        const std::unordered_map<std::string, int>& init2 = {{"c", 3}, {"d", 4}})
    {
        Leader->Recover();
        for (const auto& [k, v] : init1) {
            LeaderState1->insert_or_assign(k, v);
        }
        for (const auto& [k, v] : init2) {
            LeaderState2->insert_or_assign(k, v);
        }
        Tx1 = Leader->StartTransaction();
        Tx2 = LeaderState2->StartTransaction();
        Tx3 = LeaderState1->StartTransaction();
        ReplicaSync();
        TxReplica = Replica->StartTransaction();
    }

    void ReplicaSync()
    {
        Follow(Replica, Leader, 100);
    }

    void ReplicaRejoin()
    {
        Replica = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>();
        ReplicaState1 = Replica->CreateState<int, std::string>("test1");
        ReplicaState2 = Replica->CreateState<std::string, int>("test2");
        ReplicaSync();
    }

    void FinalCheckContent(const std::unordered_map<int, std::string>& mustBe1, const std::unordered_map<std::string, int>& mustBe2)
    {
        CheckStateContents(LeaderState1, mustBe1, {});
        CheckStateContents(LeaderState2, mustBe2, {});
        ReplicaSync();
        CheckStateContents(ReplicaState1, mustBe1, {});
        CheckStateContents(ReplicaState2, mustBe2, {});
        EXPECT_EQ(Leader->Follow(TSequenceId{0}, SSIZE_MAX), Replica->Follow(TSequenceId{0}, SSIZE_MAX));
        ReplicaRejoin();
        CheckStateContents(ReplicaState1, mustBe1, {});
        CheckStateContents(ReplicaState2, mustBe2, {});
        EXPECT_EQ(Leader->Follow(TSequenceId{0}, SSIZE_MAX), Replica->Follow(TSequenceId{0}, SSIZE_MAX));
    }

    template <class TKey, class TValue>
    static void CheckLeaderTransactionVisionCommon(
        TPersistedStateTransactionPtr& tx,
        const TPersistedStatePtr<TKey, TValue>& state,
        const std::unordered_map<TKey, TValue>& mustBe,
        const std::vector<TKey>& checkKeys)
    {
        for (const auto& [key, value] : mustBe) {
            EXPECT_TRUE(state->contains(tx, key));
            EXPECT_EQ(state->at(tx, key), value);
            EXPECT_EQ(*state->FindPtr(tx, key), value);
            EXPECT_NE(state->find(tx, key), state->end(tx));
            EXPECT_EQ(state->find(tx, key)->first, key);
            EXPECT_EQ(state->find(tx, key)->second, value);
        }
        std::unordered_set<TKey> foundKeys;
        for (auto it = state->begin(tx); it != state->end(tx); ++it) {
            const auto& key = it->first;
            const auto& value = it->second;
            EXPECT_EQ(value, mustBe.at(key));
            foundKeys.insert(key);
        }
        EXPECT_EQ(foundKeys.size(), mustBe.size());
        for (const auto& key : checkKeys) {
            if (!mustBe.contains(key)) {
                EXPECT_FALSE(state->contains(tx, key));
                EXPECT_EQ(state->FindPtr(tx, key), nullptr);
                EXPECT_EQ(state->find(tx, key), state->end(tx));
            }
        }
    }

    void CheckLeaderTransactionVision(
        TPersistedStateTransactionPtr& tx,
        const std::unordered_map<int, std::string>& mustBe1,
        const std::unordered_map<std::string, int>& mustBe2)
    {
        CheckLeaderTransactionVisionCommon(tx, LeaderState1, mustBe1, {0, 1, 2, 3, 4, 5});
        CheckLeaderTransactionVisionCommon(tx, LeaderState2, mustBe2, {"a", "b", "c", "d", "e", "f"});
    }
};

//! Test that a read-only snapshot seeded with `copyWritesFrom` observes the source
//! transaction's still-uncommitted changes on top of committed state (YTFLOW-625).
TEST(TPersistedStateTest, CopyWritesFromTransaction)
{
    // A snapshot taken with copyWritesFrom sees committed + uncommitted writes;
    // a plain read-only snapshot sees only committed state.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc"); // insert new
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa"); // update existing
        t.LeaderState1->erase(t.Tx1, 2);                  // erase existing
        t.LeaderState2->insert_or_assign(t.Tx1, "e", 5);  // insert new into another state

        // Plain read-only snapshot: only committed state is visible.
        auto plain = t.Leader->StartTransaction(/*readOnly*/ true);
        t.CheckLeaderTransactionVision(plain, {{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});

        // Snapshot seeded with the write transaction's write set: committed + uncommitted.
        auto snapshot = t.Leader->StartTransaction(/*readOnly*/ true, t.Tx1);
        t.CheckLeaderTransactionVision(snapshot, {{1, "aa"}, {3, "cc"}}, {{"c", 3}, {"d", 4}, {"e", 5}});
        EXPECT_EQ(t.LeaderState1->size(snapshot), 2u);
        EXPECT_EQ(t.LeaderState2->size(snapshot), 3u);

        // The snapshot is still read-only.
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->insert_or_assign(snapshot, 9, "z"),
            "Transaction over state is read-only, modification is prohibited");
    }

    // The snapshot is an independent copy: writes made to the source afterwards are not
    // visible in it, and the source transaction can still commit normally.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->erase(t.Tx1, 2);

        auto snapshot = t.Leader->StartTransaction(/*readOnly*/ true, t.Tx1);
        t.CheckLeaderTransactionVision(snapshot, {{1, "aa"}, {3, "cc"}}, {{"c", 3}, {"d", 4}});

        // A write to the source after the snapshot does not leak into the snapshot.
        t.LeaderState1->insert_or_assign(t.Tx1, 4, "dd");
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 4), "dd");
        t.CheckLeaderTransactionVision(snapshot, {{1, "aa"}, {3, "cc"}}, {{"c", 3}, {"d", 4}});

        // Committed state is untouched while the source remains uncommitted.
        EXPECT_EQ(t.LeaderState1->at(1), "a");
        EXPECT_TRUE(t.LeaderState1->contains(2));

        // The source transaction commits normally; the read-only snapshot causes no conflict.
        t.Tx1->Commit();
        t.FinalCheckContent({{1, "aa"}, {3, "cc"}, {4, "dd"}}, {{"c", 3}, {"d", 4}});
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Test transactional conflicts.
TEST(TPersistedStateTest, Transactions)
{
    // Simple transaction just works.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        auto [it1, is_inserted1] = t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        EXPECT_FALSE(is_inserted1);
        EXPECT_EQ(it1->first, 1);
        EXPECT_EQ(it1->second, "aa");
        auto [it2, is_inserted2] = t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        EXPECT_TRUE(is_inserted2);
        EXPECT_EQ(it2->first, 3);
        EXPECT_EQ(it2->second, "cc");
        EXPECT_EQ(t.LeaderState1->erase(t.Tx1, 2), 1u);
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->at(t.Tx1, 2), "TPersistedState::at: out of range");
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "aa");
        EXPECT_EQ(t.LeaderState1->at(1), "a");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(1), "aa");
        t.FinalCheckContent({{1, "aa"}, {3, "cc"}}, {{"c", 3}, {"d", 4}});
    }

    // Read-only transaction forbids modification.
    {
        TTester t;
        auto tx = t.Leader->StartTransaction(/*readOnly*/ true);
        EXPECT_EQ(t.LeaderState1->at(tx, 1), "a");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->insert_or_assign(tx, 1, "aa"),
            "Transaction over state is read-only, modification is prohibited");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->emplace(tx, 1, "aa"),
            "Transaction over state is read-only, modification is prohibited");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->erase(tx, 1),
            "Transaction over state is read-only, modification is prohibited");
        EXPECT_TRUE(t.LeaderState1->contains(tx, 2));
    }

    // Simple transaction with erases and size checks.
    {
        TTester t;
        auto [it1, is_inserted1] = t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        EXPECT_FALSE(is_inserted1);
        EXPECT_EQ(it1->first, 1);
        EXPECT_EQ(it1->second, "aa");
        EXPECT_EQ(t.LeaderState1->size(t.Tx1), 2u);
        size_t eraseResult = t.LeaderState1->erase(t.Tx1, 1);
        EXPECT_EQ(t.LeaderState1->size(t.Tx1), 1u);
        EXPECT_EQ(eraseResult, 1u);
        size_t eraseResult2 = t.LeaderState1->erase(t.Tx1, 3);
        EXPECT_EQ(t.LeaderState1->size(t.Tx1), 1u);
        EXPECT_EQ(eraseResult2, 0u);
        size_t eraseResult3 = t.LeaderState1->erase(t.Tx1, 2);
        EXPECT_EQ(t.LeaderState1->size(t.Tx1), 0u);
        EXPECT_EQ(eraseResult3, 1u);
        t.Tx1->Commit();
        t.FinalCheckContent({}, {{"c", 3}, {"d", 4}});
    }

    // Simple R/W conflict.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "a");
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "ab");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "ab");
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "cc");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "cc");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(1), "aa");
        t.FinalCheckContent({{1, "aa"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // Several writes of the same key works similar.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "a");
        {
            auto [it, is_inserted] = t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa0");
            EXPECT_FALSE(is_inserted);
            EXPECT_EQ(it->first, 1);
            EXPECT_EQ(it->second, "aa0");
        }
        {
            auto [it, is_inserted] = t.LeaderState1->insert_or_assign(t.Tx2, 1, "ab0");
            EXPECT_FALSE(is_inserted);
            EXPECT_EQ(it->first, 1);
            EXPECT_EQ(it->second, "ab0");
        }
        {
            auto [it, is_inserted] = t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
            EXPECT_FALSE(is_inserted);
            EXPECT_EQ(it->first, 1);
            EXPECT_EQ(it->second, "aa");
        }
        {
            auto [it, is_inserted] = t.LeaderState1->insert_or_assign(t.Tx2, 1, "ab");
            EXPECT_FALSE(is_inserted);
            EXPECT_EQ(it->first, 1);
            EXPECT_EQ(it->second, "ab");
        }
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "ab");
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "cc");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "cc");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(1), "aa");
        t.FinalCheckContent({{1, "aa"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // RO transaction has a snapshot read view and is not aborted by conflict.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "a");
        t.Tx1->Commit();
        t.LeaderState2->insert_or_assign("e", 5);
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "a");
        EXPECT_FALSE(t.LeaderState2->contains(t.Tx2, "e"));
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->at(1), "aa");
        t.FinalCheckContent({{1, "aa"}, {2, "b"}}, {{"c", 3}, {"d", 4}, {"e", 5}});
    }

    // More complex R/W conflict works identically.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState2->at(t.Tx2, "c"), 3);
        t.LeaderState2->insert_or_assign(t.Tx1, "c", 5);
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "aa");
        t.Tx1->Commit();
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->at(t.Tx2, 1), "Operation over state has been interrupted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->insert_or_assign(t.Tx2, 1, ""), "Operation over state has been interrupted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(1), "a");
        EXPECT_EQ(t.LeaderState2->at("c"), 5);
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 5}, {"d", 4}});
    }

    // Many transactions can be aborted due to conflict.
    for (int txCount = 1; txCount <= 10; txCount++) {
        TTester t;
        std::vector<TPersistedStateTransactionPtr> losers;
        for (int i = 0; i < txCount; i++) {
            losers.push_back(t.Leader->StartTransaction());
            if (i % 2 == 0) {
                t.LeaderState1->FindPtr(losers.back(), 1);
                t.LeaderState1->insert_or_assign(losers.back(), 1, "f");
            } else {
                t.LeaderState2->FindPtr(losers.back(), "f");
                t.LeaderState2->insert_or_assign(losers.back(), "f", 0);
            }
        }
        t.LeaderState1->insert_or_assign(1, "f");
        t.LeaderState2->insert_or_assign("f", 0);
        for (int i = 0; i < txCount; i++) {
            EXPECT_THROW_WITH_SUBSTRING(losers[i]->Commit(), "Transaction has been aborted due to conflict");
        }
    }

    // More complex R/W conflict with deletes works identically.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState2->at(t.Tx2, "c"), 3);
        EXPECT_EQ(t.LeaderState2->erase(t.Tx1, "c"), 1u);
        EXPECT_EQ(t.LeaderState1->erase(t.Tx2, 1), 1u);
        t.Tx1->Commit();
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->at(t.Tx2, 1), "Operation over state has been interrupted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->insert_or_assign(t.Tx2, 1, ""), "Operation over state has been interrupted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(1), "a");
        EXPECT_EQ(t.LeaderState2->FindPtr("c"), nullptr);
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"d", 4}});
    }

    // Several deletes of the same key works quite the same.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState2->at(t.Tx2, "c"), 3);
        EXPECT_EQ(t.LeaderState2->erase(t.Tx1, "c"), 1u);
        EXPECT_EQ(t.LeaderState1->erase(t.Tx2, 1), 1u);
        EXPECT_EQ(t.LeaderState2->erase(t.Tx1, "c"), 0u);
        EXPECT_EQ(t.LeaderState1->erase(t.Tx2, 1), 0u);
        t.Tx1->Commit();
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->at(t.Tx2, 1), "Operation over state has been interrupted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.LeaderState1->insert_or_assign(t.Tx2, 1, ""), "Operation over state has been interrupted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(1), "a");
        EXPECT_EQ(t.LeaderState2->FindPtr("c"), nullptr);
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"d", 4}});
    }

    // Emplace implies read.
    {
        TTester t;
        auto [it1, is_inserted1] = t.LeaderState1->emplace(t.Tx1, 3, "c");
        EXPECT_TRUE(is_inserted1);
        EXPECT_EQ(it1->first, 3);
        EXPECT_EQ(it1->second, "c");
        auto [it2, is_inserted2] = t.LeaderState1->emplace(t.Tx2, 3, "d");
        EXPECT_TRUE(is_inserted2);
        EXPECT_EQ(it2->first, 3);
        EXPECT_EQ(it2->second, "d");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 3), "d");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(3), "c");
        t.FinalCheckContent({{1, "a"}, {2, "b"}, {3, "c"}}, {{"c", 3}, {"d", 4}});
    }

    // Emplace implies read even if it didn't change anything.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "bb");
        auto [it, is_inserted] = t.LeaderState1->emplace(t.Tx2, 2, "bbb");
        EXPECT_FALSE(is_inserted);
        EXPECT_EQ(it->first, 2);
        EXPECT_EQ(it->second, "b");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 2), "b");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->at(2), "bb");
        t.FinalCheckContent({{1, "a"}, {2, "bb"}}, {{"c", 3}, {"d", 4}});
    }

    // Emplace that fails because of previous assign.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "bb");
        t.LeaderState1->insert_or_assign(t.Tx2, 2, "bbb");
        auto [it, is_inserted] = t.LeaderState1->emplace(t.Tx2, 2, "bbb?");
        EXPECT_FALSE(is_inserted);
        EXPECT_EQ(it->first, 2);
        EXPECT_EQ(it->second, "bbb");
        t.Tx1->Commit();
        t.Tx2->Commit();
        t.FinalCheckContent({{1, "a"}, {2, "bbb"}}, {{"c", 3}, {"d", 4}});
    }

    // Conflicts can be caused by modification without transactions.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        EXPECT_EQ(t.LeaderState2->at(t.Tx2, "c"), 3);
        t.LeaderState2->insert_or_assign(t.Tx2, "c", 6);
        t.LeaderState1->erase(1);
        t.LeaderState2->insert_or_assign("c", 33);
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "aa");
        EXPECT_EQ(t.LeaderState2->at(t.Tx2, "c"), 6);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{2, "b"}}, {{"c", 33}, {"d", 4}});
    }

    // Phantom read is not allowed.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 3), false);
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "bb");
        t.LeaderState1->insert_or_assign(t.Tx2, 3, "??");
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 3), false);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{1, "a"}, {2, "b"}, {3, "??"}}, {{"c", 3}, {"d", 4}});
    }

    // Erase of nothing has no side effects and does not aborts phantom reader.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 3), false);
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "bb");
        EXPECT_EQ(t.LeaderState1->erase(t.Tx2, 3), 0u);
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 3), false);
        t.Tx1->Commit();
        t.FinalCheckContent({{1, "a"}, {2, "bb"}}, {{"c", 3}, {"d", 4}});
    }

    // Reading one key whilst writing another is not a conflict.
    {
        TTester t;
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 1), "a");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 1), "a");
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "ba");
        t.LeaderState1->insert_or_assign(t.Tx2, 2, "bb");
        EXPECT_EQ(t.LeaderState1->at(2), "b");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(2), "ba");
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->at(2), "bb");
        t.FinalCheckContent({{1, "a"}, {2, "bb"}}, {{"c", 3}, {"d", 4}});
    }

    // Reading an own value does not lead to conflict.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 2, "ba");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 2), true);
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 2), "ba");
        EXPECT_NE(t.LeaderState1->FindPtr(t.Tx1, 2), nullptr);
        t.LeaderState1->insert_or_assign(t.Tx2, 2, "bb");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 2), "bb");
        EXPECT_NE(t.LeaderState1->FindPtr(t.Tx2, 2), nullptr);
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->at(2), "bb");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(2), "ba");
        t.FinalCheckContent({{1, "a"}, {2, "ba"}}, {{"c", 3}, {"d", 4}});
    }

    // Several writes to that same key is allowed.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "c1");
        t.LeaderState1->insert_or_assign(t.Tx2, 3, "c2");
        EXPECT_EQ(t.LeaderState1->contains(3), false);
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 3), "c1");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 3), "c2");
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "c3");
        t.LeaderState1->insert_or_assign(t.Tx2, 3, "c4");
        EXPECT_EQ(t.LeaderState1->contains(3), false);
        EXPECT_EQ(t.LeaderState1->at(t.Tx1, 3), "c3");
        EXPECT_EQ(t.LeaderState1->at(t.Tx2, 3), "c4");
        t.Tx1->Commit();
        EXPECT_EQ(t.LeaderState1->at(3), "c3");
        t.Tx2->Commit();
        EXPECT_EQ(t.LeaderState1->at(3), "c4");
        t.FinalCheckContent({{1, "a"}, {2, "b"}, {3, "c4"}}, {{"c", 3}, {"d", 4}});
    }

    // Following on replica do not lead to conflict.
    {
        TTester t;
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 1), true);
        t.LeaderState1->insert_or_assign(1, "aa");
        t.Tx1->Commit();
        t.ReplicaSync();
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 1), true);
        t.TxReplica->Commit();
        t.FinalCheckContent({{1, "aa"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // Following on replica do not lead to conflict (erase too).
    {
        TTester t;
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 1), true);
        t.LeaderState1->erase(1);
        t.Tx1->Commit();
        t.ReplicaSync();
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 1), true);
        t.TxReplica->Commit();
        t.FinalCheckContent({{2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // Following on replica do not lead to conflict (phantom too).
    {
        TTester t;
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 3), false);
        t.LeaderState1->insert_or_assign(3, "cc");
        t.Tx1->Commit();
        t.ReplicaSync();
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 3), false);
        t.TxReplica->Commit();
        t.FinalCheckContent({{1, "a"}, {2, "b"}, {3, "cc"}}, {{"c", 3}, {"d", 4}});
    }

    // Read-only transaction on replica is correct even if data from leader degenerately erases phantom readings.
    {
        TTester t;
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 3), false);
        EXPECT_EQ(t.LeaderState1->erase(3), 0u);
        t.Tx1->Commit();
        t.ReplicaSync();
        EXPECT_EQ(t.ReplicaState1->contains(t.TxReplica, 3), false);
        t.TxReplica->Commit();
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // User-provided read tracking may interrupt transaction as usual read.
    {
        TTester t;
        t.LeaderState1->TrackRead(t.Tx1, 1);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->TrackRead(t.Tx2);
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "bb");
        t.LeaderState1->TrackRead(t.Tx3, 2);
        t.LeaderState1->insert_or_assign(1, "a!");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 2), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.Tx3->Commit();
    }

    // Multiple read tracking with the same key have no consequences.
    {
        TTester t;
        t.LeaderState1->contains(t.Tx1, 1);
        t.LeaderState1->TrackRead(t.Tx1, 1);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->TrackRead(t.Tx2, 1);
        t.LeaderState1->TrackRead(t.Tx2);
        t.LeaderState1->insert_or_assign(t.Tx2, 1, "bb");
        t.LeaderState1->TrackRead(t.Tx2);
        t.LeaderState1->TrackRead(t.Tx3, 2);
        t.LeaderState1->TrackRead(t.Tx3, 2);
        t.LeaderState1->insert_or_assign(1, "a!");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx1, 2), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Transaction has been aborted due to conflict");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.Tx3->Commit();
    }

    // Check transactional view.
    {
        TTester t;
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "a1");
        t.LeaderState1->erase(t.Tx1, 2);
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "c1");
        t.LeaderState1->emplace(t.Tx1, 4, "d1");
        t.LeaderState2->erase(t.Tx1, "c");
        t.LeaderState2->insert_or_assign(t.Tx1, "e", 1);

        t.LeaderState1->insert_or_assign(t.Tx2, 1, "a2");
        t.LeaderState1->erase(t.Tx2, 2);
        t.LeaderState1->insert_or_assign(t.Tx2, 3, "c2");
        t.LeaderState1->emplace(t.Tx2, 4, "d2");
        t.LeaderState2->erase(t.Tx2, "c");
        t.LeaderState2->insert_or_assign(t.Tx2, "e", 2);

        t.CheckLeaderTransactionVision(t.Tx1, {{1, "a1"}, {3, "c1"}, {4, "d1"}}, {{"d", 4}, {"e", 1}});
        t.CheckLeaderTransactionVision(t.Tx2, {{1, "a2"}, {3, "c2"}, {4, "d2"}}, {{"d", 4}, {"e", 2}});
    }

    // Check full CoW transactional view.
    {
        TTester t;
        t.LeaderState1->erase(1);
        t.LeaderState1->erase(2);
        t.LeaderState2->erase("c");
        t.LeaderState2->erase("d");

        t.CheckLeaderTransactionVision(t.Tx1, {{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
        t.CheckLeaderTransactionVision(t.Tx2, {{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // Check untouched and fully erased transactional view.
    {
        TTester t;
        t.LeaderState1->erase(t.Tx1, 1);
        t.LeaderState1->erase(t.Tx1, 2);
        t.LeaderState2->erase(t.Tx1, "c");
        t.LeaderState2->erase(t.Tx1, "d");

        t.CheckLeaderTransactionVision(t.Tx1, {}, {});
        t.CheckLeaderTransactionVision(t.Tx2, {{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }

    // Simple test of wrapper.
    {
        TTester t;
        TTransactionalPersistedState leader11{t.LeaderState1, t.Tx1};
        TTransactionalPersistedState leader12{t.LeaderState2, t.Tx1};
        TTransactionalPersistedState leader21{t.LeaderState1, t.Tx2};
        TTransactionalPersistedState leader22{t.LeaderState2, t.Tx2};
        leader11.insert_or_assign(2, "bb");
        leader11.emplace(3, "cc");
        leader12.erase("c");
        EXPECT_EQ(leader11.at(2), "bb");
        EXPECT_TRUE(leader11.contains(1));
        EXPECT_FALSE(leader11.contains(4));
        EXPECT_EQ(leader11.find(1)->second, "a");
        EXPECT_EQ(leader11.find(4), leader11.end());
        EXPECT_EQ(leader11.at(3), "cc");
        EXPECT_EQ(leader12.FindPtr("c"), nullptr);
        std::vector<std::pair<int, std::string>> test;
        for (const auto& [k, v] : leader21) {
            test.emplace_back(k, v);
        }
        leader22.erase("c");
        std::ranges::sort(test);
        std::vector<std::pair<int, std::string>> expected = {{1, "a"}, {2, "b"}};
        EXPECT_EQ(test, expected);
        t.Tx1->Commit();
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
    }
}

//! Test rollback of transactions in case if DB failure.
TEST(TPersistedStateTest, TransactionRevert)
{
    // Replace.
    {
        TTester t;
        t.Database.DebugDisconnect = true;
        auto storedStateBefore = t.Leader->Follow(TSequenceId{0}, 100);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 3), false);
        t.LeaderState2->insert_or_assign(t.Tx2, "c", 33);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Debug disconnect");
        auto storedStateAfter = t.Leader->Follow(TSequenceId{0}, 100);
        EXPECT_EQ(storedStateBefore, storedStateAfter);
        EXPECT_EQ(t.Replica->GetSequenceId(), t.Leader->GetSequenceId());
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }
    // Insert.
    {
        TTester t;
        t.Database.DebugDisconnect = true;
        auto storedStateBefore = t.Leader->Follow(TSequenceId{0}, 100);
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 3), false);
        t.LeaderState2->emplace(t.Tx2, "c", 33);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Debug disconnect");
        auto storedStateAfter = t.Leader->Follow(TSequenceId{0}, 100);
        EXPECT_EQ(storedStateBefore, storedStateAfter);
        EXPECT_EQ(t.Replica->GetSequenceId(), t.Leader->GetSequenceId());
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }
    // Erase.
    {
        TTester t;
        t.Database.DebugDisconnect = true;
        auto storedStateBefore = t.Leader->Follow(TSequenceId{0}, 100);
        t.LeaderState1->erase(t.Tx1, 2);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 3), false);
        t.LeaderState2->erase(t.Tx2, "c");
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Debug disconnect");
        auto storedStateAfter = t.Leader->Follow(TSequenceId{0}, 100);
        EXPECT_EQ(storedStateBefore, storedStateAfter);
        EXPECT_EQ(t.Replica->GetSequenceId(), t.Leader->GetSequenceId());
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }
    // All together.
    {
        TTester t;
        auto storedStateBefore = t.Leader->Follow(TSequenceId{0}, 100);
        t.Database.DebugDisconnect = true;
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.LeaderState1->erase(t.Tx1, 2);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 2), true);
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 3), false);
        t.LeaderState2->insert_or_assign(t.Tx2, "c", 33);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Debug disconnect");
        auto storedStateAfter = t.Leader->Follow(TSequenceId{0}, 100);
        EXPECT_EQ(storedStateBefore, storedStateAfter);
        EXPECT_EQ(t.Replica->GetSequenceId(), t.Leader->GetSequenceId());
        EXPECT_EQ(t.LeaderState1->contains(t.Tx2, 1), true);
        EXPECT_THROW_WITH_SUBSTRING(t.Tx2->Commit(), "Transaction has been aborted due to conflict");
        t.FinalCheckContent({{1, "a"}, {2, "b"}}, {{"c", 3}, {"d", 4}});
    }
}

struct TCounts
{
    THashMap<int, int> LeadInsCount;
    THashMap<int, int> LeadDelCount;
    THashMap<int, int> ReplInsCount;
    THashMap<int, int> ReplDelCount;
};

void Subscribe(TTester& t, TCounts& counts)
{
    using TOnInsert = TPersistedState<int, std::string>::TOnInsert;
    using TOnErase = TPersistedState<int, std::string>::TOnErase;
    t.LeaderState1->Subscribe(
        TOnInsert{[&counts] (int key) {
            counts.LeadInsCount[key]++;
        }},
        TOnErase{[&counts] (int key) {
            counts.LeadDelCount[key]++;
        }});
    t.ReplicaState1->Subscribe(
        TOnInsert{[&counts] (int key) {
            counts.ReplInsCount[key]++;
        }},
        TOnErase{[&counts] (int key) {
            counts.ReplDelCount[key]++;
        }});
}

//! Test subscriptions with transactions.
TEST(TPersistedStateTest, TransactionSubscriptions)
{
    // Replace.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.Tx1->Commit();
        EXPECT_EQ(counts.LeadInsCount.size(), 1u);
        EXPECT_EQ(counts.LeadInsCount.at(1), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 1u);
        EXPECT_EQ(counts.LeadDelCount.at(1), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
        t.ReplicaSync();
        EXPECT_EQ(counts.LeadInsCount.size(), 1u);
        EXPECT_EQ(counts.LeadInsCount.at(1), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 1u);
        EXPECT_EQ(counts.LeadDelCount.at(1), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 1u);
        EXPECT_EQ(counts.ReplInsCount.at(1), 1);
        EXPECT_EQ(counts.ReplDelCount.size(), 1u);
        EXPECT_EQ(counts.ReplDelCount.at(1), 1);
    }

    // Insert.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.Tx1->Commit();
        EXPECT_EQ(counts.LeadInsCount.size(), 1u);
        EXPECT_EQ(counts.LeadInsCount.at(3), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 0u);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
        t.ReplicaSync();
        EXPECT_EQ(counts.LeadInsCount.size(), 1u);
        EXPECT_EQ(counts.LeadInsCount.at(3), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 0u);
        EXPECT_EQ(counts.ReplInsCount.size(), 1u);
        EXPECT_EQ(counts.ReplInsCount.at(3), 1);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
    }

    // Erase.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->erase(t.Tx1, 2);
        t.Tx1->Commit();
        EXPECT_EQ(counts.LeadInsCount.size(), 0u);
        EXPECT_EQ(counts.LeadDelCount.size(), 1u);
        EXPECT_EQ(counts.LeadDelCount.at(2), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
        t.ReplicaSync();
        EXPECT_EQ(counts.LeadInsCount.size(), 0u);
        EXPECT_EQ(counts.LeadDelCount.size(), 1u);
        EXPECT_EQ(counts.LeadDelCount.at(2), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 1u);
        EXPECT_EQ(counts.ReplDelCount.at(2), 1);
    }

    // All together.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.LeaderState1->erase(t.Tx1, 2);
        t.Tx1->Commit();
        EXPECT_EQ(counts.LeadInsCount.size(), 2u);
        EXPECT_EQ(counts.LeadInsCount.at(1), 1);
        EXPECT_EQ(counts.LeadInsCount.at(3), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 2u);
        EXPECT_EQ(counts.LeadDelCount.at(1), 1);
        EXPECT_EQ(counts.LeadDelCount.at(2), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
        t.ReplicaSync();
        EXPECT_EQ(counts.LeadInsCount.size(), 2u);
        EXPECT_EQ(counts.LeadInsCount.at(1), 1);
        EXPECT_EQ(counts.LeadInsCount.at(3), 1);
        EXPECT_EQ(counts.LeadDelCount.size(), 2u);
        EXPECT_EQ(counts.LeadDelCount.at(1), 1);
        EXPECT_EQ(counts.LeadDelCount.at(2), 1);
        EXPECT_EQ(counts.ReplInsCount.size(), 2u);
        EXPECT_EQ(counts.ReplInsCount.at(1), 1);
        EXPECT_EQ(counts.ReplInsCount.at(3), 1);
        EXPECT_EQ(counts.ReplDelCount.size(), 2u);
        EXPECT_EQ(counts.ReplDelCount.at(1), 1);
        EXPECT_EQ(counts.ReplDelCount.at(2), 1);
    }

    // Must not trigger on revert.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.LeaderState1->erase(t.Tx1, 2);
        t.Database.DebugDisconnect = true;
        EXPECT_THROW_WITH_SUBSTRING(t.Tx1->Commit(), "Debug disconnect");
        EXPECT_EQ(counts.LeadInsCount.size(), 0u);
        EXPECT_EQ(counts.LeadDelCount.size(), 0u);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
    }

    // Must not trigger on Abort.
    {
        TTester t;
        TCounts counts;
        Subscribe(t, counts);
        t.LeaderState1->insert_or_assign(t.Tx1, 1, "aa");
        t.LeaderState1->insert_or_assign(t.Tx1, 3, "cc");
        t.LeaderState1->erase(t.Tx1, 2);
        t.Tx1->Abort();
        EXPECT_EQ(counts.LeadInsCount.size(), 0u);
        EXPECT_EQ(counts.LeadDelCount.size(), 0u);
        EXPECT_EQ(counts.ReplInsCount.size(), 0u);
        EXPECT_EQ(counts.ReplDelCount.size(), 0u);
    }
}

//! Test various exceptions with transactions.
TEST(TPersistedStateTest, TransactionMisuse)
{
    // All conflict exceptions were already tested above.
    TTester t;
    auto txCommitted = t.Leader->StartTransaction();
    txCommitted->Commit();
    auto txAborted = t.Leader->StartTransaction();
    txAborted->Abort();
    auto txReverted = t.Leader->StartTransaction();
    t.LeaderState1->insert_or_assign(txReverted, 3, "?");
    t.Database.DebugDisconnect = true;
    EXPECT_THROW_WITH_SUBSTRING(txReverted->Commit(), "Debug disconnect");
    t.Database.DebugDisconnect = false;

    EXPECT_THROW_WITH_SUBSTRING(
        t.LeaderState1->contains(txCommitted, 3),
        "Operation over state has been already committed");
    EXPECT_THROW_WITH_SUBSTRING(
        t.LeaderState1->contains(txAborted, 3),
        "Operation over state has been cancelled");
    EXPECT_THROW_WITH_SUBSTRING(
        t.LeaderState1->contains(txReverted, 3),
        "Operation over state was failed to persist");

    EXPECT_THROW_WITH_SUBSTRING(
        t.LeaderState1->contains(t.TxReplica, 3),
        "Transaction object must be used within the same control in which it was created");
    EXPECT_THROW_WITH_SUBSTRING(
        t.ReplicaState1->insert_or_assign(t.TxReplica, 3, "x"),
        "Can't modify state: is either not leader or not recovered yet");
    t.Leader = nullptr;
    EXPECT_THROW_WITH_SUBSTRING(
        t.LeaderState1->StartTransaction(),
        "Persisted state control has been destroyed");
    EXPECT_THROW_WITH_SUBSTRING(
        t.Tx1->Commit(),
        "Persisted state control has been destroyed");
    EXPECT_THROW_WITH_SUBSTRING(
        txCommitted->Abort(),
        "Persisted state control has been destroyed");
    EXPECT_THROW_WITH_SUBSTRING(
        txAborted->Abort(),
        "Persisted state control has been destroyed");
    EXPECT_THROW_WITH_SUBSTRING(
        txReverted->Abort(),
        "Persisted state control has been destroyed");
}

//! Test various exceptions during recovery with corrupted data.
TEST(TPersistedStateTest, RecoveryException)
{
    {
        TTestDatabase db;
        db.DebugInjectFake1 = db.DebugInjectFake2 = true;
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Bad database: introducing fake sequence ID");
    }
    {
        TTestDatabase db;
        db.DebugInjectFake2 = true;
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Bad database: introducing fake sequence ID");
    }
    {
        TTestDatabase db;
        db.DebugInjectFake1 = true;
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Bad database: introducing fake sequence ID");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::Commit, "test", "", "0", "0"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Bad value record of persisted state: no key present");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::Commit, "test", "0", "1", "0"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Bad value record of persisted state: not a point record");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::Commit, "test", "0", "", "0"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "The first record in persisted state must start with minus infinity (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::Commit, "test", "0", "1", ""}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Even record in persisted state must have a value (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::Commit, "test", "1", "", "1"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "There's must be no gap in persisted before value (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::Commit, "test", "0", "", "1"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Unexpected end of records in persisted state: open interval expected (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::None, "test", "0", "", "1"}},
            {TSequenceId{3}, {TSequenceId{3}, EStorageRowFlags::Commit, "test", "1", "", "1"}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Odd record in persisted state must not have a value (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::None, "test", "0", "", "1"}},
            {TSequenceId{3}, {TSequenceId{3}, EStorageRowFlags::Commit, "test", "1", "2", ""}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "There's must be no gap before open interval in persisted state (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::None, "test", "0", "", "1"}},
            {TSequenceId{3}, {TSequenceId{3}, EStorageRowFlags::Commit, "test", "0", "-1", ""}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "Right boundary must be greater than left in persisted state (StateName: test)");
    }
    {
        TTestDatabase db;
        db.Data = {{TSequenceId{1}, {TSequenceId{1}, EStorageRowFlags::None, "test", "", "0", ""}},
            {TSequenceId{2}, {TSequenceId{2}, EStorageRowFlags::None, "test", "0", "", "1"}},
            {TSequenceId{3}, {TSequenceId{3}, EStorageRowFlags::Commit, "test", "0", "1", ""}}};
        auto control = New<TPersistedStateControl<std::string, std::string, TTestSerializer>>(New<TStorageHandler>(db));
        auto state = control->CreateState<int, std::string>("test");
        EXPECT_THROW_WITH_SUBSTRING(
            control->Recover(),
            "The last record in persisted state must end with plus infinity (StateName: test)");
    }
}

//! Test that check big transaction with several statements and different operations.
TEST(TPersistedStateTest, ComplexTransaction)
{
    std::vector<int> possibleKeys1;
    std::vector<std::string> possibleKeys2;
    for (int i = 1; i < 10; i++) {
        possibleKeys1.push_back(i);
        possibleKeys2.push_back(std::to_string(i));
    }
    std::unordered_map<int, std::string> initial1;
    std::unordered_map<std::string, int> initial2;
    for (int i = 2; i < 10; i += 2) {
        initial1.emplace(i, std::to_string(i));
        initial2.emplace(std::to_string(i), i);
    }
    auto modify = [] (auto&& state1, auto&& state2) {
        state1.erase(2);
        state2.insert_or_assign("8", 108);
        for (int i = 1; i < 10; i += 2) {
            state1.emplace(i, std::to_string(i));
            state2.emplace(std::to_string(i), i);
        }
        state1.insert_or_assign(4, "4!");
        state2.erase("4");
    };
    std::unordered_map<int, std::string> final1 = initial1;
    std::unordered_map<std::string, int> final2 = initial2;
    modify(final1, final2);

    TTester t(initial1, initial2);
    t.FinalCheckContent(initial1, initial2);
    auto tx = t.Leader->StartTransaction();
    TTransactionalPersistedState state1(t.LeaderState1, tx);
    TTransactionalPersistedState state2(t.LeaderState2, tx);
    modify(state1, state2);
    tx->Commit();
    t.FinalCheckContent(final1, final2);

    auto copyHandler = New<TStorageHandler>(t.Database);
    auto testControl = New<TPersistedStateControl<TTester::TDBKey, TTester::TDBValue, TTester::TSerializer>>(copyHandler);
    auto testState1 = testControl->CreateState<int, std::string>("test1");
    auto testState2 = testControl->CreateState<std::string, int>("test2");
    testControl->Recover();
    CheckStateContents(testState1, final1, possibleKeys1);
    CheckStateContents(testState2, final2, possibleKeys2);
}

//! Test handling various fault during DB write.
TEST(TPersistedStateTest, DbTransactionFailure)
{
    using TOnInsertState1 = TPersistedState<int, std::string>::TOnInsert;
    using TOnEraseState1 = TPersistedState<int, std::string>::TOnErase;
    using TOnInsertState2 = TPersistedState<std::string, int>::TOnInsert;
    using TOnEraseState2 = TPersistedState<std::string, int>::TOnErase;

    std::vector<int> possibleKeys1;
    std::vector<std::string> possibleKeys2;
    for (int i = 1; i < 10; i++) {
        possibleKeys1.push_back(i);
        possibleKeys2.push_back(std::to_string(i));
    }
    std::unordered_map<int, std::string> initial1;
    std::unordered_map<std::string, int> initial2;
    for (int i = 2; i < 10; i += 2) {
        initial1.emplace(i, std::to_string(i));
        initial2.emplace(std::to_string(i), i);
    }
    auto modify = [] (auto&& state1, auto&& state2) {
        state1.erase(2);
        state2.insert_or_assign("8", 108);
        for (int i = 1; i < 10; i += 2) {
            state1.emplace(i, std::to_string(i));
            state2.emplace(std::to_string(i), i);
        }
        state1.insert_or_assign(4, "4!");
        state2.erase("4");
    };
    std::unordered_map<int, std::string> final1 = initial1;
    std::unordered_map<std::string, int> final2 = initial2;
    modify(final1, final2);

    struct TSubscribeLog
    {
        std::unordered_map<int, int> InsertState1;
        std::unordered_map<int, int> EraseState1;
        std::unordered_map<std::string, int> InsertState2;
        std::unordered_map<std::string, int> EraseState2;
        bool operator==(const TSubscribeLog&) const = default;

        void clear()
        {
            InsertState1.clear();
            EraseState1.clear();
            InsertState2.clear();
            EraseState2.clear();
        }
    } subscribeLog;

    auto onInsertState1 = [&] (int key) {
        subscribeLog.InsertState1[key]++;
    };
    auto onEraseState1 = [&] (int key) {
        subscribeLog.EraseState1[key]++;
    };
    auto onInsertState2 = [&] (const std::string& key) {
        subscribeLog.InsertState2[key]++;
    };
    auto onEraseState2 = [&] (const std::string& key) {
        subscribeLog.EraseState2[key]++;
    };
    auto subscribe = [&] (auto& state1, auto& state2) {
        subscribeLog.clear();
        auto cookie1 = state1->Subscribe(TOnInsertState1{onInsertState1}, TOnEraseState1{onEraseState1});
        auto cookie2 = state2->Subscribe(TOnInsertState2{onInsertState2}, TOnEraseState2{onEraseState2});
        return std::pair(cookie1, cookie2);
    };
    auto unsubscribe = [&] (auto& state1, auto& state2, auto cookies) {
        state1->Unsubscribe(cookies.first);
        state2->Unsubscribe(cookies.second);
    };

    // The numbers in the block below are calculated manually.
    constexpr ssize_t neededInsertionCount = 30;
    constexpr ssize_t neededDeletionCount = 14;
    TSubscribeLog testSubscribeLog;
    testSubscribeLog.InsertState1 = {{1, 1}, {3, 1}, {5, 1}, {7, 1}, {9, 1}, {4, 1}};
    testSubscribeLog.EraseState1 = {{2, 1}, {4, 1}};
    testSubscribeLog.InsertState2 = {{"1", 1}, {"3", 1}, {"5", 1}, {"7", 1}, {"9", 1}, {"8", 1}};
    testSubscribeLog.EraseState2 = {{"8", 1}, {"4", 1}};

    TTester tmp(initial1, initial2);
    auto initialSequenceId = tmp.Leader->GetSequenceId();
    auto initialSequenceData = tmp.Leader->Follow(TSequenceId{0}, 100500);
    auto initialDatabaseData = tmp.Database.Data;
    auto initialSubscribeLog = subscribeLog;
    {
        auto cookies = subscribe(tmp.LeaderState1, tmp.LeaderState2);
        auto tx = tmp.Leader->StartTransaction();
        modify(TTransactionalPersistedState(tmp.LeaderState1, tx), TTransactionalPersistedState(tmp.LeaderState2, tx));
        tx->Commit();
        unsubscribe(tmp.LeaderState1, tmp.LeaderState2, cookies);
    }
    auto finalSequenceId = tmp.Leader->GetSequenceId();
    auto finalSequenceData = tmp.Leader->Follow(TSequenceId{0}, 100500);
    auto finalDatabaseData = tmp.Database.Data;
    auto finalSubscribeLog = subscribeLog;
    EXPECT_NE(initialSequenceId, finalSequenceId);
    EXPECT_NE(initialSequenceData, finalSequenceData);
    EXPECT_NE(initialDatabaseData, finalDatabaseData);
    EXPECT_NE(initialSubscribeLog, finalSubscribeLog);
    EXPECT_EQ(finalSubscribeLog, testSubscribeLog);

    for (ssize_t maxExecuteSize : {7, 9, 10, 11, 30, 35, 50}) {
        const ssize_t neededInsertionTransactions = (neededInsertionCount + maxExecuteSize - 1) / maxExecuteSize;
        const ssize_t neededDeletionTransactions = (neededDeletionCount + maxExecuteSize - 1) / maxExecuteSize;
        const ssize_t maxTestFailureCountdown = neededInsertionTransactions + neededDeletionTransactions;
        for (bool failureBeforeActualCommit : {true, false}) {
            bool allCasesMet = false;
            for (ssize_t failureCountdown = 0; failureCountdown <= maxTestFailureCountdown; failureCountdown++) {
                TTester t(initial1, initial2);
                t.FinalCheckContent(initial1, initial2);

                t.Database.InsertionCount = t.Database.DeletionCount = 0;
                if (failureBeforeActualCommit) {
                    t.Database.ExecuteActionsBeforeFailure = failureCountdown;
                } else {
                    t.Database.ExecuteActionsBeforeCommittedFailure = failureCountdown;
                }
                t.StorageHandler->DebugAlterMaxExecuteSize(maxExecuteSize);
                subscribe(t.LeaderState1, t.LeaderState2);

                auto tx = t.Leader->StartTransaction();
                TTransactionalPersistedState state1(t.LeaderState1, tx);
                TTransactionalPersistedState state2(t.LeaderState2, tx);
                modify(state1, state2);

                if (failureCountdown < neededInsertionTransactions) {
                    EXPECT_THROW_WITH_SUBSTRING(tx->Commit(), "Countdown failure");
                    if (failureBeforeActualCommit) {
                        EXPECT_EQ(t.Database.InsertionCount, failureCountdown * maxExecuteSize);
                    } else {
                        EXPECT_EQ(t.Database.InsertionCount, std::min(neededInsertionCount, (failureCountdown + 1) * maxExecuteSize));
                    }
                    EXPECT_EQ(t.Database.DeletionCount, 0);

                    t.FinalCheckContent(initial1, initial2);
                    EXPECT_EQ(t.Leader->GetSequenceId(), initialSequenceId);
                    auto currentSequenceData = t.Leader->Follow(TSequenceId{0}, 100500);
                    EXPECT_EQ(currentSequenceData, initialSequenceData);
                    if (failureCountdown == 0 && failureBeforeActualCommit) {
                        EXPECT_EQ(t.Database.Data, initialDatabaseData);
                    }
                    // The database may contain (definitely contains if failureCountdown != 0) uncommitted garbage now.
                } else if (failureCountdown < neededInsertionTransactions + neededDeletionTransactions) {
                    tx->Commit();
                    EXPECT_EQ(t.Database.InsertionCount, neededInsertionCount);
                    if (failureBeforeActualCommit) {
                        EXPECT_EQ(t.Database.DeletionCount, (failureCountdown - neededInsertionTransactions) * maxExecuteSize);
                    } else {
                        EXPECT_EQ(t.Database.DeletionCount, std::min(neededDeletionCount, (failureCountdown - neededInsertionTransactions + 1) * maxExecuteSize));
                    }

                    t.FinalCheckContent(final1, final2);
                    EXPECT_EQ(t.Leader->GetSequenceId(), finalSequenceId);
                    auto currentSequenceData = t.Leader->Follow(TSequenceId{0}, 100500);
                    EXPECT_EQ(currentSequenceData, finalSequenceData);
                    // The database contains undeleted garbage now.
                } else {
                    tx->Commit();
                    EXPECT_EQ(t.Database.InsertionCount, neededInsertionCount);
                    EXPECT_EQ(t.Database.DeletionCount, neededDeletionCount);

                    t.FinalCheckContent(final1, final2);
                    EXPECT_EQ(t.Leader->GetSequenceId(), finalSequenceId);
                    auto currentSequenceData = t.Leader->Follow(TSequenceId{0}, 100500);
                    EXPECT_EQ(currentSequenceData, finalSequenceData);
                    EXPECT_EQ(t.Database.Data, finalDatabaseData);
                    allCasesMet = true;
                }

                // The database may contain garbage. Check that it is properly collected.
                ssize_t expectedGarbageSize = 0;
                if (failureCountdown < neededInsertionTransactions) {
                    // The garbage is that what was inserted but not finalized.
                    if (failureBeforeActualCommit) {
                        expectedGarbageSize = failureCountdown * maxExecuteSize;
                    } else {
                        expectedGarbageSize = std::min(neededInsertionCount, (failureCountdown + 1) * maxExecuteSize);
                    }
                } else if (failureCountdown < neededInsertionTransactions + neededDeletionTransactions) {
                    // The garbage is that what was not deleted after finalization.
                    if (failureBeforeActualCommit) {
                        expectedGarbageSize = neededDeletionCount - (failureCountdown - neededInsertionTransactions) * maxExecuteSize;
                    } else {
                        expectedGarbageSize = std::max(ssize_t{0}, neededDeletionCount - (failureCountdown - neededInsertionTransactions + 1) * maxExecuteSize);
                    }
                }
                if (expectedGarbageSize > 0) {
                    EXPECT_NE(t.Database.Data, initialDatabaseData);
                    EXPECT_NE(t.Database.Data, finalDatabaseData);
                }

                bool expectedSuccess = failureCountdown >= neededInsertionTransactions;
                const auto& expected1 = expectedSuccess ? final1 : initial1;
                const auto& expected2 = expectedSuccess ? final2 : initial2;
                auto expectedSequenceId = expectedSuccess ? finalSequenceId : initialSequenceId;
                const auto& expectedSequenceData = expectedSuccess ? finalSequenceData : initialSequenceData;
                const auto& expectedDatabaseData = expectedSuccess ? finalDatabaseData : initialDatabaseData;
                const auto& expectedSubscribeLog = expectedSuccess ? finalSubscribeLog : initialSubscribeLog;

                EXPECT_EQ(std::ssize(t.Database.Data), std::ssize(expectedDatabaseData) + expectedGarbageSize);
                t.FinalCheckContent(expected1, expected2);
                EXPECT_EQ(t.Leader->GetSequenceId(), expectedSequenceId);
                auto currentSequenceData = t.Leader->Follow(TSequenceId{0}, 100500);
                EXPECT_EQ(currentSequenceData, expectedSequenceData);
                EXPECT_EQ(subscribeLog, expectedSubscribeLog);

                bool recoveryExpectedCommitted = (failureCountdown + !failureBeforeActualCommit) >= neededInsertionTransactions;
                const auto& recoveryExpected1 = recoveryExpectedCommitted ? final1 : initial1;
                const auto& recoveryExpected2 = recoveryExpectedCommitted ? final2 : initial2;
                auto recoveryExpectedSequenceId = recoveryExpectedCommitted ? finalSequenceId : initialSequenceId;
                const auto& recoveryExpectedSequenceData = recoveryExpectedCommitted ? finalSequenceData : initialSequenceData;
                const auto& recoveryExpectedDatabaseData = recoveryExpectedCommitted ? finalDatabaseData : initialDatabaseData;
                for (bool noMoreErrors : {false, true}) {
                    // Check that recover from database with garbage will not prevent correct recover.
                    auto copyDb = t.Database.Clone();
                    copyDb.ExecuteActionsBeforeFailure = std::numeric_limits<ssize_t>::max();
                    copyDb.ExecuteActionsBeforeCommittedFailure = std::numeric_limits<ssize_t>::max();
                    auto copyHandler = New<TStorageHandler>(copyDb);
                    copyHandler->DebugAlterMaxExecuteSize(maxExecuteSize);
                    auto testControl = New<TPersistedStateControl<TTester::TDBKey, TTester::TDBValue, TTester::TSerializer>>(copyHandler);
                    auto testState1 = testControl->CreateState<int, std::string>("test1");
                    auto testState2 = testControl->CreateState<std::string, int>("test2");
                    testControl->Recover();
                    CheckStateContents(testState1, recoveryExpected1, possibleKeys1);
                    CheckStateContents(testState2, recoveryExpected2, possibleKeys2);
                    EXPECT_EQ(testControl->GetSequenceId(), recoveryExpectedSequenceId);
                    auto currentSequenceData = testControl->Follow(TSequenceId{0}, 100500);
                    EXPECT_EQ(currentSequenceData, recoveryExpectedSequenceData);

                    // Check that if database is inaccessible then any commit does not change anything.
                    copyDb.ExecuteActionsBeforeFailure = 0;
                    EXPECT_THROW_WITH_SUBSTRING(testControl->StartTransaction()->Commit(), "Countdown failure");
                    EXPECT_EQ(copyDb.Data, t.Database.Data);

                    auto testTx = testControl->StartTransaction();
                    testState1->insert_or_assign(testTx, 0, "0");
                    EXPECT_THROW_WITH_SUBSTRING(testTx->Commit(), "Countdown failure");
                    EXPECT_EQ(copyDb.Data, t.Database.Data);

                    // Check that the garbage is deleted at the first successful transaction.
                    if (noMoreErrors) {
                        // Case when there are no more database error happens.
                        copyDb.ExecuteActionsBeforeFailure = std::numeric_limits<ssize_t>::max();
                        testControl->StartTransaction()->Commit();
                        EXPECT_EQ(copyDb.Data, recoveryExpectedDatabaseData);
                    } else {
                        // Case when the database is still periodically inaccessible.
                        ssize_t garbageSize = std::ssize(copyDb.Data) - std::ssize(recoveryExpectedDatabaseData);
                        while (garbageSize > maxExecuteSize) {
                            copyDb.ExecuteActionsBeforeFailure = 1;
                            EXPECT_THROW_WITH_SUBSTRING(testControl->StartTransaction()->Commit(), "Countdown failure");
                            garbageSize -= maxExecuteSize;
                            EXPECT_EQ(std::ssize(copyDb.Data), std::ssize(recoveryExpectedDatabaseData) + garbageSize);
                        }
                        copyDb.ExecuteActionsBeforeFailure = 2; // 1 for garbage collection + 1 for the empty transaction.
                        testControl->StartTransaction()->Commit();
                        EXPECT_EQ(copyDb.Data, recoveryExpectedDatabaseData);
                    }
                }

                // Check that the garbage is deleted at the first successful transaction.
                t.Database.ExecuteActionsBeforeFailure = std::numeric_limits<ssize_t>::max();
                t.Database.ExecuteActionsBeforeCommittedFailure = std::numeric_limits<ssize_t>::max();
                t.Leader->StartTransaction()->Commit();
                EXPECT_EQ(t.Database.Data, expectedDatabaseData);
            }
            EXPECT_TRUE(allCasesMet);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

//! RacyTester is a brute force checker for PersistedState.
//! It runs one or more procedures that modifies persisted state in multiple threads, and runs check procedure (including on replicas and on persisted state recovered from db at random times).
//! It also runs in a multistate control where each state has different probability of being taken in each iteration.
//! It also allows to emulate random failures of DB with different probabilities.
class TRacyTester
{
public:
    static constexpr std::array<double, 9> StateProbability = {0.95, 0.7, 0.5, 0.5, 0.5, 0.5, 0.5, 0.2, 0.05};
    static constexpr i64 StateCount = StateProbability.size();

    static constexpr std::array<double, 8> DbFailProbability = {0.001, 0.005, 0.01, 0.05, 0.1, 0.3, 0.8, 0.95};

    static constexpr i64 ExecThreads = 8;
    static constexpr i64 CheckThreads = 3;

    using TDBKey = std::string;
    using TDBValue = std::string;
    using TSerializer = TTestSerializer;

    using TKey = int;
    using TValue = int;

    using TFunction = std::function<void(TIntrusivePtr<TPersistedState<TKey, TValue>>)>;
    using TTransactionalFunction = std::function<void(TIntrusivePtr<TPersistedState<TKey, TValue>>, TPersistedStateTransactionPtr)>;

    int RunCount;
    TFunction Init;
    std::vector<TTransactionalFunction> Functions;
    std::optional<TTransactionalFunction> CheckFunction;
    TTestDatabase Database;
    TIntrusivePtr<TStorageHandler> StorageHandler;
    NConcurrency::IFairShareThreadPoolPtr ThreadPool;
    IInvokerPtr Invoker;

    TRacyTester(int runCount, TFunction init, std::vector<TTransactionalFunction> functions, std::optional<TTransactionalFunction> checkFunction = std::nullopt)
        : RunCount(runCount)
        , Init(init)
        , Functions(functions)
        , CheckFunction(checkFunction)
    {
        ThreadPool = NConcurrency::CreateFairShareThreadPool(25, "UT");
        Invoker = ThreadPool->GetInvoker("UTInvoker");
    }

    void Execute()
    {
        for (const double dbFailProbability : DbFailProbability) {
            TTestDatabase Database;
            StorageHandler = New<TStorageHandler>(Database);

            TPersistedStateControlPtr<TDBKey, TDBValue, TSerializer> leader = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>(StorageHandler);
            std::vector<TPersistedStatePtr<int, int>> states(StateCount);
            TPersistedStateControlPtr<TDBKey, TDBValue, TSerializer> replica = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>(nullptr);
            std::vector<TPersistedStatePtr<int, int>> replicaStates(StateCount);

            for (int i = 0; i < StateCount; ++i) {
                states[i] = leader->CreateState<TKey, TValue>("test" + std::to_string(i));
            }
            for (int i = 0; i < StateCount; ++i) {
                replicaStates[i] = replica->CreateState<TKey, TValue>("test" + std::to_string(i));
            }
            leader->Recover();
            for (int i = 0; i < StateCount; ++i) {
                Init(states[i]);
            }
            auto rows = leader->Follow(TSequenceId{0});
            replica->Apply(rows, nullptr);

            Database.RandomExecuteFailProbability = dbFailProbability;
            std::vector<TFuture<void>> futures;

            for (const auto& function : Functions) {
                auto proc = [&] () {
                    for (int run = 0; run < RunCount; ++run) {
                        auto tx = leader->StartTransaction();

                        for (int i = 0; i < StateCount; ++i) {
                            if (RandomNumber<double>() < StateProbability[i]) {
                                function(states[i], tx);
                            }
                        }

                        try {
                            tx->Commit();
                        } catch (const std::exception& e) {
                        }
                    }
                };

                for (int i = 0; i < ExecThreads; ++i) {
                    futures.push_back(BIND(proc).AsyncVia(Invoker).Run());
                }
            }

            TPromise<void> mainStopped = NewPromise<void>();
            TFuture<void> mainStoppedFuture = mainStopped.ToFuture();
            std::vector<TFuture<void>> secondWaveFutures;

            if (CheckFunction) {
                auto checkFunction = *CheckFunction;

                auto dbCheckProc = [&] {
                    auto db = Database.Clone();
                    db.RandomExecuteFailProbability = 0;
                    auto handler = New<TStorageHandler>(db);
                    TPersistedStateControlPtr<TDBKey, TDBValue, TSerializer> checkLeader = New<TPersistedStateControl<TDBKey, TDBValue, TSerializer>>(handler);
                    std::vector<TPersistedStatePtr<int, int>> checkStates(StateCount);
                    for (int i = 0; i < StateCount; ++i) {
                        checkStates[i] = checkLeader->CreateState<TKey, TValue>("test" + std::to_string(i));
                    }

                    checkLeader->Recover();
                    auto tx = checkLeader->StartTransaction();

                    for (int i = 0; i < StateCount; ++i) {
                        (*CheckFunction)(checkStates[i], tx);
                    }
                    tx->Commit();
                    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10 + RandomNumber<ui64>(90)));
                };

                auto checkProc = [&] (int threadNum) {
                    while (true) {
                        if (threadNum == 0) {
                            auto rows = leader->Follow(replica->GetSequenceId(), std::numeric_limits<ssize_t>::max());
                            replica->Apply(rows, nullptr);
                        }

                        auto tx = replicaStates[0]->StartTransaction();

                        for (int i = 0; i < StateCount; ++i) {
                            if (RandomNumber<double>() < StateProbability[i]) {
                                (*CheckFunction)(replicaStates[i], tx);
                            }
                        }

                        try {
                            tx->Commit();
                        } catch (const std::exception&) {
                        }

                        if (mainStoppedFuture.IsSet()) {
                            break;
                        }
                        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10 + RandomNumber<ui64>(90)));
                    }
                };
                for (int i = 0; i < CheckThreads; ++i) {
                    secondWaveFutures.push_back(BIND(checkProc, i).AsyncVia(Invoker).Run());
                }
                secondWaveFutures.push_back(BIND(dbCheckProc).AsyncVia(Invoker).Run());
            }

            NConcurrency::WaitFor(AllSucceeded(futures)).ThrowOnError();
            mainStopped.Set();
            NConcurrency::WaitFor(AllSucceeded(secondWaveFutures)).ThrowOnError();
        }
    }
};

TEST(TPersistedStateRacyTest, RacyMoveSingleValue)
{
    auto init = [] (auto state) {
        state->insert_or_assign(0, 1);
    };

    auto proc = [] (auto state, auto tx) {
        auto it = state->begin(tx);
        int key = it->first;
        for (int i = 1; i <= 2 * 10; ++i) {
            EXPECT_FALSE(state->contains(tx, key + i));
        }
        for (int i = 1; i <= 2 * 10; ++i) {
            EXPECT_FALSE(state->contains(tx, key - i));
        }
        state->erase(tx, key);
        state->insert_or_assign(tx, key + 1, GetSequentialThreadId());
    };

    auto checkProc = [] (auto state, auto tx) {
        ASSERT_EQ(state->size(tx), 1ull);
    };

    TRacyTester tester(200, init, {proc}, checkProc);
    tester.Execute();
}

TEST(TPersistedStateRacyTest, RacyMove10Values)
{
    auto init = [] (auto state) {
        for (int i = 0; i < 10; ++i) {
            state->insert_or_assign(i, i);
        }
    };

    auto proc = [] (auto state, auto tx) {
        ASSERT_EQ(state->size(tx), 10ull);
        auto it = state->begin(tx);
        int key = it->first;
        int expLast = key + 9;
        int diff = RandomNumber<ui32>(4) + 1;
        state->TrackRead(tx);

        for (int j = 0; j < diff; ++j) {
            state->erase(tx, key + j);
            state->insert_or_assign(tx, expLast + 1 + j, GetSequentialThreadId());
        }
    };

    auto checkProc = [] (auto state, auto tx) {
        auto it = state->begin(tx);
        int key = it->first;
        int expLast = key + 9;
        EXPECT_TRUE(!state->contains(tx, expLast + 1));
        EXPECT_EQ(state->size(tx), 10u);

        for (int i = 0; i < 10; ++i) {
            EXPECT_TRUE(state->contains(tx, key + i));
        }
    };

    TRacyTester tester(100, init, {proc}, checkProc);
    tester.Execute();
}

TEST(TPersistedStateRacyTest, TrulyRandomTest)
{
    auto init = [] (auto) {
    };

    auto proc = [] (auto state, auto tx) {
        int maxKey = 14;
        int actions = RandomNumber<ui32>(20);
        for (int i = 0; i < actions; ++i) {
            int key = RandomNumber<ui32>(maxKey);
            double whatToDo = RandomNumber<double>();
            if (whatToDo < 0.3) {
                state->insert_or_assign(tx, key, GetSequentialThreadId());
            } else if (whatToDo < 0.6) {
                state->erase(tx, key);
            } else {
                state->emplace(tx, key, GetSequentialThreadId());
            }
        }
    };

    TRacyTester tester(100, init, {proc});
    tester.Execute();
}

//! Invariant is that sum of keys is a multiple of 4.
TEST(TPersistedStateRacyTest, ComplexInvariantTest)
{
    auto init = [] (auto) {
    };

    auto proc = [] (auto state, auto tx) {
        int maxKey = 50;
        int currentSum = 0;
        for (auto it = state->begin(tx); it != state->end(tx); ++it) {
            currentSum += it->first;
        }

        do {
            int key = RandomNumber<ui32>(maxKey);
            if (state->contains(tx, key)) {
                state->erase(tx, key);
                currentSum -= key;
            } else {
                state->insert_or_assign(tx, key, GetSequentialThreadId());
                currentSum += key;
            }
        } while (currentSum % 4 != 0);
    };

    auto checkProc = [] (auto state, auto tx) {
        int currentSum = 0;
        for (auto it = state->begin(tx); it != state->end(tx); ++it) {
            currentSum += it->first;
        }
        EXPECT_EQ(currentSum % 4, 0);
    };

    TRacyTester tester(100, init, {proc}, checkProc);
    tester.Execute();
}

//! Invariant is all keys from 100 to 110 are either present or missing.
TEST(TPersistedStateRacyTest, RacyInsertDelete)
{
    auto init = [] (auto) {
    };

    auto taskInsert = [] (auto state, auto tx) {
        for (int i = 100; i < 110; i++) {
            state->insert_or_assign(tx, i, GetSequentialThreadId());
        }
    };

    auto taskRemove = [] (auto state, auto tx) {
        std::vector<int> keys;
        for (auto it = state->begin(tx); it != state->end(tx); ++it) {
            keys.push_back(it->first);
        }
        for (const auto& key : keys) {
            state->erase(tx, key);
        }
    };

    auto checkProc = [] (auto state, auto tx) {
        auto size = state->size(tx);
        EXPECT_TRUE(size == 0 || size == 10);
        if (size == 10) {
            auto first = state->begin(tx);
            auto expectedLast = state->find(tx, 109);
            EXPECT_TRUE(expectedLast != state->end(tx));
            EXPECT_TRUE(first->first == 100 && expectedLast->first == 109);
            expectedLast++;
            EXPECT_TRUE(expectedLast == state->end(tx));
        }
    };

    TRacyTester tester(15, init, {taskInsert, taskRemove}, checkProc);
    tester.Execute();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
