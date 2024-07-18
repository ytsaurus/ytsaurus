#include "sorted_store_manager_ut_helpers.h"

#include <yt/yt/server/lib/tablet_node/public.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/utilex/random.h>

#include <library/cpp/yt/threading/public.h>

#include <random>

namespace NYT::NTabletNode {
namespace {

using namespace NConcurrency;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManagerStressTest
    : public WithParamInterface<int>
    , public TSortedStoreManagerTestBase
{
public:
    using TSelf = TSortedStoreManagerStressTest;

    void SetUp() override
    {
        TSortedStoreManagerTestBase::SetUp();

        auto seed = static_cast<int>(std::chrono::high_resolution_clock::now().time_since_epoch().count());
        Cerr << Format("Running with seed %v", seed) << Endl;

        Rng = std::mt19937(seed);
    }

    TTableSchemaPtr GetSchema() const override
    {
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("x", EValueType::Int64),
            TColumnSchema("y", EValueType::Int64)
                .SetLock("l_y"),
            TColumnSchema("z", EValueType::Int64)
                .SetLock("l_z"),
        });
    }

    // Common stuff.

    void RunTest();

    std::mt19937 Rng;

    using TKey = int;

    static constexpr int ValueCount = 3;

    struct TRow
    {
        TKey Key;

        std::optional<int> X;
        std::optional<int> Y;
        std::optional<int> Z;

        bool operator == (const TRow& other) const
        {
            return
                Key == other.Key &&
                X == other.X &&
                Y == other.Y &&
                Z == other.Z;
        }

        std::optional<int>& GetValue(int index)
        {
            switch (index) {
                case 0:
                    return X;
                case 1:
                    return Y;
                case 2:
                    return Z;
                default:
                    YT_ABORT();
            }
        }

        TUnversionedOwningRow ToRow()
        {
            TUnversionedOwningRowBuilder rowBuilder;
            rowBuilder.AddValue(MakeUnversionedInt64Value(Key, 0));
            for (int index = 0; index < ValueCount; ++index) {
                if (auto value = GetValue(index)) {
                    rowBuilder.AddValue(MakeUnversionedInt64Value(*value, index + 1));
                }
            }

            return rowBuilder.FinishRow();
        }

        TLegacyOwningKey ToKey()
        {
            TUnversionedOwningRowBuilder rowBuilder;
            rowBuilder.AddValue(MakeUnversionedInt64Value(Key, 0));

            return rowBuilder.FinishRow();
        }

        static std::optional<TRow> FromRow(TUnversionedRow row)
        {
            if (!row) {
                return std::nullopt;
            }

            TRow result;
            for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                auto value = row[index];
                if (value.Type != EValueType::Int64) {
                    YT_VERIFY(value.Type == EValueType::Null);
                    continue;
                }

                if (value.Id == 0) {
                    result.Key = value.Data.Int64;
                } else {
                    result.GetValue(value.Id - 1) = value.Data.Int64;
                }
            }

            return result;
        }
    };

    // Lookup stuff.

    using TValueFilter = std::array<bool, ValueCount>;

    struct TLookupRequest
    {
        TKey Key;
        TValueFilter ValueFilter;
        TTimestamp Timestamp;
        TGuid RequestId;
    };

    struct TLookupResult
    {
        std::optional<TRow> Row;
        bool Locked = false;
    };

    class TLookuper
        : public TRefCounted
    {
    public:
        TLookuper(
            TSortedStoreManagerStressTest* owner,
            TLookupRequest request,
            bool async)
            : Owner_(owner)
            , Request_(std::move(request))
            , TabletSnapshot_(Owner_->Tablet_->BuildSnapshot(nullptr))
            , Async_(async)
            , Wait_(Owner_->Rng() % 3)
        { }

        void Run()
        {
            if (Async_) {
                Owner_->LookupInvoker_->Invoke(BIND(&TLookuper::DoLookup, MakeStrong(this)));
            } else {
                DoLookup();
            }
        }

        bool IsCompleted() const
        {
            return Result_.IsSet();
        }

        const TLookupRequest& GetRequest() const
        {
            return Request_;
        }

        TFuture<TLookupResult> GetResult() const
        {
            return Result_.ToFuture();
        }

    private:
        TSortedStoreManagerStressTest* Owner_;
        const TLookupRequest Request_;
        const TTabletSnapshotPtr TabletSnapshot_;
        const IInvokerPtr Invoker_;
        const bool Async_;
        const bool Wait_;

        const TPromise<TLookupResult> Result_ = NewPromise<TLookupResult>();

        void DoLookup()
        {
            try {
                GuardedLookup();
            } catch (const TErrorException& ex) {
                Result_.Set(ex.Error());
            } catch (const TFiberCanceledException&) {
                Result_.Set(TError(NYT::EErrorCode::Canceled, "Canceled"));
            } catch (...) {
                Result_.Set(TError("Unknown error"));
            }
        }

        void GuardedLookup()
        {
            if (Async_ && Wait_) {
                TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::MilliSeconds(10)));
            }
            auto key = TRow{.Key = Request_.Key}.ToKey();

            std::vector<int> columnFilter;
            columnFilter.push_back(0);
            for (int index = 0; index < ValueCount; ++index) {
                if (Request_.ValueFilter[index]) {
                    columnFilter.push_back(index + 1);
                }
            }

            auto row = Owner_->LookupRow(
                key,
                Request_.Timestamp,
                std::move(columnFilter),
                TabletSnapshot_);
            if (!row) {
                Result_.Set(TLookupResult{
                    .Row = std::nullopt,
                    .Locked = false
                });
                return;
            }

            TLookupResult result{
                .Row = TRow(),
                .Locked = false
            };
            for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                auto value = row[index];
                if (value.Type != EValueType::Int64) {
                    continue;
                }
                auto id = columnFilter[value.Id];
                if (id == 0) {
                    result.Row->Key = value.Data.Int64;
                } else {
                    result.Row->GetValue(id - 1) = value.Data.Int64;
                }
            }
            Result_.Set(result);
        }

        void SetResult(const TLookupResult& result)
        {
            Result_.Set(result);

            auto guard = Guard(Owner_->LookupResultsLock_);
            Owner_->LookupResults_.emplace_back(Request_, result);
        }
    };
    using TLookuperPtr = TIntrusivePtr<TLookuper>;
    THashSet<TLookuperPtr> Lookupers;

    TAdaptiveLock LookupResultsLock_;
    std::vector<std::pair<TLookupRequest, TLookupResult>> LookupResults_;

    const TActionQueuePtr LookupThread_ = New<TActionQueue>("TestLookup");
    TCancelableContextPtr LookupContext_ = New<TCancelableContext>();
    IInvokerPtr LookupInvoker_;

    void ResetLookupInvoker()
    {
        LookupContext_->Cancel(TError("Reset"));
        LookupContext_ = New<TCancelableContext>();
        LookupInvoker_ = LookupContext_->CreateInvoker(LookupThread_->GetInvoker());
    }

    // Transaction stuff.

    struct TModification
    {
        TSelf* Self;

        //! Transaction owning this action.
        TTestTransaction* Transaction = nullptr;

        //! Was #ModifyRow called for this action?
        bool Applied = false;

        //! Was #ConfirmRow called for this action?
        bool Confirmed = false;

        //! Row reference obtained by #ModifyRow call.
        TSortedDynamicRowRef RowRef;

        //! Modification type.
        std::optional<ERowModificationType> Type;

        //! Row (for Write and LockAndWrite commands) or key (for Delete command).
        TRow Row;

        //! Lock mask (for LockAndWrite command).
        TLockMask LockMask;

        //! Returns true if locks were acquired.
        bool Apply(bool prelock = true)
        {
            switch (*Type) {
                case ERowModificationType::Write:
                    RowRef = Self->WriteRow(Transaction, Row.ToRow(), prelock);
                    break;
                case ERowModificationType::WriteAndLock:
                    RowRef = Self->WriteAndLockRow(Transaction, Row.ToRow(), LockMask, prelock);
                    break;
                case ERowModificationType::Delete:
                    RowRef = Self->DeleteRow(Transaction, Row.ToKey(), prelock);
                    break;
                default:
                    YT_ABORT();
            }

            Applied = static_cast<bool>(RowRef);
            return Applied;
        }

        void Confirm()
        {
            Self->ConfirmRow(Transaction, RowRef);
            Confirmed = true;
        }

        void Prepare()
        {
            Self->PrepareRow(Transaction, RowRef);
        }

        void Commit()
        {
            switch (*Type) {
                case ERowModificationType::Write: {
                    auto row = Row.ToRow();
                    TWriteRowCommand command{
                        .Row = row
                    };
                    Self->CommitRow(Transaction, command, RowRef);
                    break;
                }
                case ERowModificationType::Delete: {
                    auto row = Row.ToRow();
                    TDeleteRowCommand command{
                        .Row = row
                    };
                    Self->CommitRow(Transaction, command, RowRef);
                    break;
                }
                case ERowModificationType::WriteAndLock: {
                    auto row = Row.ToRow();
                    TWriteAndLockRowCommand command{
                        .Row = row,
                        .LockMask = LockMask
                    };
                    Self->CommitRow(Transaction, command, RowRef);
                    break;
                }
                default:
                    YT_ABORT();
            }
        }

        void Abort()
        {
            if (Applied) {
                Self->AbortRow(Transaction, RowRef);
            }
        }

        void Reset()
        {
            Applied = false;
            RowRef = {};
        }

        TKey GetKey() const
        {
            return Row.Key;
        }

        TLockMask GetLockMask()
        {
            TLockMask lockMask = LockMask;
            switch (*Type) {
                case ERowModificationType::Write:
                case ERowModificationType::WriteAndLock: {
                    for (int index = 0; index < ValueCount; ++index) {
                        if (Row.GetValue(index)) {
                            lockMask.Set(index, ELockType::Exclusive);
                        }
                    }
                    break;
                }
                case ERowModificationType::Delete: {
                    lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
                    break;
                }
                default:
                    YT_ABORT();
            }

            return lockMask;
        }
    };

    struct TTransactionState
    {
        TSelf* Self;

        std::unique_ptr<TTestTransaction> Transaction;

        std::vector<std::unique_ptr<TModification>> Modifications;

        std::optional<TTimestamp> GeneratedPrepareTimestamp;
        std::optional<TTimestamp> GeneratedCommitTimestamp;

        int PrelockedModificationCount = 0;
        int LockedModificationCount = 0;

        bool Prepared = false;
        bool Aborted = false;
        bool Committed = false;
        bool SignatureMismatch = false;

        void Start()
        {
            InsertOrCrash(Self->RunningTransactions_, this);
        }

        bool CanPrelock() const
        {
            return !Aborted && PrelockedModificationCount < std::ssize(Modifications);
        }

        bool Prelock()
        {
            YT_VERIFY(CanPrelock());
            if (Modifications[PrelockedModificationCount]->Apply()) {
                ++PrelockedModificationCount;
                return true;
            } else {
                return false;
            }
        }

        bool CanLock() const
        {
            return !Aborted && LockedModificationCount < PrelockedModificationCount;
        }

        void Lock()
        {
            YT_VERIFY(CanLock());
            Modifications[LockedModificationCount]->Confirm();
            ++LockedModificationCount;
        }

        bool CanGeneratePrepareTimestamp() const
        {
            return
                !Committed &&
                !Aborted &&
                !Prepared &&
                LockedModificationCount == std::ssize(Modifications) &&
                !GeneratedPrepareTimestamp;
        }

        void GeneratePrepareTimestamp()
        {
            YT_VERIFY(CanGeneratePrepareTimestamp());
            GeneratedPrepareTimestamp = Self->GenerateTimestamp();
        }

        bool CanPrepare() const
        {
            return
                !Committed &&
                !Aborted &&
                !Prepared &&
                LockedModificationCount == std::ssize(Modifications) &&
                GeneratedPrepareTimestamp;
        }

        bool Prepare()
        {
            YT_VERIFY(CanPrepare());

            if (SignatureMismatch) {
                Abort();
                return false;
            }

            Self->PrepareTransaction(Transaction.get(), *GeneratedPrepareTimestamp);
            // Order should not be important.
            for (auto* action : GetShuffledModifications()) {
                action->Prepare();
            }

            Prepared = true;
            return true;
        }

        bool CanGenerateCommitTimestamp() const
        {
            return Prepared && !Aborted && !Committed && !GeneratedCommitTimestamp;
        }

        void GenerateCommitTimestamp()
        {
            YT_VERIFY(CanGenerateCommitTimestamp());
            GeneratedCommitTimestamp = Self->GenerateTimestamp();
        }

        bool CanCommit() const
        {
            return Prepared && !Aborted && !Committed && GeneratedCommitTimestamp;
        }

        void Commit()
        {
            YT_VERIFY(CanCommit());
            Self->CommitTransaction(Transaction.get(), *GeneratedCommitTimestamp);
            // Order should not be important.
            for (auto* action : GetShuffledModifications()) {
                action->Commit();
            }
            Committed = true;

            EraseOrCrash(Self->RunningTransactions_, this);
        }

        bool CanAbort()
        {
            return !Committed && !Aborted;
        }

        void Abort()
        {
            YT_VERIFY(CanAbort());
            Self->AbortTransaction(Transaction.get());
            // Order should not be important.
            for (auto* action : GetShuffledModifications()) {
                action->Abort();
            }
            Aborted = true;

            EraseOrCrash(Self->RunningTransactions_, this);
        }

        void Revive()
        {
            if (Committed || Aborted) {
                return;
            }

            for (int index = 0; index < PrelockedModificationCount; ++index) {
                Modifications[index]->Reset();
            }
            // Locked actions are revived, prelocked-but-not-locked not.
            PrelockedModificationCount = LockedModificationCount;
            for (int index = 0; index < LockedModificationCount; ++index) {
                Modifications[index]->Apply(/*prelock*/ false);
            }

            if (Prepared) {
                for (auto* action : GetShuffledModifications()) {
                    action->Prepare();
                }
            }

            if (LockedModificationCount != PrelockedModificationCount) {
                SignatureMismatch = true;
            }
        }

        std::vector<TModification*> GetShuffledModifications() const
        {
            std::vector<TModification*> modifications;
            modifications.reserve(Modifications.size());
            for (const auto& modification : Modifications) {
                modifications.push_back(modification.get());
            }
            std::shuffle(modifications.begin(), modifications.end(), Self->Rng);

            return modifications;
        }

        TTimestamp GetStartTimestamp() const
        {
            return Transaction->GetStartTimestamp();
        }

        std::optional<TTimestamp> GetCommitTimestamp() const
        {
            return Committed
                ? std::make_optional(Transaction->GetCommitTimestamp())
                : std::nullopt;
        }
    };
    std::vector<std::unique_ptr<TTransactionState>> Transactions_;
    THashSet<TTransactionState*> RunningTransactions_;

    // Naive implementations.

    struct TValueState
    {
        bool Enabled = false;
        std::optional<int> Value;
        TTimestamp ValueTimestamp = NullTimestamp;

        void TryUpdate(
            std::optional<int> value,
            TTimestamp timestamp)
        {
            if (!Enabled) {
                return;
            }

            if (timestamp > ValueTimestamp) {
                Value = value;
                ValueTimestamp = timestamp;
            }
        }
    };

    TLookupResult NaiveLookup(const TLookupRequest& request)
    {
        TLookupResult result;

        std::array<TValueState, ValueCount> values;
        for (int index = 0; index < ValueCount; ++index) {
            values[index].Enabled = request.ValueFilter[index];
        }

        TValueState RowExistence;
        RowExistence.Enabled = true;

        for (const auto& transaction : Transactions_) {
            if (transaction->Aborted) {
                continue;
            }
            if (!transaction->Prepared) {
                continue;
            }
            if (transaction->Transaction->GetPrepareTimestamp() >= request.Timestamp) {
                continue;
            }
            for (const auto& modification : transaction->Modifications) {
                if (modification->GetKey() != request.Key) {
                    continue;
                }
                if (transaction->Committed) {
                    auto commitTimestamp = *transaction->GetCommitTimestamp();
                    if (commitTimestamp > request.Timestamp) {
                        continue;
                    }
                    if (modification->Type == ERowModificationType::Delete) {
                        for (int index = 0; index < ValueCount; ++index) {
                            values[index].TryUpdate(/*value*/ std::nullopt, commitTimestamp);
                        }
                        RowExistence.TryUpdate(/*value*/ std::nullopt, commitTimestamp);
                    } else {
                        auto row = modification->Row;
                        for (int index = 0; index < ValueCount; ++index) {
                            if (auto value = row.GetValue(index)) {
                                values[index].TryUpdate(value, commitTimestamp);
                                RowExistence.TryUpdate(/*value*/ 1, commitTimestamp);
                            }
                            // TODO(gritukan): Exclusive lock ~ write.
                            if (modification->LockMask.Get(index) == ELockType::Exclusive) {
                                RowExistence.TryUpdate(/*value*/ 1, commitTimestamp);
                            }
                        }
                    }
                } else {
                    auto lockMask = modification->GetLockMask();
                    bool locked = false;
                    if (lockMask.Get(0) == ELockType::Exclusive) {
                        locked = true;
                    }
                    for (int index = 0; index < ValueCount; ++index) {
                        // TODO(gritukan): Respect column filter when sorted store will be fixed.
                        if (lockMask.Get(index) == ELockType::Exclusive) {
                            locked = true;
                        }
                    }
                    if (locked) {
                        result.Locked = true;
                        return result;
                    }
                }
            }
        }

        if (!RowExistence.Value) {
            return result;
        }

        TRow row;
        row.Key = request.Key;
        for (int index = 0; index < 3; ++index) {
            row.GetValue(index) = values[index].Value;
        }
        result.Row = row;

        return result;
    };

    TLookupResult VeryNaiveLookup(const TLookupRequest& request)
    {
        TLookupResult result;

        std::array<TValueState, ValueCount> values;
        for (int index = 0; index < ValueCount; ++index) {
            values[index].Enabled = request.ValueFilter[index];
        }

        TValueState RowExistence;
        RowExistence.Enabled = true;

        for (const auto& transaction : Transactions_) {
            YT_VERIFY(transaction->Aborted || transaction->Committed);
            if (transaction->Aborted) {
                continue;
            }

            for (const auto& modification : transaction->Modifications) {
                if (modification->GetKey() != request.Key) {
                    continue;
                }

                auto commitTimestamp = *transaction->GetCommitTimestamp();
                if (modification->Type == ERowModificationType::Delete) {
                    for (int index = 0; index < ValueCount; ++index) {
                        values[index].TryUpdate(/*value*/ std::nullopt, commitTimestamp);
                    }
                    RowExistence.TryUpdate(/*value*/ std::nullopt, commitTimestamp);
                } else {
                    auto row = modification->Row;
                    bool hasWrite = false;
                    for (int index = 0; index < ValueCount; ++index) {
                        if (auto value = row.GetValue(index)) {
                            values[index].TryUpdate(value, commitTimestamp);
                            hasWrite = true;
                        }
                    }
                    if (hasWrite) {
                        RowExistence.TryUpdate(/*value*/ 1, commitTimestamp);
                    }
                }
            }
        }

        if (!RowExistence.Value) {
            return result;
        }

        TRow row;
        row.Key = request.Key;
        for (int index = 0; index < 3; ++index) {
            row.GetValue(index) = values[index].Value;
        }
        result.Row = row;

        return result;
    }

    bool CheckConflict(
        TTransactionState* tx1,
        TModification* modification1,
        TTransactionState* tx2,
        TModification* modification2,
        bool ignoreFirstLiveness = false)
    {
        if (tx1 == tx2) {
            return false;
        }
        if (modification1->GetKey() != modification2->GetKey()) {
            return false;
        }

        auto inside = [&] (
            TTimestamp startTs,
            TTimestamp commitTs,
            TTimestamp ts)
        {
            return startTs <= ts && ts <= commitTs;
        };

        auto intersects = [&] (
            TTimestamp startTs1,
            TTimestamp commitTs1,
            TTimestamp startTs2,
            TTimestamp commitTs2)
        {
            return
                inside(startTs1, commitTs1, startTs2) ||
                inside(startTs1, commitTs1, commitTs2) ||
                inside(startTs2, commitTs2, startTs1) ||
                inside(startTs2, commitTs2, commitTs1);
        };

        auto checkAlive = [&] (TTransactionState* tx, TModification* modification) {
            if (tx->Aborted) {
                return false;
            }
            if (!modification->Applied) {
                return false;
            }
            return true;
        };
        if ((!ignoreFirstLiveness && !checkAlive(tx1, modification1)) || !checkAlive(tx2, modification2)) {
            return false;
        }

        if (!intersects(
            tx1->GetStartTimestamp(),
            tx1->GetCommitTimestamp().value_or(MaxTimestamp),
            tx2->GetStartTimestamp(),
            tx2->GetCommitTimestamp().value_or(MaxTimestamp)))
        {
            return false;
        }

        auto checkConflict = [&] (ELockType lock1, ELockType lock2) {
            if (lock1 == ELockType::Exclusive && lock2 == ELockType::Exclusive) {
                return true;
            }
            if (lock1 == ELockType::SharedStrong && lock2 == ELockType::Exclusive) {
                return true;
            }
            if (lock1 == ELockType::Exclusive && lock2 == ELockType::SharedStrong) {
                return true;
            }

            // Tx1 aims for exclusive lock and tx2 for shared weak.
            auto checkExclusiveVsSharedWeak = [&] (
                const TTransactionState* tx1,
                const TTransactionState* tx2)
            {
                if (tx1->Committed && tx2->Committed) {
                    return false;
                }
                if (tx1->Committed && tx2->GetStartTimestamp() < *tx1->GetCommitTimestamp()) {
                    return true;
                }
                if (!tx1->Committed && !tx2->Committed) {
                    return true;
                }
                return false;
            };

            if (lock1 == ELockType::Exclusive &&
                lock2 == ELockType::SharedWeak &&
                checkExclusiveVsSharedWeak(tx1, tx2))
            {
                return true;
            }

            if (lock1 == ELockType::SharedWeak &&
                lock2 == ELockType::Exclusive &&
                checkExclusiveVsSharedWeak(tx2, tx1))
            {
                return true;
            }

            return false;
        };

        auto lockMask1 = modification1->GetLockMask();
        auto lockMask2 = modification2->GetLockMask();

        if (checkConflict(lockMask1.Get(PrimaryLockIndex), lockMask2.Get(PrimaryLockIndex))) {
            return true;
        }

        for (int index = 1; index <= ValueCount; ++index) {
            if (checkConflict(lockMask1.Get(index), lockMask2.Get(index))) {
                return true;
            }
            if (checkConflict(lockMask1.Get(PrimaryLockIndex), lockMask2.Get(index))) {
                return true;
            }
            if (checkConflict(lockMask1.Get(index), lockMask2.Get(PrimaryLockIndex))) {
                return true;
            }
        }

        return false;
    }

    bool CheckConflict(
        TTransactionState* tx,
        TModification* modification,
        bool ignoreFirstLiveness = false)
    {
        for (const auto& otherTx : Transactions_) {
            // Fast path.
            if (otherTx->Aborted) {
                continue;
            }
            for (const auto& otherModification : otherTx->Modifications) {
                if (CheckConflict(tx, modification, otherTx.get(), otherModification.get(), ignoreFirstLiveness)) {
                    return true;
                }
            }
        }

        return false;
    }

    TGuid GenerateId()
    {
        auto part0 = Rng();
        auto part1 = Rng();
        auto part2 = Rng();
        auto part3 = Rng();

        return TGuid(part0, part1, part2, part3);
    }
};

TEST_P(TSortedStoreManagerStressTest, Test)
{
    BIND(&TSortedStoreManagerStressTest::RunTest, Unretained(this))
        .AsyncVia(TestQueue_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TSortedStoreManagerStressTest::RunTest()
{
    // Reset everything.
    ResetLookupInvoker();
    StoreManager_.Reset();
    DoSetUp();

    auto randomElement = [&] (const THashSet<TTransactionState*>& set) {
        std::vector<TTransactionState*> elements(set.begin(), set.end());
        SortBy(elements, [&] (TTransactionState* transaction) {
            return transaction->Transaction->GetId();
        });

        return elements[Rng() % elements.size()];
    };

    constexpr int Iterations = 10'000;
    int MaxRunningTransactions = Rng() % 30 + 1;
    int MaxActionsPerTransaction = Rng() % 20 + 2;
    int KeyCount = Rng() % 20 + 1;
    int MaxValue = Rng() % 100'000 + 1;
    MaxActionsPerTransaction = std::min(KeyCount, MaxActionsPerTransaction);
    int MaxConcurrentLookups = Rng() % 50 + 1;

    Cerr << Format("Test parameters (Iterations: %v, MaxRunningTransactions: %v, MaxActionsPerTransaction: %v, KeyCount: %v, "
        "MaxValue: %v, MaxConcurrentLookups: %v)",
        Iterations,
        MaxRunningTransactions,
        MaxActionsPerTransaction,
        KeyCount,
        MaxValue,
        MaxConcurrentLookups) << Endl;

    THashSet<TLookuperPtr> lookupers;

    int createdTransactions = 0;
    int committedTransactions = 0;
    int modifications = 0;
    int conflicts = 0;
    int storeRotations = 0;
    int immediateLookups = 0;
    int delayedLookups = 0;

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        auto eventType = Rng() % 100;

        if (eventType <= 10) {
            if (std::ssize(RunningTransactions_) == MaxRunningTransactions) {
                continue;
            }

            auto transaction = std::make_unique<TTransactionState>();
            transaction->Self = this;

            auto transactionId = MakeId(EObjectType::Transaction, TCellTag(0x10), createdTransactions, 0x42);
            transaction->Transaction = StartTransaction(NullTimestamp, transactionId);
            ++createdTransactions;

            THashSet<int> usedKeys;
            int actionCount = Rng() % MaxActionsPerTransaction;
            for (int actionIndex = 0; actionIndex < actionCount; ++actionIndex) {
                int actionType = Rng() % 10;
                int key = -1;
                while (key == -1 || usedKeys.contains(key)) {
                    key = Rng() % KeyCount;
                }
                usedKeys.insert(key);
                auto modification = std::make_unique<TModification>();
                modification->Self = this;
                modification->Transaction = transaction->Transaction.get();
                if (actionType <= 3) {
                    modification->Type = ERowModificationType::Write;
                    auto genValue = [&] () -> std::optional<int> {
                        if (Rng() % 2 == 0) {
                            return std::nullopt;
                        } else {
                            return Rng() % MaxValue;
                        }
                    };

                    TRow row{
                        .Key = key,
                        .X = genValue(),
                        .Y = genValue(),
                        .Z = genValue()
                    };
                    // NB: At least one value should be changed.
                    if (!row.X && !row.Y && !row.Z) {
                        continue;
                    }
                    YT_VERIFY(row.X || row.Y || row.Z);
                    modification->Row = row;
                } else if (actionType <= 7) {
                    modification->Type = ERowModificationType::WriteAndLock;

                    TRow row;
                    row.Key = key;
                    bool emptyRequest = true;
                    for (int index = 0; index < ValueCount; ++index) {
                        int mode = Rng() % 3;
                        if (mode == 0) {
                            row.GetValue(index) = Rng() % MaxValue;
                            emptyRequest = false;
                        } else if (mode == 1) {
                            int lockMode = Rng() % 3;
                            switch (lockMode) {
                                case 0:
                                    modification->LockMask.Set(index, ELockType::SharedWeak);
                                    break;
                                case 1:
                                    modification->LockMask.Set(index, ELockType::SharedWeak);
                                    break;
                                case 2:
                                    modification->LockMask.Set(index, ELockType::Exclusive);
                                    break;
                                default:
                                    YT_ABORT();
                            }
                            emptyRequest = false;
                        }
                    }
                    // NB: At least one value should be changed.
                    if (emptyRequest) {
                        continue;
                    }
                    modification->Row = row;
                } else {
                    modification->Type = ERowModificationType::Delete;
                    TRow row{
                        .Key = key
                    };
                    modification->Row = row;
                }
                transaction->Modifications.push_back(std::move(modification));
                ++modifications;
            }
            transaction->Start();
            Transactions_.push_back(std::move(transaction));
        } else if (eventType <= 50) {
            if (RunningTransactions_.empty()) {
                continue;
            }
            auto* transaction = randomElement(RunningTransactions_);

            if (transaction->CanPrelock() || transaction->CanLock()) {
                auto doPrelock = [&] {
                    auto* modification = transaction->Modifications[transaction->PrelockedModificationCount].get();
                    bool conflict = CheckConflict(transaction, modification, /*ignoreFirstLiveness*/ true);
                    bool prelocked = transaction->Prelock();
                    ASSERT_EQ(prelocked, !conflict);
                    if (!prelocked) {
                        ++conflicts;
                        transaction->Abort();
                    }
                };
                auto doLock = [&] {
                    transaction->Lock();
                };
                if (!transaction->CanPrelock()) {
                    doLock();
                } else if (!transaction->CanLock()) {
                    doPrelock();
                } else if (Rng() % 2 == 0) {
                    doLock();
                } else {
                    doPrelock();
                }
            } else if (transaction->CanGeneratePrepareTimestamp()) {
                transaction->GeneratePrepareTimestamp();
            } else if (transaction->CanPrepare()) {
                transaction->Prepare();
            } else if (transaction->CanGenerateCommitTimestamp()) {
                transaction->GenerateCommitTimestamp();
            } else if (transaction->CanCommit()) {
                transaction->Commit();
                ++committedTransactions;
            }
        } else if (eventType <= 53) {
            if (RunningTransactions_.empty()) {
                continue;
            }
            auto* transaction = randomElement(RunningTransactions_);
            if (!transaction->CanAbort()) {
                continue;
            }

            transaction->Abort();
        } else if (eventType <= 54) {
            RotateStores();
            ++storeRotations;
        } else if (eventType <= 55) {
            // TODO(gritukan): Fix reserialization.
            /*
            ResetLookupInvoker();
            for (const auto& lookuper : lookupers) {
                Y_UNUSED(WaitFor(lookuper->GetResult()));
            }
            lookupers.clear();
            ReserializeTablet();
            for (const auto& transaction : RunningTransactions_) {
                transaction->Revive();
            }
            */
        } else {
            constexpr int TsGap = 20;
            TLookupRequest request {
                .Key = static_cast<TKey>(Rng() % KeyCount),
                .ValueFilter = {},
                .Timestamp = LastGeneratedTimestamp_ - TsGap + Rng() % (TsGap + 1),
                .RequestId = GenerateId(),
            };
            for (int index = 0; index < ValueCount; ++index) {
                request.ValueFilter[index] = (Rng() % 2 == 0);
            }

            auto expected = NaiveLookup(request);

            if (expected.Locked && std::ssize(lookupers) >= MaxConcurrentLookups) {
                continue;
            }

            auto lookuper = New<TLookuper>(
                this,
                request,
                /*async*/ expected.Locked);
            lookuper->Run();

            if (expected.Locked) {
                EXPECT_FALSE(lookuper->IsCompleted());
                InsertOrCrash(lookupers, lookuper);
            } else {
                EXPECT_TRUE(lookuper->IsCompleted());
                EXPECT_EQ(lookuper->GetResult().Get().Value().Row, expected.Row);
                ++immediateLookups;
            }
        }

        {
            std::vector<TLookuperPtr> completedLookupers;
            for (const auto& lookuper : lookupers) {
                auto expected = NaiveLookup(lookuper->GetRequest());

                if (expected.Locked) {
                    if (lookuper->IsCompleted()) {
                        completedLookupers.push_back(lookuper);
                        ++delayedLookups;
                    }
                } else {
                    auto result = WaitFor(lookuper->GetResult())
                        .ValueOrThrow();
                    ASSERT_EQ(result.Row, expected.Row);
                    completedLookupers.push_back(lookuper);
                    ++delayedLookups;
                }
            }

            for (const auto& lookuper : completedLookupers) {
                EraseOrCrash(lookupers, lookuper);
            }
        }
    }

    auto runningTransactions = RunningTransactions_;
    for (const auto& transaction : runningTransactions) {
        transaction->Abort();
    }

    for (const auto& lookuper : lookupers) {
        WaitFor(lookuper->GetResult())
            .ThrowOnError();
    }

    for (const auto& [lookupRequest, lookupResult] : LookupResults_) {
        EXPECT_EQ(VeryNaiveLookup(lookupRequest).Row, lookupResult.Row);
    }

    for (const auto& tx1 : Transactions_) {
        if (tx1->Aborted) {
            continue;
        }
        for (const auto& modification1 : tx1->Modifications) {
            for (const auto& tx2 : Transactions_) {
                if (tx2->Aborted) {
                    continue;
                }
                if (tx1.get() == tx2.get()) {
                    continue;
                }
                for (const auto& modification2 : tx2->Modifications) {
                    EXPECT_FALSE(CheckConflict(tx1.get(), modification1.get(), tx2.get(), modification2.get()));
                }
            }
        }
    }

    Cerr << Format("Test passed (CreatedTransactions: %v, CommittedTransactions: %v, Modifications: %v, Conflicts: %v, "
        "StoreRotations: %v, ImmediateLookups: %v, DelayedLookups: %v, State: %v)",
        createdTransactions,
        committedTransactions,
        modifications,
        conflicts,
        storeRotations,
        immediateLookups,
        delayedLookups,
        Rng()) << Endl;
}

INSTANTIATE_TEST_SUITE_P(Instantiation,
    TSortedStoreManagerStressTest,
    ::testing::Range(0, 100));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
