#include "sorted_dynamic_store_ut_helpers.h"
#include "store_context_mock.h"

#include <yt/yt/core/profiling/timing.h>

#include <util/random/random.h>

namespace NYT::NTabletNode {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicStorePerfTest
    : public TSortedStoreTestBase
{
public:
    void RunDynamic(
        int iterationCount,
        int writePercentage)
    {
        Cerr << "Iterations: " << iterationCount << ", "
             << "WritePercentage: " << writePercentage
             << Endl;

        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };

        auto executeRead = [&] {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(RandomNumber<ui64>(1000000000), 0));

            auto key = builder.FinishRow();
            auto keySuccessor = GetKeySuccessor(key);

            auto reader = Store_->CreateReader(
                Tablet_->BuildSnapshot(nullptr),
                MakeSingletonRowRange(key, keySuccessor),
                SyncLastCommittedTimestamp,
                false,
                TColumnFilter(),
                ChunkReadOptions_,
                /*workloadCategory*/ std::nullopt);

            reader->Open().Get();
            reader->Read(options);
        };

        auto executeWrite = [&] {
            auto transaction = StartTransaction();

            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(RandomNumber<ui64>(1000000000), 0));
            builder.AddValue(MakeUnversionedInt64Value(123, 1));
            builder.AddValue(MakeUnversionedDoubleValue(3.1415, 2));
            builder.AddValue(MakeUnversionedStringValue("hello from YT", 3));
            auto row = builder.FinishRow();

            TWriteContext context;
            context.Phase = EWritePhase::Lock;
            context.Transaction = transaction.get();

            TLockMask lockMask;
            lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
            auto dynamicRow = Store_->ModifyRow(
                row,
                lockMask,
                false,
                &context);
            transaction->LockedRows().push_back(TSortedDynamicRowRef(Store_.Get(), nullptr, dynamicRow));

            PrepareTransaction(transaction.get());
            Store_->PrepareRow(transaction.get(), dynamicRow);

            CommitTransaction(transaction.get());
            Store_->CommitRow(transaction.get(), dynamicRow, lockMask, /*commandIsPureLock*/ false);
        };

        Cerr << "Warming up..." << Endl;

        for (int iteration = 0; iteration < iterationCount; ++iteration) {
            executeWrite();
        }

        Cerr << "Testing..." << Endl;

        TWallTimer timer;

        for (int iteration = 0; iteration < iterationCount; ++iteration) {
            if (RandomNumber<unsigned>(100) < static_cast<unsigned>(writePercentage)) {
                executeWrite();
            } else {
                executeRead();
            }
        }

        auto elapsed = timer.GetElapsedTime();
        Cerr << "Elapsed: " << elapsed.MilliSeconds() << "ms, "
             << "RPS: " << (int) iterationCount / elapsed.SecondsFloat() << Endl;
    }

private:
    const IStoreContextPtr StoreContext_ = New<TStoreContextMock>();

    TSortedDynamicStorePtr Store_;

    void SetUp() override
    {
        TSortedStoreTestBase::SetUp();
        CreateDynamicStore();
    }

    void CreateDynamicStore() override
    {
        Store_ = New<TSortedDynamicStore>(
            TStoreId(),
            Tablet_.get(),
            StoreContext_);
    }

    IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedDynamicStorePerfTest, DISABLED_DynamicWrite)
{
    RunDynamic(
        1000000,
        100);
}

TEST_F(TSortedDynamicStorePerfTest, DISABLED_DynamicRead)
{
    RunDynamic(
        1000000,
        0);
}

TEST_F(TSortedDynamicStorePerfTest, DISABLED_DynamicReadWrite)
{
    RunDynamic(
        1000000,
        50);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
