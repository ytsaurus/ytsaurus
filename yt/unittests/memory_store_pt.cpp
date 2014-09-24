#include "stdafx.h"
#include "memory_store_ut.h"

#include <yt/core/profiling/scoped_timer.h>

#include <util/random/random.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TMemoryStorePerfTest
    : public TMemoryStoreTestBase
{
public:
    void RunDynamic(
        int iterationCount,
        int writePercentage)
    {
        Cerr << "Iterations: " << iterationCount << ", "
             << "WritePercentage: " << writePercentage
             << Endl;

        std::vector<TVersionedRow> rows;
        rows.reserve(1);

        auto executeRead = [&] () {
            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(RandomNumber<ui64>(1000000000), 0));

            auto key = builder.FinishRow();
            auto keySuccessor = GetKeySuccessor(key.Get());

            auto reader = Store_->CreateReader(
                std::move(key),
                std::move(keySuccessor),
                LastCommittedTimestamp,
                TColumnFilter());

            reader->Open().Get();
            reader->Read(&rows);
        };

        auto executeWrite = [&] () {
            auto transaction = StartTransaction();

            TUnversionedOwningRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(RandomNumber<ui64>(1000000000), 0));
            builder.AddValue(MakeUnversionedInt64Value(123, 1));
            builder.AddValue(MakeUnversionedDoubleValue(3.1415, 2));
            builder.AddValue(MakeUnversionedStringValue("hello from YT", 3));
            auto row = builder.FinishRow();

            auto dynamicRow = Store_->WriteRow(
                transaction.get(),
                row.Get(),
                false,
                TDynamicRow::PrimaryLockMask);

            PrepareTransaction(transaction.get());
            Store_->PrepareRow(transaction.get(), dynamicRow);

            CommitTransaction(transaction.get());
            Store_->CommitRow(transaction.get(), dynamicRow);
        };

        Cerr << "Warming up..." << Endl;

        for (int iteration = 0; iteration < iterationCount; ++iteration) {
            executeWrite();
        }

        Cerr << "Testing..." << Endl;

        TScopedTimer timer;

        for (int iteration = 0; iteration < iterationCount; ++iteration) {
            if (RandomNumber<unsigned>(100) < writePercentage) {
                executeWrite();
            } else {
                executeRead();
            }
        }

        auto elapsed = timer.GetElapsed();
        Cerr << "Elapsed: " << elapsed.MilliSeconds() << "ms, "
             << "RPS: " << (int) iterationCount / elapsed.SecondsFloat() << Endl;
    }

private:
    virtual void SetUp() override
    {
        TMemoryStoreTestBase::SetUp();

        auto config = New<TTabletManagerConfig>();
        Store_ = New<TDynamicMemoryStore>(
            config,
            TStoreId(),
            Tablet_.get());
    }


    TDynamicMemoryStorePtr Store_;

};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TMemoryStorePerfTest, DISABLED_DynamicWrite)
{
    RunDynamic(
        1000000,
        100);
}

TEST_F(TMemoryStorePerfTest, DISABLED_DynamicRead)
{
    RunDynamic(
        1000000,
        0);
}

TEST_F(TMemoryStorePerfTest, DISABLED_DynamicReadWrite)
{
    RunDynamic(
        1000000,
        50);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT
