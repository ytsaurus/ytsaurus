#include "stdafx.h"
#include "memory_store_ut.h"

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTabletClient;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TStaticMemoryStoreTest
    : public TMemoryStoreTestBase
{
protected:
    TStaticMemoryStoreTest()
        : Builder(
            New<TTabletManagerConfig>(),
            Tablet.get())
    { }

    TStaticMemoryStoreBuilder Builder;
};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TStaticMemoryStoreTest, Empty)
{
    auto store = Builder.Finish();
    auto scanner = store->CreateScanner();
    ASSERT_EQ(
        scanner->Find(BuildKey("1").Get(), SyncLastCommittedTimestamp),
        NullTimestamp);
}

TEST_F(TStaticMemoryStoreTest, Small1)
{
    {
        Builder.BeginRow();

        auto* keys = Builder.AllocateKeys();
        keys[0] = MakeUnversionedIntegerValue(1);

        auto* timestamps = Builder.AllocateTimestamps(1);
        timestamps[0] = 10;

        auto* as = Builder.AllocateFixedValues(0, 1);
        as[0] = MakeVersionedIntegerValue(123, 10);
        
        Builder.EndRow();
    }

    auto store = Builder.Finish();
    auto scanner = store->CreateScanner();

    ASSERT_EQ(scanner->Find(BuildKey("0").Get(), SyncLastCommittedTimestamp), NullTimestamp);
    
    ASSERT_EQ(scanner->Find(BuildKey("1").Get(), SyncLastCommittedTimestamp), 10 | IncrementalTimestampMask);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(0), MakeUnversionedIntegerValue(123)), 0);
    ASSERT_EQ(scanner->GetFixedValue(1), nullptr);
    ASSERT_EQ(scanner->GetFixedValue(2), nullptr);

    ASSERT_EQ(scanner->Find(BuildKey("2").Get(), SyncLastCommittedTimestamp), NullTimestamp);
}

TEST_F(TStaticMemoryStoreTest, Small2)
{
    {
        Builder.BeginRow();
        
        auto* keys = Builder.AllocateKeys();
        keys[0] = MakeIntegerValue<TUnversionedValue>(1);

        auto* timestamps = Builder.AllocateTimestamps(2);
        timestamps[0] = 10;
        timestamps[1] = 20 | TombstoneTimestampMask;

        auto* as = Builder.AllocateFixedValues(0, 1);
        as[0] = MakeVersionedIntegerValue(123, 10);
        
        Builder.EndRow();
    }

    auto store = Builder.Finish();
    auto scanner = store->CreateScanner();
    auto key = BuildKey("1");

    ASSERT_EQ(scanner->Find(key.Get(), 1), NullTimestamp);

    ASSERT_EQ(scanner->Find(key.Get(), 19), 10 | IncrementalTimestampMask);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(0), MakeUnversionedIntegerValue(123)), 0);
    ASSERT_EQ(scanner->GetFixedValue(1), nullptr);
    ASSERT_EQ(scanner->GetFixedValue(2), nullptr);

    ASSERT_EQ(scanner->Find(key.Get(), 21), 20 | TombstoneTimestampMask);
}

TEST_F(TStaticMemoryStoreTest, Small3)
{
    {
        Builder.BeginRow();
        
        auto* keys = Builder.AllocateKeys();
        keys[0] = MakeIntegerValue<TUnversionedValue>(1);

        auto* timestamps = Builder.AllocateTimestamps(4);
        timestamps[0] = 10;
        timestamps[1] = 20 | TombstoneTimestampMask;
        timestamps[2] = 30;
        timestamps[3] = 40 | TombstoneTimestampMask;

        auto* bs = Builder.AllocateFixedValues(1, 2);
        bs[0] = MakeVersionedDoubleValue(1.0, 10);
        bs[1] = MakeVersionedDoubleValue(2.0, 11);

        auto* cs = Builder.AllocateFixedValues(2, 2);
        cs[0] = MakeVersionedStringValue("value1", 30);
        cs[1] = MakeVersionedStringValue("value2", 31);
        
        Builder.EndRow();
    }

    auto store = Builder.Finish();
    auto scanner = store->CreateScanner();
    auto key = BuildKey("1");

    ASSERT_EQ(scanner->Find(key.Get(), 9), NullTimestamp);

    ASSERT_EQ(scanner->Find(key.Get(), 10), 10 | IncrementalTimestampMask);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(scanner->GetFixedValue(0), nullptr);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(1), MakeVersionedDoubleValue(1.0, 10)), 0);
    ASSERT_EQ(scanner->GetFixedValue(2), nullptr);

    ASSERT_EQ(scanner->Find(key.Get(), 11), 10 | IncrementalTimestampMask);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(scanner->GetFixedValue(0), nullptr);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(1), MakeVersionedDoubleValue(2.0, 11)), 0);
    ASSERT_EQ(scanner->GetFixedValue(2), nullptr);

    ASSERT_EQ(scanner->Find(key.Get(), 20), 20 | TombstoneTimestampMask);

    ASSERT_EQ(scanner->Find(key.Get(), 30), 30);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(scanner->GetFixedValue(0), nullptr);
    ASSERT_EQ(scanner->GetFixedValue(1), nullptr);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(2), MakeVersionedStringValue("value1", 30)), 0);

    ASSERT_EQ(scanner->Find(key.Get(), 31), 30);
    ASSERT_EQ(CompareRowValues(scanner->GetKeys()[0], MakeUnversionedIntegerValue(1)), 0);
    ASSERT_EQ(scanner->GetFixedValue(0), nullptr);
    ASSERT_EQ(scanner->GetFixedValue(1), nullptr);
    ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(2), MakeVersionedStringValue("value2", 31)), 0);

    ASSERT_EQ(scanner->Find(key.Get(), 40), 40 | TombstoneTimestampMask);
}

TEST_F(TStaticMemoryStoreTest, Large1)
{
    for (int i = 0; i < 65536; ++i) {
        Builder.BeginRow();
        
        auto* keys = Builder.AllocateKeys();
        keys[0] = MakeIntegerValue<TUnversionedValue>(i * 100);

        auto* timestamps = Builder.AllocateTimestamps(2);
        timestamps[0] = i * 10 + 100;
        timestamps[1] = (i * 10 + 110) | TombstoneTimestampMask;

        auto* cs = Builder.AllocateFixedValues(2, 1);
        auto str = Stroka("value") + ToString(i);
        cs[0] = MakeVersionedStringValue(str, i * 10 + 100);
        
        Builder.EndRow();
    }

    auto store = Builder.Finish();
    auto scanner = store->CreateScanner();

    for (int i = 0; i < 65536; ++i) {
        auto key = BuildKey(ToString(i * 100));

        ASSERT_EQ(scanner->Find(key.Get(), i * 10 + 99), NullTimestamp);

        ASSERT_EQ(scanner->Find(key.Get(), i * 10 + 100), (i * 10 + 100) | IncrementalTimestampMask);
        ASSERT_EQ(scanner->Find(key.Get(), i * 10 + 101), (i * 10 + 100) | IncrementalTimestampMask);
        ASSERT_EQ(scanner->GetFixedValue(0), nullptr);
        ASSERT_EQ(scanner->GetFixedValue(1), nullptr);
        ASSERT_EQ(CompareRowValues(*scanner->GetFixedValue(2), MakeVersionedStringValue("value" + ToString(i), i * 10 + 100)), 0);

        ASSERT_EQ(scanner->Find(key.Get(), i * 10 + 110), (i * 10 + 110) | TombstoneTimestampMask);
    }
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

