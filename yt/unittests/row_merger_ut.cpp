#include "stdafx.h"
#include "framework.h"

#include <core/misc/protobuf_helpers.h>

#include <core/ytree/convert.h>

#include <ytlib/new_table_client/versioned_row.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <server/tablet_node/row_merger.h>
#include <server/tablet_node/config.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTabletNode;
using namespace NTransactionClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TRowMergerTestBase
    : public ::testing::Test
{
protected:
    TRowBuffer Buffer_;
    int KeyCount_ = -1;

    TVersionedRow BuildVersionedRow(
        const Stroka& keyYson,
        const Stroka& valueYson,
        const std::vector<TTimestamp>& deleteTimestamps = std::vector<TTimestamp>())
    {
        TVersionedRowBuilder builder(&Buffer_);

        auto keys = ConvertTo<std::vector<INodePtr>>(TYsonString(keyYson, EYsonType::ListFragment));

        if (KeyCount_ == -1) {
            KeyCount_ = keys.size();
        } else {
            YCHECK(KeyCount_ == keys.size());
        }

        int keyId = 0;
        for (auto key : keys) {
            switch (key->GetType()) {
                case ENodeType::Int64:
                    builder.AddKey(MakeUnversionedInt64Value(key->GetValue<i64>(), keyId));
                    break;
                case ENodeType::Uint64:
                    builder.AddKey(MakeUnversionedUint64Value(key->GetValue<ui64>(), keyId));
                    break;
                case ENodeType::Double:
                    builder.AddKey(MakeUnversionedDoubleValue(key->GetValue<double>(), keyId));
                    break;
                case ENodeType::String:
                    builder.AddKey(MakeUnversionedStringValue(key->GetValue<Stroka>(), keyId));
                    break;
                default:
                    YUNREACHABLE();
                    break;
            }
            ++keyId;
        }

        auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
        for (auto value : values) {
            int id = value->Attributes().Get<int>("id");
            auto timestamp = SecondsToTimestamp(value->Attributes().Get<TTimestamp>("ts"));
            switch (value->GetType()) {
                case ENodeType::Entity:
                    builder.AddValue(MakeVersionedSentinelValue(EValueType::Null, timestamp, id));
                    break;
                case ENodeType::Int64:
                    builder.AddValue(MakeVersionedInt64Value(value->GetValue<i64>(), timestamp, id));
                    break;
                case ENodeType::Uint64:
                    builder.AddValue(MakeVersionedUint64Value(value->GetValue<ui64>(), timestamp, id));
                    break;
                case ENodeType::Double:
                    builder.AddValue(MakeVersionedDoubleValue(value->GetValue<double>(), timestamp, id));
                    break;
                case ENodeType::String:
                    builder.AddValue(MakeVersionedStringValue(value->GetValue<Stroka>(), timestamp, id));
                    break;
                default:
                    builder.AddValue(MakeVersionedAnyValue(ConvertToYsonString(value).Data(), timestamp, id));
                    break;
            }
        }

        for (auto timestamp : deleteTimestamps) {
            builder.AddDeleteTimestamp(SecondsToTimestamp(timestamp));
        }

        return builder.FinishRow();
    }

    TUnversionedRow BuildUnversionedRow(const Stroka& valueYson)
    {
        TUnversionedRowBuilder builder;

        auto values = ConvertTo<std::vector<INodePtr>>(TYsonString(valueYson, EYsonType::ListFragment));
        for (auto value : values) {
            int id = value->Attributes().Get<int>("id");
            switch (value->GetType()) {
                case ENodeType::Entity:
                    builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
                    break;
                case ENodeType::Int64:
                    builder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                    break;
                case ENodeType::Uint64:
                    builder.AddValue(MakeUnversionedUint64Value(value->GetValue<ui64>(), id));
                    break;
                case ENodeType::Double:
                    builder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                    break;
                case ENodeType::String:
                    builder.AddValue(MakeUnversionedStringValue(value->GetValue<Stroka>(), id));
                    break;
                default:
                    builder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).Data(), id));
                    break;
            }
        }

        return Buffer_.Capture(builder.GetRow());
    }


    static TTimestamp SecondsToTimestamp(int seconds)
    {
        return TTimestamp(seconds) << TimestampCounterWidth;
    }

};


////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMergerTest
    : public TRowMergerTestBase
{ };

TEST_F(TUnversionedRowMergerTest, Simple1)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=200> 3.14"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 2; <id=2> 3.14; <id=3> \"test\""),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Simple2)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 3; <id=2> #; <id=3> #"),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete1)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "", { 100 }));

    EXPECT_EQ(
        TUnversionedRow(),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete2)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "", { 100 }));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete3)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "", { 100 }));
    merger.AddPartialRow(BuildVersionedRow("0", "", { 300 }));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));

    EXPECT_EQ(
        TUnversionedRow(),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete4)
{
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, TColumnFilter());

    merger.AddPartialRow(BuildVersionedRow("0", "", { 100 }));
    merger.AddPartialRow(BuildVersionedRow("0", "", { 300 }));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=400> 3.15"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> 3.15; <id=3> #"),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Filter1)
{
    TColumnFilter filter { 0 };
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, filter);

    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=200> 3.14"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0"),
        merger.BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Filter2)
{
    TColumnFilter filter { 1, 2 };
    TUnversionedRowMerger merger(Buffer_.GetAlignedPool(), 4, 1, filter);

    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=200> 3.14"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=1> 2; <id=2> 3.14"),
        merger.BuildMergedRow());
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMergerTest
    : public TRowMergerTestBase
{ };

TEST_F(TVersionedRowMergerTest, KeepAll1)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));

    EXPECT_EQ(
        BuildVersionedRow("0", "<id=1;ts=100> 1"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepAll2)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3"));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=100> 1;"
            "<id=1;ts=200> 2;"
            "<id=1;ts=300> 3;"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepAll3)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2", {  50 }));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1", { 150 }));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3", { 250 }));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=100> 1; <id=1;ts=200> 2; <id=1;ts=300> 3;",
            { 50, 150, 250 }),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepAll4)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2; <id=2;ts=200> 3.14"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3; <id=3;ts=500> \"test\""));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=100> 1; <id=1;ts=200> 2; <id=1;ts=300> 3;"
            "<id=2;ts=200> 3.14;"
            "<id=3;ts=500> \"test\";"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepAll5)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1; <id=1;ts=200> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=100> 3; <id=2;ts=200> 4"));
    
    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=100> 1; <id=1;ts=200> 2;"
            "<id=2;ts=100> 3; <id=2;ts=200> 4"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepLatest1)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3"));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=300> 3"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepLatest2)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2; <id=1;ts=199> 20"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=100> 3.14; <id=2;ts=99> 3.15"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=3;ts=300> \"test\"; <id=3;ts=299> \"tset\""));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=200> 2; <id=2;ts=100> 3.14; <id=3;ts=300> \"test\""),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepLatest3)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), SecondsToTimestamp(200));
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "", { 200 }));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "",
            { 200 }),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepLatest4)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000000), SecondsToTimestamp(201));
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "", { 200 }));

    EXPECT_FALSE(merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, Expire1)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 0;
    config->MaxDataTtl = TDuration::Seconds(1000);

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1101), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));

    EXPECT_EQ(
        BuildVersionedRow("0", "<id=1;ts=100> 1"),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, Expire2)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 0;
    config->MaxDataTtl = TDuration::Seconds(1000);

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1102), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));

    EXPECT_FALSE(merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, Expire3)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 3;
    config->MinDataTtl = TDuration::Seconds(0);
    config->MaxDataTtl = TDuration::Seconds(10000);

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000), 0);
    
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=100> 1"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=200> 2"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=300> 3"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=1;ts=400> 4"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=2;ts=200> 3.14"));
    merger.AddPartialRow(BuildVersionedRow("0", "<id=3;ts=300> \"test\""));
    merger.AddPartialRow(BuildVersionedRow("0", "", { 350 }));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "<id=1;ts=300> 3; <id=1;ts=400> 4;"
            "<id=2;ts=200> 3.14;"
            "<id=3;ts=300> \"test\";",
            { 350 }),
        merger.BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, DeleteOnly)
{
    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 10;

    TChunkedMemoryPool pool;
    TVersionedRowMerger merger(&pool, 1, config, SecondsToTimestamp(1000), 0);

    merger.AddPartialRow(BuildVersionedRow("0", "", { 100 }));

    EXPECT_EQ(
        BuildVersionedRow(
            "0",
            "",
            { 100 }),
        merger.BuildMergedRow());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT
