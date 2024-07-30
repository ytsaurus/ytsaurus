#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/overlapping_reader.h>
#include <yt/yt/ytlib/table_client/row_merger.h>
#include <yt/yt/ytlib/table_client/versioned_row_merger.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/tablet_client/watermark_runtime_data.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

namespace NYT::NTableClient {

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;
using NTabletClient::ERowMergerType;

////////////////////////////////////////////////////////////////////////////////

struct TIdentityComparableVersionedRow
{
    TVersionedRow Row;
};

bool operator == (TIdentityComparableVersionedRow lhs, TIdentityComparableVersionedRow rhs)
{
    return TBitwiseVersionedRowEqual()(lhs.Row, rhs.Row);
}

void PrintTo(TVersionedRow row, ::std::ostream* os)
{
    *os << ToString(row);
}

void PrintTo(TIdentityComparableVersionedRow row, ::std::ostream* os)
{
    *os << ToString(row.Row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient


namespace NYT::NTableClient {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NQueryClient;
using namespace NChunkClient::NProto;
using namespace NChunkClient;
using namespace NTabletClient;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TRowMergerTestBase
    : public ::testing::Test
{
protected:
    const TRowBufferPtr Buffer_ = New<TRowBuffer>();
    const IColumnEvaluatorCachePtr ColumnEvaluatorCache_ = CreateColumnEvaluatorCache(
        New<TColumnEvaluatorCacheConfig>());

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson,
        const std::vector<TTimestamp>& deleteTimestamps = {},
        const std::vector<TTimestamp>& extraWriteTimestamps = {})
    {
        return NTableClient::YsonToVersionedRow(Buffer_, keyYson, valueYson, deleteTimestamps, extraWriteTimestamps);
    }

    TUnversionedRow BuildUnversionedRow(const TString& valueYson)
    {
        auto row = NTableClient::YsonToSchemalessRow(valueYson);
        return Buffer_->CaptureRow(row);
    }

    static TDuration TimestampToDuration(TTimestamp timestamp)
    {
        // Round up.
        timestamp += (1ULL << TimestampCounterWidth) - 1;
        return TDuration::Seconds(timestamp >> TimestampCounterWidth);
    }

    static TTableSchema GetTypicalSchema()
    {
        TTableSchema schema({
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("l", EValueType::Int64),
            TColumnSchema("m", EValueType::Int64),
            TColumnSchema("n", EValueType::Int64)
        });
        return schema;
    }

    static TTableSchema GetTypicalWatermarkSchema()
    {
        TTableSchema schema({
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("watermark", EValueType::Uint64),
            TColumnSchema("m", EValueType::Int64),
            TColumnSchema("n", EValueType::Int64)
        });
        return schema;
    }

    static TTableSchema GetAggregateSumSchema()
    {
        TTableSchema schema({
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("l", EValueType::Int64),
            TColumnSchema("m", EValueType::Int64),
            TColumnSchema("n", EValueType::Int64)
                .SetAggregate(TString("sum"))
        });
        return schema;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulRowMergerTest
    : public TRowMergerTestBase
{
public:
    std::unique_ptr<TSchemafulRowMerger> GetTypicalMerger(
        TColumnFilter filter = TColumnFilter(),
        TTableSchema schema = GetTypicalSchema())
    {
        auto evaluator = ColumnEvaluatorCache_->Find(New<TTableSchema>(schema));
        return std::make_unique<TSchemafulRowMerger>(
            MergedRowBuffer_,
            schema.Columns().size(),
            schema.GetKeyColumnCount(),
            filter,
            evaluator);
    }

protected:
    const TRowBufferPtr MergedRowBuffer_ = New<TRowBuffer>();
};

TEST_F(TSchemafulRowMergerTest, Simple1)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=200> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 2; <id=2> 3.14; <id=3> \"test\""),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Simple2)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300> 3"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 3; <id=2> #; <id=3> #"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Delete1)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));

    EXPECT_EQ(
        TUnversionedRow(),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Delete2)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Delete3)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 300 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));

    EXPECT_EQ(
        TUnversionedRow(),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Delete4)
{
    auto merger = GetTypicalMerger();

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 300 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 1; <id=2;ts=200> 3.14; <id=3;ts=200> \"test\""));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=400> 3.15"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> 3.15; <id=3> #"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Filter1)
{
    TColumnFilter filter { 0 };
    auto merger = GetTypicalMerger(filter);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=200> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Filter2)
{
    TColumnFilter filter { 1, 2 };
    auto merger = GetTypicalMerger(filter);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=200> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=1> 2; <id=2> 3.14"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Aggregate1)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1; ts=100> 1"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> #; <id=3;aggregate=false> #;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, Aggregate2)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=400;aggregate=true> #"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 6;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, DeletedAggregate1)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200 }));

    EXPECT_EQ(
        TUnversionedRow(),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, DeletedAggregate2)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 1;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, DeletedAggregate3)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 1"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 1;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, DeletedAggregate4)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=400;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200 }));
    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 2;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, ResetAggregate1)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=false> 2"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 5;"),
        merger->BuildMergedRow());
}

TEST_F(TSchemafulRowMergerTest, ResetAggregate2)
{
    auto merger = GetTypicalMerger(TColumnFilter(), GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=false> #"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 2"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> #; <id=3;aggregate=false> 2;"),
        merger->BuildMergedRow());
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedRowMergerTest
    : public TRowMergerTestBase
{
public:
    std::unique_ptr<TUnversionedRowMerger> GetTypicalMerger(
        TTableSchema schema = GetTypicalSchema())
    {
        auto evaluator = ColumnEvaluatorCache_->Find(New<TTableSchema>(schema));
        return std::make_unique<TUnversionedRowMerger>(
            MergedRowBuffer_,
            schema.Columns().size(),
            schema.GetKeyColumnCount(),
            evaluator);
    }

protected:
    const TRowBufferPtr MergedRowBuffer_ = New<TRowBuffer>();
};

TEST_F(TUnversionedRowMergerTest, Simple1)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 2"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=2> 3.14"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 2; <id=2> 3.14; <id=3> \"test\""),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Simple2)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 1"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 2"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 3"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 3;"),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete1)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0"),
        merger->BuildDeleteRow());
}

TEST_F(TUnversionedRowMergerTest, Delete2)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Delete3)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0"),
        merger->BuildDeleteRow());
}

TEST_F(TUnversionedRowMergerTest, Delete4)
{
    auto merger = GetTypicalMerger();

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 1; <id=2> 3.14; <id=3> \"test\""));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=2> 3.15"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> 3.15; <id=3> #"),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Aggregate1)
{
    auto merger = GetTypicalMerger(GetAggregateSumSchema());

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=1> 1"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> 1;"),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, Aggregate2)
{
    auto merger = GetTypicalMerger(GetAggregateSumSchema());

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 1"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 2"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 3"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 6;"),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, DeletedAggregate1)
{
    auto merger = GetTypicalMerger(GetAggregateSumSchema());

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 1"));
    merger->DeletePartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=2> 3.15"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=1> #; <id=2> 3.15; <id=3;aggregate=false> #"),
        merger->BuildMergedRow());
}

TEST_F(TUnversionedRowMergerTest, ResetAggregate1)
{
    auto merger = GetTypicalMerger(GetAggregateSumSchema());

    merger->InitPartialRow(BuildUnversionedRow("<id=0> 0"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 1"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=false> 2"));
    merger->AddPartialRow(BuildUnversionedRow("<id=0> 0; <id=3;aggregate=true> 3"));

    EXPECT_EQ(
        BuildUnversionedRow("<id=0> 0; <id=3;aggregate=false> 5"),
        merger->BuildMergedRow());
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowMergerTest
    : public TRowMergerTestBase
    , public ::testing::WithParamInterface<ERowMergerType>
{
public:
    std::unique_ptr<IVersionedRowMerger> GetTypicalMerger(
        ERowMergerType rowMergerType,
        TRetentionConfigPtr config,
        TTimestamp currentTimestamp,
        TTimestamp majorTimestamp,
        TTableSchema schema = GetTypicalSchema(),
        TColumnFilter columnFilter = TColumnFilter(),
        bool mergeRowsOnFlush = false,
        bool mergeDeletionsOnFlush  = false,
        TYsonString runtimeData = {})
    {
        auto schemaPtr = New<TTableSchema>(schema);
        auto evaluator = ColumnEvaluatorCache_->Find(schemaPtr);

        return CreateVersionedRowMerger(
            rowMergerType,
            MergedRowBuffer_,
            std::move(schemaPtr),
            columnFilter,
            config,
            currentTimestamp,
            majorTimestamp,
            evaluator,
            std::move(runtimeData),
            mergeRowsOnFlush,
            true,
            mergeDeletionsOnFlush);
    }

    TRetentionConfigPtr GetRetentionConfig()
    {
        auto config = New<TRetentionConfig>();
        config->MinDataTtl = TDuration::Minutes(5);
        config->MaxDataTtl = TDuration::Minutes(5);
        return config;
    }

    TYsonString CreateWatermarkRuntimeData(const TWatermarkRuntimeDataConfig& config)
    {
        return BuildYsonStringFluently()
            .BeginMap()
                .Item(CustomRuntimeDataWatermarkKey).Value(config)
            .EndMap();
    }

protected:
    const TRowBufferPtr MergedRowBuffer_ = New<TRowBuffer>();
};

TEST_P(TVersionedRowMergerTest, ReuseSimple)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000,
        MinTimestamp,
        GetTypicalSchema(),
        TColumnFilter::MakeUniversal(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});

    // Test reuse.
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=100> 1"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow("<id=0> 0", "<id=2;ts=100> 1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, PreserveDeleteRow)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000,
        MinTimestamp,
        GetTypicalSchema(),
        TColumnFilter::MakeUniversal(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {2000000}));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow("<id=0> 0", "", {2000000})},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepAll1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepAll2)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300> 3; <id=1;ts=200> 2; <id=1;ts=100> 1;")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepAll3)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 2", {  50 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1", { 150 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300> 3", { 250 }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300> 3; <id=1;ts=200> 2; <id=1;ts=100> 1;",
            { 50, 150, 250 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepAll4)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 2; <id=2;ts=200> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300> 3; <id=3;ts=500> \"test\""));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300> 3; <id=1;ts=200> 2; <id=1;ts=100> 1;"
            "<id=2;ts=200> 3.14;"
            "<id=3;ts=500> \"test\";")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepAll5)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 1; <id=1;ts=200> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=100> 3; <id=2;ts=200> 4"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=200> 2; <id=1;ts=100> 1;"
            "<id=2;ts=200> 4; <id=2;ts=100> 3;")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepLatest1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300000000000> 3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepLatest2)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2; <id=1;ts=199000000000> 20"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=100000000000> 3.14; <id=2;ts=99000000000> 3.15"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000> \"test\"; <id=3;ts=299000000000> \"tset\""));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=200000000000> 2;"
            "<id=2;ts=100000000000> 3.14;"
            "<id=3;ts=300000000000> \"test\"")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepLatest3)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000000000000ULL, 200000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 200000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, KeepLatest4)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(GetParam(), config, 1000000000000000ULL, 201000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL }));

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, KeepLatest5Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 3;
    config->MaxDataVersions = 3;

    auto merger = GetTypicalMerger(ERowMergerType::Legacy, config, 1000000000000000ULL, 400000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 150000000000ULL, 250000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300000000000> 3; <id=1;ts=200000000000> 2;",
            { 250000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, KeepLatest5New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(ERowMergerType::New, config, 1000000000000000ULL, 400000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 150000000000ULL, 250000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=300000000000> 3;")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, KeepLatest6Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 2;
    config->MaxDataVersions = 2;

    auto merger = GetTypicalMerger(ERowMergerType::Legacy, config, 1000000000000000ULL, 150000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100000000000ULL, 200000000000ULL, 300000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 200000000000ULL, 300000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, KeepLatest6New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(ERowMergerType::New, config, 1000000000000000ULL, 150000000000ULL);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100000000000ULL, 200000000000ULL, 300000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 300000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Expire1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MaxDataTtl = TimestampToDuration(1000000000000ULL);

    auto merger = GetTypicalMerger(GetParam(), config, 1100000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=100000000000> 1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Expire2)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MaxDataTtl = TimestampToDuration(500000000000ULL);

    auto merger = GetTypicalMerger(GetParam(), config, 1102000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, Expire3Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 3;
    config->MinDataTtl = TimestampToDuration(0);
    config->MaxDataTtl = TimestampToDuration(10000000000000ULL);

    auto merger = GetTypicalMerger(ERowMergerType::Legacy, config, 1100000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=400000000000> 4"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=200000000000> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000> \"test\""));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 350000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=400000000000> 4; <id=1;ts=300000000000> 3;"
            "<id=2;ts=200000000000> 3.14;"
            "<id=3;ts=300000000000> \"test\";",
            { 350000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, Expire3New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MaxDataVersions = 3;
    config->MinDataTtl = TimestampToDuration(0);
    config->MaxDataTtl = TimestampToDuration(10000000000000ULL);

    auto merger = GetTypicalMerger(ERowMergerType::New, config, 1100000000000ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000> 3"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=400000000000> 4"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=2;ts=200000000000> 3.14"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000> \"test\""));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 350000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=400000000000> 4;",
            { 350000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, Expire4Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TimestampToDuration(0);

    auto merger = GetTypicalMerger(ERowMergerType::Legacy, config, 11ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=10> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=11> 2"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=11> 2")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, Expire4New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TimestampToDuration(0);

    auto merger = GetTypicalMerger(ERowMergerType::New, config, 12ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=10> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=11> 2"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=11> 2;")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Expire5)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->MinDataTtl = TimestampToDuration(0);

    auto merger = GetTypicalMerger(GetParam(), config, 12ULL, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=10> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=11> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=12> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=11> 2; <id=1;ts=12> 3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, DeleteOnly)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1100, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 100 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, ManyDeletes)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 10;

    auto merger = GetTypicalMerger(GetParam(), config, 1100, 0);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 300 }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 100, 200, 300 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Aggregate1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        300,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1; ts=100> 1"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=1;ts=100> 1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Aggregate2)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        100,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=100;aggregate=true> 1; <id=3;ts=200;aggregate=true> 2; <id=3;ts=300;aggregate=true> 10")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Aggregate3)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        200,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=100;aggregate=true> 1; <id=3;ts=200;aggregate=true> 2; <id=3;ts=300;aggregate=true> 10")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Aggregate4)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        300000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=200000000000;aggregate=true> 3; <id=3;ts=300000000000;aggregate=true> 10")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, Aggregate5)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        400000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=300000000000;aggregate=true> 13")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate1Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        1000000000000ULL,
        200000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 300000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=100000000000;aggregate=true> 1",
            { 300000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate1New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        1000000000000ULL,
        200000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 300000000000ULL }));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 300000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, DeletedAggregate2)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        300000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL }));

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_P(TVersionedRowMergerTest, DeletedAggregate3)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000,
        500000000000,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000, 400000000000 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000000;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=500000000000;aggregate=true> 3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, DeletedAggregate4)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        500000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100000000000ULL, 300000000000ULL }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=400000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000000;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=400000000000;aggregate=true> 2; <id=3;ts=500000000000;aggregate=true> 3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate5Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        1000000000000ULL,
        500000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL, 600000000000ULL }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 2; <id=3;ts=400000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000000;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=400000000000;aggregate=true> 4; <id=3;ts=500000000000;aggregate=true> 3",
            { 600000000000 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate5New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        1000000000000ULL,
        500000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL, 600000000000ULL }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 2; <id=3;ts=400000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000001;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 600000000000 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate6Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        1000,
        200,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100, 600 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=200;aggregate=true> 1; <id=3;ts=500;aggregate=true> 3",
            { 600 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeletedAggregate6New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        1000,
        200,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 100, 600 }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500;aggregate=true> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=200;aggregate=true> 1; <id=3;ts=500;aggregate=true> 3",
            { 100, 600 })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, ResetAggregate1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        300000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=false> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=false> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=false> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=200000000000;aggregate=false> 2; <id=3;ts=300000000000;aggregate=false> 10")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, ResetAggregate2Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        1000000000000ULL,
        500000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL, 600000000000ULL }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 2; <id=3;ts=400000000000;aggregate=false> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000000;aggregate=false> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=400000000000;aggregate=false> 2; <id=3;ts=500000000000;aggregate=false> 3",
            { 600000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, ResetAggregate2New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        1000000000000ULL,
        500000000000ULL,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", { 200000000000ULL, 600000000000ULL }));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 2; <id=3;ts=400000000000;aggregate=false> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=500000000000;aggregate=false> 3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "",
            { 600000000000ULL })},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, ExpiredAggregate)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TimestampToDuration(0);
    config->MaxDataTtl = TimestampToDuration(100000000000ULL);

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        300000000000ULL,
        0,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_P(TVersionedRowMergerTest, MergeAggregates1)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=300000000000;aggregate=true> 13")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, MergeAggregates2Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 2;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        1000000000000ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=200000000000;aggregate=true> 3;"
            "<id=3;ts=300000000000;aggregate=true> 10")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, MergeAggregates2New)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        1000000000000ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=300000000000;aggregate=true> 13;")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, MergeAggregates3)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=false> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=300000000000;aggregate=false> 12")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, MergeAggregates4)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TDuration::Zero();

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        100000000003ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000001;aggregate=false> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000002;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=100000000002;aggregate=false> 12")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, MergeAggregates5)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TDuration::Zero();

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        100000000003ULL,
        0,
        GetAggregateSumSchema(),
        TColumnFilter(),
        true);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000001;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000002;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=100000000002;aggregate=true> 13")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TTableSchema GetAggregateMinSchema()
{
    TTableSchema schema({
        TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("l", EValueType::Int64),
        TColumnSchema("m", EValueType::Int64),
        TColumnSchema("n", EValueType::Uint64)
            .SetAggregate(TString("min"))
    });
    return schema;
}

TEST_P(TVersionedRowMergerTest, MergeAggregates6)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        100000000003ULL,
        100,
        GetAggregateMinSchema(),
        TColumnFilter(),
        false);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {40}));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=50;aggregate=true> 67890u"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=60;aggregate=true> 12345u"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=60;aggregate=true> 12345u")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, IgnoreMajorTimestamp)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 1;
    config->IgnoreMajorTimestamp = true;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000000000000ULL,
        0,
        GetAggregateSumSchema());

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=100000000000;aggregate=true> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=200000000000;aggregate=true> 2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=3;ts=300000000000;aggregate=true> 10"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0",
            "<id=3;ts=300000000000;aggregate=true> 13")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, NoKeyColumnFilter)
{
    auto config = GetRetentionConfig();
    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        0,
        GetTypicalSchema(),
        TColumnFilter({1, 2, 3}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100>1;<id=2;ts=100>2;<id=3;ts=100>3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "", "<id=1;ts=100>1;<id=2;ts=100>2;<id=3;ts=100>3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, NoValueColumnFilter)
{
    auto config = GetRetentionConfig();
    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        0,
        GetTypicalSchema(),
        TColumnFilter({0}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100>1;<id=2;ts=100>2;<id=3;ts=100>3"));
    auto mergedRow = merger->BuildMergedRow();
    ASSERT_TRUE(!!mergedRow);

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "", {}, {100})},
        TIdentityComparableVersionedRow{mergedRow});
}

TEST_P(TVersionedRowMergerTest, OneValueColumnFilter)
{
    auto config = GetRetentionConfig();
    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        0,
        GetTypicalSchema(),
        TColumnFilter({1}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100>1;<id=2;ts=100>2;<id=3;ts=100>3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "", "<id=1;ts=100>1")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, YT_6800)
{
    auto merger = GetTypicalMerger(GetParam(), nullptr, SyncLastCommittedTimestamp, MaxTimestamp);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000>1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000>2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000>3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=100000000000>1;<id=1;ts=200000000000>2;<id=1;ts=300000000000>3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, SyncLastCommittedRetention)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MinDataVersions = 1;
    config->MaxDataTtl = TimestampToDuration(10000000000000ULL);
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(GetParam(), config, SyncLastCommittedTimestamp, MaxTimestamp);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=100000000000>1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000>2"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=300000000000>3"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=300000000000>3")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, YT_7668_1)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TimestampToDuration(1000);
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        10,
        0,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64),
        }},
        TColumnFilter({2}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=1> 1; <id=2;ts=1> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {2}));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=3> 3;"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "", "", {2}, {3})},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, YT_7668_2Legacy)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TimestampToDuration(1000);
    config->MinDataVersions = 2;
    config->MaxDataVersions = 2;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        SyncLastCommittedTimestamp,
        MaxTimestamp,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64),
        }},
        TColumnFilter({2}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=1> 1; <id=2;ts=1> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {2}));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=3> 3;"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "", "<id=2;ts=1> 1", {2}, {3})},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, YT_7668_2New)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TimestampToDuration(1000);
    config->MinDataVersions = 1;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::New,
        config,
        SyncLastCommittedTimestamp,
        MaxTimestamp,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64),
        }},
        TColumnFilter({2}));

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=1> 1; <id=2;ts=1> 1"));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {2}));
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=3> 3;"));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "", "", {}, {3})},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

// YT-13129
TEST_P(TVersionedRowMergerTest, DeleteTimestampsPrunedOnFlush1)
{
    auto config = GetRetentionConfig();
    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        0,
        GetTypicalSchema(),
        TColumnFilter(),
        true,
        true);

    // Sequence of deletes and writes:
    //  d d w d d d w d w  w  d
    //  1 2 3 4 5 6 7 8 9 10 11

    auto row = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=3> 1; <id=1;ts=7> 2; <id=1;ts=9> 3; <id=1;ts=10> 4",
        {1, 2, 4, 5, 6, 8, 11});

    merger->AddPartialRow(row);

    auto expectedMergedRow = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=3> 1; <id=1;ts=7> 2; <id=1;ts=9> 3; <id=1;ts=10> 4",
        {1, 4, 8, 11});

    EXPECT_EQ(
        TIdentityComparableVersionedRow{expectedMergedRow},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

// YT-13129
TEST_P(TVersionedRowMergerTest, DeleteTimestampsPrunedOnFlush2)
{
    auto config = GetRetentionConfig();
    auto merger = GetTypicalMerger(
        GetParam(),
        config,
        1000,
        0,
        GetTypicalSchema(),
        TColumnFilter(),
        true,
        true);

    // Sequence of deletes and writes:
    //  d d w d d d w d w  w
    //  1 2 3 4 5 6 7 8 9 10

    auto row = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=3> 1; <id=1;ts=7> 2; <id=1;ts=9> 3; <id=1;ts=10> 4",
        {1, 2, 4, 5, 6, 8});

    merger->AddPartialRow(row);

    auto expectedMergedRow = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=3> 1; <id=1;ts=7> 2; <id=1;ts=9> 3; <id=1;ts=10> 4",
        {1, 4, 8});

    EXPECT_EQ(
        TIdentityComparableVersionedRow{expectedMergedRow},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_P(TVersionedRowMergerTest, MergeWithLimit)
{
    auto merger = GetTypicalMerger(GetParam(), nullptr, SyncLastCommittedTimestamp, MaxTimestamp);

    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=10>1;<id=1;ts=20>2", {12}), 20);
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "<id=1;ts=15>3;<id=1;ts=30>4", {25}), 20);
    merger->AddPartialRow(BuildVersionedRow("<id=0> 0", "", {17}), 20);

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=10>1;<id=1;ts=15>3", {12, 17})},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeleteByTtlColumn)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TDuration::Seconds(30);
    config->MinDataVersions = 0;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        TimestampFromUnixTime(13),
        MaxTimestamp,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("$ttl", EValueType::Uint64),
        }},
        TColumnFilter({0, 1, 2}));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 1; <id=1;ts=%v> 2; "
            "<id=1;ts=%v> 3; <id=1;ts=%v> 4; "
            "<id=2;ts=%v> 10000u; <id=2;ts=%v> 1000u; "
            "<id=2;ts=%v> 1000u; <id=2;ts=%v> 1000u",
            TimestampFromUnixTime(3),
            TimestampFromUnixTime(7),
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10),
            TimestampFromUnixTime(3),
            TimestampFromUnixTime(7),
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10)),
        {TimestampFromUnixTime(6)});

    merger->AddPartialRow(row);

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, MergeIncreasedTtlColumn)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TDuration::Zero();
    config->MinDataVersions = 0;
    config->MaxDataVersions = 2;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        TimestampFromUnixTime(13),
        MaxTimestamp,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("v2", EValueType::Int64),
            TColumnSchema("$ttl", EValueType::Uint64),
        }},
        TColumnFilter({0, 1, 2, 3}));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 1; <id=2;ts=%v> 4; "
            "<id=3;ts=%v> 1000u; <id=3;ts=%v> 5000u",
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10),
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10)),
        {});

    merger->AddPartialRow(row);

    EXPECT_EQ(
        TIdentityComparableVersionedRow{row},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, DeleteIncreasedTtlColumn)
{
    auto config = GetRetentionConfig();
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TDuration::Seconds(30);
    config->MinDataVersions = 0;
    config->MaxDataVersions = 1;

    auto merger = GetTypicalMerger(
        ERowMergerType::Legacy,
        config,
        TimestampFromUnixTime(18),
        MaxTimestamp,
        TTableSchema{{
            TColumnSchema("k", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("v1", EValueType::Int64),
            TColumnSchema("$ttl", EValueType::Uint64),
        }},
        TColumnFilter({0, 1, 2}));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 1; <id=1;ts=%v> 4; "
            "<id=2;ts=%v> 1000u; <id=2;ts=%v> 5000u",
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10),
            TimestampFromUnixTime(9),
            TimestampFromUnixTime(10)),
        {});

    merger->AddPartialRow(row);

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, WatermarkBasic)
{
    auto watermarkDataConfig = TWatermarkRuntimeDataConfig();
    watermarkDataConfig.ColumnName = "watermark";
    watermarkDataConfig.Watermark = 11;
    auto merger = GetTypicalMerger(
        ERowMergerType::Watermark,
        nullptr,
        1000,
        0,
        GetTypicalWatermarkSchema(),
        TColumnFilter(),
        false,
        false,
        CreateWatermarkRuntimeData(watermarkDataConfig));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=10> 5; <id=1;ts=15> 10; <id=1;ts=20> 12;"
        "<id=2;ts=15> 42; <id=2;ts=13> 52");
    merger->AddPartialRow(row);

    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
                "<id=0> 0","<id=1;ts=20> 12")},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, InvalidWatermarkDataFormat)
{
    auto invalidRuntimeData = TYsonString(Format("{ %v = {column_name=l; watermark=\"11\"} }", CustomRuntimeDataWatermarkKey));
    auto merger = GetTypicalMerger(
        ERowMergerType::Watermark,
        nullptr,
        1000,
        0,
        GetTypicalWatermarkSchema(),
        TColumnFilter(),
        false,
        false,
        std::move(invalidRuntimeData));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=10> 5; <id=1;ts=15> 10; <id=1;ts=20> 12;"
        "<id=2;ts=15> 42; <id=2;ts=13> 52");
    merger->AddPartialRow(row);

    // Invalid runtime data is ignored.
    EXPECT_EQ(
        TIdentityComparableVersionedRow{row},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, WatermarkFullClear)
{
    auto watermarkDataConfig = TWatermarkRuntimeDataConfig();
    watermarkDataConfig.ColumnName = "watermark";
    watermarkDataConfig.Watermark = 13;
    auto merger = GetTypicalMerger(
        ERowMergerType::Watermark,
        nullptr,
        1000,
        0,
        GetTypicalWatermarkSchema(),
        TColumnFilter(),
        false,
        false,
        CreateWatermarkRuntimeData(watermarkDataConfig));

    auto row = BuildVersionedRow(
        "<id=0> 0",
        "<id=1;ts=10> 5; <id=1;ts=15> 10; <id=1;ts=20> 12;"
        "<id=2;ts=15> 42; <id=2;ts=13> 52");
    merger->AddPartialRow(row);

    EXPECT_FALSE(merger->BuildMergedRow());
}

TEST_F(TVersionedRowMergerTest, WatermarkBlocksMaxDataTtlRemoval)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TDuration::Zero();
    config->MaxDataTtl = TDuration::Seconds(5);

    auto watermarkDataConfig = TWatermarkRuntimeDataConfig();
    watermarkDataConfig.ColumnName = "watermark";
    watermarkDataConfig.Watermark = 13;
    auto merger = GetTypicalMerger(
        ERowMergerType::Watermark,
        config,
        TimestampFromUnixTime(10),
        0,
        GetTypicalWatermarkSchema(),
        TColumnFilter(),
        false,
        false,
        CreateWatermarkRuntimeData(watermarkDataConfig));

    // Without watermarks all rows should be deleted according to MaxDataTtl.
    auto row = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 5; <id=1;ts=%v> 10; <id=1;ts=%v> 12;"
            "<id=2;ts=%v> 42; <id=2;ts=%v> 52",
            TimestampFromUnixTime(1),
            TimestampFromUnixTime(3),
            TimestampFromUnixTime(4),
            TimestampFromUnixTime(2),
            TimestampFromUnixTime(4)));
    merger->AddPartialRow(row);

    auto expectedRow = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 12; <id=2;ts=%v> 52",
            TimestampFromUnixTime(4),
            TimestampFromUnixTime(4)));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{expectedRow},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

TEST_F(TVersionedRowMergerTest, WatermarkRemovalRespectsMinDataTtl)
{
    auto config = GetRetentionConfig();
    config->MinDataVersions = 0;
    config->MinDataTtl = TDuration::Seconds(5);
    config->MaxDataTtl = TDuration::Seconds(10);

    auto watermarkDataConfig = TWatermarkRuntimeDataConfig();
    watermarkDataConfig.ColumnName = "watermark";
    watermarkDataConfig.Watermark = 100;
    auto merger = GetTypicalMerger(
        ERowMergerType::Watermark,
        config,
        TimestampFromUnixTime(8),
        0,
        GetTypicalWatermarkSchema(),
        TColumnFilter(),
        false,
        false,
        CreateWatermarkRuntimeData(watermarkDataConfig));

    // According to watermark all rows should be deleted, but MinDataTtl prevents it.
    auto row = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> #; <id=1;ts=%v> 10; <id=1;ts=%v> 12;"
            "<id=2;ts=%v> 42; <id=2;ts=%v> 52",
            TimestampFromUnixTime(1),
            TimestampFromUnixTime(2),
            TimestampFromUnixTime(4),
            TimestampFromUnixTime(3),
            TimestampFromUnixTime(4)));
    merger->AddPartialRow(row);

    auto expectedRow = BuildVersionedRow(
        "<id=0> 0",
        Format(
            "<id=1;ts=%v> 12; <id=2;ts=%v> 42; <id=2;ts=%v> 52",
            TimestampFromUnixTime(4),
            TimestampFromUnixTime(3),
            TimestampFromUnixTime(4)));

    EXPECT_EQ(
        TIdentityComparableVersionedRow{expectedRow},
        TIdentityComparableVersionedRow{merger->BuildMergedRow()});
}

INSTANTIATE_TEST_SUITE_P(
    Test,
    TVersionedRowMergerTest,
    ::testing::Values(
       ERowMergerType::Legacy,
       ERowMergerType::New),
    [] (const auto& info) {
        return ToString(info.param);
    });

////////////////////////////////////////////////////////////////////////////////

class TMockVersionedReader
    : public IVersionedReader
{
public:
    explicit TMockVersionedReader(std::vector<TVersionedRow> rows)
        : Rows_(std::move(rows))
    { }

    TFuture<void> Open() override
    {
        return VoidFuture;
    }

    IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        if (Position_ == std::ssize(Rows_)) {
            return nullptr;
        }

        while (Position_ < std::ssize(Rows_) && rows.size() < rows.capacity()) {
            rows.push_back(Rows_[Position_]);
            ++Position_;
        }

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    TDataStatistics GetDataStatistics() const override
    {
        return TDataStatistics();
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        return TCodecStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return true;
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return std::vector<TChunkId>();
    }

private:
    std::vector<TVersionedRow> Rows_;
    int Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulMergingReaderTest
    : public TSchemafulRowMergerTest
{
public:
    void ReadAll(ISchemafulUnversionedReaderPtr reader, std::vector<TUnversionedRow>* result)
    {
        while (auto batch = reader->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto row : batch->MaterializeRows()) {
                result->push_back(Buffer_->CaptureRow(row));
            }
        }
    }
};

TEST_F(TSchemafulMergingReaderTest, Merge1)
{
    auto readers = std::vector<IVersionedReaderPtr>{
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 1")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=900> 2")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=600> 7")})
    };

    auto boundaries = std::vector<TUnversionedOwningRow>{
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0"))
    };

    auto merger = GetTypicalMerger();

    auto reader = CreateSchemafulOverlappingRangeReader(
        boundaries,
        std::move(merger),
        [readers] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    std::vector<TUnversionedRow> result;
    ReadAll(reader, &result);

    EXPECT_EQ(1u, result.size());
    EXPECT_EQ(BuildUnversionedRow("<id=0> 0; <id=1> 2; <id=2> #; <id=3> #"), result[0]);
}

TEST_F(TSchemafulMergingReaderTest, Merge2)
{
    auto readers = std::vector<IVersionedReaderPtr>{
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 0"),
            BuildVersionedRow("<id=0> 1", "<id=1;ts=200> 1")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 2", "<id=1;ts=100> 2"),
            BuildVersionedRow("<id=0> 3", "<id=1;ts=300> 3")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 1", "<id=1;ts=300> 4"),
            BuildVersionedRow("<id=0> 2", "<id=1;ts=600> 5")})
    };

    auto boundaries = std::vector<TUnversionedOwningRow>{
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 2")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 1"))
    };

    auto merger = GetTypicalMerger();

    auto reader = CreateSchemafulOverlappingRangeReader(
        boundaries,
        std::move(merger),
        [readers] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    std::vector<TUnversionedRow> result;
    ReadAll(reader, &result);

    EXPECT_EQ(4u, result.size());
    EXPECT_EQ(BuildUnversionedRow("<id=0> 0; <id=1> 0; <id=2> #; <id=3> #"), result[0]);
    EXPECT_EQ(BuildUnversionedRow("<id=0> 1; <id=1> 4; <id=2> #; <id=3> #"), result[1]);
    EXPECT_EQ(BuildUnversionedRow("<id=0> 2; <id=1> 5; <id=2> #; <id=3> #"), result[2]);
    EXPECT_EQ(BuildUnversionedRow("<id=0> 3; <id=1> 3; <id=2> #; <id=3> #"), result[3]);
}

TEST_F(TSchemafulMergingReaderTest, Lookup)
{
    auto readers = std::vector<IVersionedReaderPtr>{
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 0", "<id=1;ts=200> 0"),
            BuildVersionedRow("<id=0> 1", "<id=1;ts=400> 1")
        }),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 0", "<id=1;ts=300> 2"),
            BuildVersionedRow("<id=0> 1", "<id=1;ts=300> 3")
        }),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{
            BuildVersionedRow("<id=0> 0", "<id=1;ts=100> 4"),
            BuildVersionedRow("<id=0> 1", "<id=1;ts=600> 5")
        })
    };

    auto merger = GetTypicalMerger();

    auto reader = CreateSchemafulOverlappingLookupReader(
        std::move(merger),
        [readers, index = 0] () mutable -> IVersionedReaderPtr {
            if (index < std::ssize(readers)) {
                return readers[index++];
            } else {
                return nullptr;
            }
        });

    std::vector<TUnversionedRow> result;
    ReadAll(reader, &result);

    EXPECT_EQ(2u, result.size());
    EXPECT_EQ(BuildUnversionedRow("<id=0> 0; <id=1> 2; <id=2> #; <id=3> #"), result[0]);
    EXPECT_EQ(BuildUnversionedRow("<id=0> 1; <id=1> 5; <id=2> #; <id=3> #"), result[1]);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedMergingReaderTest
    : public TVersionedRowMergerTest
{
public:
    void ReadAll(IVersionedReaderPtr reader, std::vector<TVersionedRow>* result)
    {
        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1024
        };

        WaitFor(reader->Open())
            .ThrowOnError();

        while (auto batch = reader->Read(options)) {
            for (const auto& row : batch->MaterializeRows()) {
                Result_.push_back(TVersionedOwningRow(row));
                result->push_back(Result_.back());
            }

            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
        }
    }

private:
    std::vector<TVersionedOwningRow> Result_;
};

TEST_F(TVersionedMergingReaderTest, Merge1Legacy)
{
    auto readers = std::vector<IVersionedReaderPtr>{
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 1")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=900000000000> 2")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=600000000000> 3")})
    };

    auto boundaries = std::vector<TUnversionedOwningRow>{
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0"))
    };

    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 2;
    config->MinDataTtl = TimestampToDuration(600000000000ULL);
    config->MaxDataTtl = TimestampToDuration(600000000000ULL);

    auto merger = GetTypicalMerger(ERowMergerType::Legacy, config, 10000000000000ULL, 0);

    auto reader = CreateVersionedOverlappingRangeReader(
        boundaries,
        std::move(merger),
        [readers] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    std::vector<TVersionedRow> result;
    ReadAll(reader, &result);

    EXPECT_EQ(1u, result.size());
    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=600000000000> 3; <id=1;ts=900000000000> 2")},
        TIdentityComparableVersionedRow{result[0]});
}

TEST_F(TVersionedMergingReaderTest, Merge1New)
{
    auto readers = std::vector<IVersionedReaderPtr>{
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=200000000000> 1")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=900000000000> 2")}),
        New<TMockVersionedReader>(std::vector<TVersionedRow>{BuildVersionedRow("<id=0> 0", "<id=1;ts=600000000000> 3")})
    };

    auto boundaries = std::vector<TUnversionedOwningRow>{
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0")),
        TUnversionedOwningRow(BuildUnversionedRow("<id=0> 0"))
    };

    auto config = New<TRetentionConfig>();
    config->MinDataVersions = 2;
    config->MinDataTtl = TimestampToDuration(600000000000ULL);
    config->MaxDataTtl = TimestampToDuration(600000000000ULL);

    auto merger = GetTypicalMerger(ERowMergerType::New, config, 10000000000000ULL, 0);

    auto reader = CreateVersionedOverlappingRangeReader(
        boundaries,
        std::move(merger),
        [readers] (int index) {
            return readers[index];
        },
        CompareValueRanges,
        1);

    std::vector<TVersionedRow> result;
    ReadAll(reader, &result);

    EXPECT_EQ(1u, result.size());
    EXPECT_EQ(
        TIdentityComparableVersionedRow{BuildVersionedRow(
            "<id=0> 0", "<id=1;ts=900000000000> 2")},
        TIdentityComparableVersionedRow{result[0]});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
