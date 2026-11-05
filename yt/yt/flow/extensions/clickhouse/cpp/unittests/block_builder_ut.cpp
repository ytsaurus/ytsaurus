#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/block_builder.h>
#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/columns/bool.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/date.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/lowcardinality.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/nullable.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/numeric.h>
#include <contrib/libs/clickhouse-cpp/clickhouse/columns/string.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TResolvedColumn Col(
    std::string name,
    EClickHouseBaseType base,
    bool nullable = false,
    bool lowCardinality = false,
    std::optional<int> fixedLength = std::nullopt)
{
    return TResolvedColumn{
        .Name = std::move(name),
        .Type = TClickHouseColumnType{
            .Base = base,
            .Nullable = nullable,
            .LowCardinality = lowCardinality,
            .FixedStringLength = fixedLength,
        },
    };
}

TColumnSchema Required(const std::string& name, ESimpleLogicalValueType type)
{
    return TColumnSchema(name, SimpleLogicalType(type));
}

TColumnSchema Optional(const std::string& name, ESimpleLogicalValueType type)
{
    return TColumnSchema(name, OptionalLogicalType(SimpleLogicalType(type)));
}

TComputationStreamSpecStoragePtr MakeStreamSpecStorage(const TTableSchemaPtr& schema)
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[TStreamId("test")][TStreamSpecId(1)] = streamSpec;
    return New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(specs),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
}

template <typename TSetter>
TOutputMessageConstPtr MakeMessage(
    const TComputationStreamSpecStoragePtr& specStorage,
    const TTableSchemaPtr& schema,
    i64 id,
    TSetter&& setter)
{
    TMessageBuilder builder("test", schema);
    builder.SetMessageId(TMessageId(LexicographicallySerialize(id)));
    builder.SetSystemTimestamp(TSystemTimestamp(1700000000));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1700000000));
    builder.SetEventTimestamp(TSystemTimestamp(1700000000));
    setter(builder.Payload());
    return New<TOutputMessage>(builder.Finish(), specStorage);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseTypeParserTest, Scalars)
{
    EXPECT_EQ(ParseClickHouseType("Int64").Base, EClickHouseBaseType::Int64);
    EXPECT_EQ(ParseClickHouseType("UInt8").Base, EClickHouseBaseType::UInt8);
    EXPECT_EQ(ParseClickHouseType("Float32").Base, EClickHouseBaseType::Float32);
    EXPECT_EQ(ParseClickHouseType("String").Base, EClickHouseBaseType::String);
    EXPECT_EQ(ParseClickHouseType("Bool").Base, EClickHouseBaseType::Bool);
    EXPECT_EQ(ParseClickHouseType("Date").Base, EClickHouseBaseType::Date);
    EXPECT_EQ(ParseClickHouseType("DateTime").Base, EClickHouseBaseType::DateTime);
    EXPECT_EQ(ParseClickHouseType("DateTime('UTC')").Base, EClickHouseBaseType::DateTime);
    EXPECT_FALSE(ParseClickHouseType("Int64").Nullable);
    EXPECT_FALSE(ParseClickHouseType("Int64").LowCardinality);
}

TEST(TClickHouseTypeParserTest, Wrappers)
{
    auto nullable = ParseClickHouseType("Nullable(String)");
    EXPECT_TRUE(nullable.Nullable);
    EXPECT_FALSE(nullable.LowCardinality);
    EXPECT_EQ(nullable.Base, EClickHouseBaseType::String);

    auto lowCardinality = ParseClickHouseType("LowCardinality(String)");
    EXPECT_TRUE(lowCardinality.LowCardinality);
    EXPECT_FALSE(lowCardinality.Nullable);
    EXPECT_EQ(lowCardinality.Base, EClickHouseBaseType::String);

    auto both = ParseClickHouseType("LowCardinality(Nullable(String))");
    EXPECT_TRUE(both.LowCardinality);
    EXPECT_TRUE(both.Nullable);
    EXPECT_EQ(both.Base, EClickHouseBaseType::String);
}

TEST(TClickHouseTypeParserTest, FixedString)
{
    auto fixed = ParseClickHouseType("FixedString(16)");
    EXPECT_EQ(fixed.Base, EClickHouseBaseType::FixedString);
    ASSERT_TRUE(fixed.FixedStringLength.has_value());
    EXPECT_EQ(*fixed.FixedStringLength, 16);

    auto nullableFixed = ParseClickHouseType("Nullable(FixedString(8))");
    EXPECT_TRUE(nullableFixed.Nullable);
    EXPECT_EQ(nullableFixed.Base, EClickHouseBaseType::FixedString);
    EXPECT_EQ(*nullableFixed.FixedStringLength, 8);
}

TEST(TClickHouseTypeParserTest, RejectsUnsupported)
{
    EXPECT_THROW(ParseClickHouseType("Decimal(10, 2)"), std::exception);
    EXPECT_THROW(ParseClickHouseType("DateTime64(3)"), std::exception);
    EXPECT_THROW(ParseClickHouseType("Array(Int64)"), std::exception);
    EXPECT_THROW(ParseClickHouseType("LowCardinality(Int64)"), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseResolveColumnsTest, SkipsMaterializedAndMissingKeepsOrder)
{
    auto schema = New<TTableSchema>(std::vector{
        Required("a", ESimpleLogicalValueType::Int64),
        Required("c", ESimpleLogicalValueType::String),
        Required("extra", ESimpleLogicalValueType::Int64),
    });
    std::vector<TClickHouseTableColumn> tableColumns{
        {.Name = "a", .Type = "Int64", .DefaultKind = ""},
        {.Name = "m", .Type = "Int64", .DefaultKind = "MATERIALIZED"},
        {.Name = "l", .Type = "Int64", .DefaultKind = "ALIAS"},
        {.Name = "b", .Type = "Int64", .DefaultKind = "DEFAULT"},
        {.Name = "c", .Type = "String", .DefaultKind = ""},
    };
    auto resolved = ResolveColumns(tableColumns, schema);
    ASSERT_EQ(resolved.size(), 2u);
    EXPECT_EQ(resolved[0].Name, "a");
    EXPECT_EQ(resolved[1].Name, "c");
}

TEST(TClickHouseResolveColumnsTest, RejectsTypeMismatch)
{
    auto schema = New<TTableSchema>(std::vector{
        Required("a", ESimpleLogicalValueType::String),
    });
    std::vector<TClickHouseTableColumn> tableColumns{
        {.Name = "a", .Type = "Int64", .DefaultKind = ""},
    };
    EXPECT_THROW(ResolveColumns(tableColumns, schema), std::exception);
}

TEST(TClickHouseResolveColumnsTest, NullableRequiresOptionalStreamColumn)
{
    std::vector<TClickHouseTableColumn> tableColumns{
        {.Name = "a", .Type = "Nullable(Int64)", .DefaultKind = ""},
    };

    auto required = New<TTableSchema>(std::vector{
        Required("a", ESimpleLogicalValueType::Int64),
    });
    EXPECT_THROW(ResolveColumns(tableColumns, required), std::exception);

    auto optional = New<TTableSchema>(std::vector{
        Optional("a", ESimpleLogicalValueType::Int64),
    });
    auto resolved = ResolveColumns(tableColumns, optional);
    ASSERT_EQ(resolved.size(), 1u);
    EXPECT_TRUE(resolved[0].Type.Nullable);
}

TEST(TClickHouseResolveColumnsTest, NonNullableRejectsOptionalStreamColumn)
{
    auto optional = New<TTableSchema>(std::vector{
        TColumnSchema("a", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });
    std::vector<TClickHouseTableColumn> tableColumns{
        {.Name = "a", .Type = "Int64", .DefaultKind = ""},
    };
    EXPECT_THROW(ResolveColumns(tableColumns, optional), std::exception);
}

TEST(TClickHouseResolveColumnsTest, ThrowsWhenNothingResolves)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("x", ESimpleLogicalValueType::Int64),
    });
    std::vector<TClickHouseTableColumn> tableColumns{
        {.Name = "a", .Type = "Int64", .DefaultKind = ""},
    };
    EXPECT_THROW(ResolveColumns(tableColumns, schema), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseBlockBuilderTest, AllScalarTypes)
{
    auto required = [] (std::string name, ESimpleLogicalValueType type) {
        return TColumnSchema(name, SimpleLogicalType(type));
    };
    auto schema = New<TTableSchema>(std::vector{
        required("i8", ESimpleLogicalValueType::Int8),
        required("i16", ESimpleLogicalValueType::Int16),
        required("i32", ESimpleLogicalValueType::Int32),
        required("i64", ESimpleLogicalValueType::Int64),
        required("u8", ESimpleLogicalValueType::Uint8),
        required("u16", ESimpleLogicalValueType::Uint16),
        required("u32", ESimpleLogicalValueType::Uint32),
        required("u64", ESimpleLogicalValueType::Uint64),
        required("f32", ESimpleLogicalValueType::Float),
        required("f64", ESimpleLogicalValueType::Double),
        required("str", ESimpleLogicalValueType::String),
        required("flag", ESimpleLogicalValueType::Boolean),
        required("d", ESimpleLogicalValueType::Date),
        required("dt", ESimpleLogicalValueType::Datetime),
    });
    auto specStorage = MakeStreamSpecStorage(schema);

    std::vector<TResolvedColumn> columns{
        Col("i8", EClickHouseBaseType::Int8),
        Col("i16", EClickHouseBaseType::Int16),
        Col("i32", EClickHouseBaseType::Int32),
        Col("i64", EClickHouseBaseType::Int64),
        Col("u8", EClickHouseBaseType::UInt8),
        Col("u16", EClickHouseBaseType::UInt16),
        Col("u32", EClickHouseBaseType::UInt32),
        Col("u64", EClickHouseBaseType::UInt64),
        Col("f32", EClickHouseBaseType::Float32),
        Col("f64", EClickHouseBaseType::Float64),
        Col("str", EClickHouseBaseType::String),
        Col("flag", EClickHouseBaseType::Bool),
        Col("d", EClickHouseBaseType::Date),
        Col("dt", EClickHouseBaseType::DateTime),
    };

    auto message = MakeMessage(specStorage, schema, 0, [] (TPayloadBuilder& payload) {
        payload.Set<i8>(-8, "i8");
        payload.Set<i16>(-16, "i16");
        payload.Set<i32>(-32, "i32");
        payload.Set<i64>(-64, "i64");
        payload.Set<ui8>(8, "u8");
        payload.Set<ui16>(16, "u16");
        payload.Set<ui32>(32, "u32");
        payload.Set<ui64>(64, "u64");
        payload.Set<float>(1.5f, "f32");
        payload.Set<double>(2.5, "f64");
        payload.Set<std::string>("hello", "str");
        payload.Set<bool>(true, "flag");
        payload.Set<ui16>(19000, "d");
        payload.Set<ui32>(1700000000, "dt");
    });

    TClickHouseBlockBuilder builder(columns);
    auto block = builder.Build({message});

    EXPECT_EQ(block.GetColumnCount(), 14u);
    EXPECT_EQ(block.GetRowCount(), 1u);

    EXPECT_EQ(block[0]->As<clickhouse::ColumnInt8>()->At(0), -8);
    EXPECT_EQ(block[3]->As<clickhouse::ColumnInt64>()->At(0), -64);
    EXPECT_EQ(block[4]->As<clickhouse::ColumnUInt8>()->At(0), 8u);
    EXPECT_EQ(block[7]->As<clickhouse::ColumnUInt64>()->At(0), 64u);
    EXPECT_FLOAT_EQ(block[8]->As<clickhouse::ColumnFloat32>()->At(0), 1.5f);
    EXPECT_DOUBLE_EQ(block[9]->As<clickhouse::ColumnFloat64>()->At(0), 2.5);
    EXPECT_EQ(block[10]->As<clickhouse::ColumnString>()->At(0), "hello");
    EXPECT_TRUE(block[11]->As<clickhouse::ColumnBool>()->At(0));
    EXPECT_EQ(block[12]->As<clickhouse::ColumnDate>()->RawAt(0), 19000u);
    EXPECT_EQ(block[13]->As<clickhouse::ColumnDateTime>()->RawAt(0), 1700000000u);

    EXPECT_EQ(block.GetColumnName(0), "i8");
    EXPECT_EQ(block.GetColumnName(13), "dt");
}

TEST(TClickHouseBlockBuilderTest, NullableColumn)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });
    auto specStorage = MakeStreamSpecStorage(schema);

    auto present = MakeMessage(specStorage, schema, 0, [] (TPayloadBuilder& payload) {
        payload.Set<i64>(42, "value");
    });
    auto missing = MakeMessage(specStorage, schema, 1, [] (TPayloadBuilder& /*payload*/) {
    });

    std::vector<TResolvedColumn> columns{Col("value", EClickHouseBaseType::Int64, /*nullable*/ true)};
    TClickHouseBlockBuilder builder(columns);
    auto block = builder.Build({present, missing});

    ASSERT_EQ(block.GetRowCount(), 2u);
    auto nullable = block[0]->As<clickhouse::ColumnNullable>();
    EXPECT_FALSE(nullable->IsNull(0));
    EXPECT_TRUE(nullable->IsNull(1));
    EXPECT_EQ(nullable->Nested()->As<clickhouse::ColumnInt64>()->At(0), 42);
}

TEST(TClickHouseBlockBuilderTest, FixedStringColumn)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("code", ESimpleLogicalValueType::String),
    });
    auto specStorage = MakeStreamSpecStorage(schema);
    auto message = MakeMessage(specStorage, schema, 0, [] (TPayloadBuilder& payload) {
        payload.Set<std::string>("ab", "code");
    });

    std::vector<TResolvedColumn> columns{
        Col("code", EClickHouseBaseType::FixedString, /*nullable*/ false, /*lowCardinality*/ false, /*fixedLength*/ 4),
    };
    TClickHouseBlockBuilder builder(columns);
    auto block = builder.Build({message});

    auto fixed = block[0]->As<clickhouse::ColumnFixedString>();
    ASSERT_EQ(fixed->Size(), 1u);
    auto stored = fixed->At(0);
    EXPECT_EQ(stored.size(), 4u);
    EXPECT_EQ(stored.substr(0, 2), "ab");
}

TEST(TClickHouseBlockBuilderTest, LowCardinalityColumn)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("category", ESimpleLogicalValueType::String),
    });
    auto specStorage = MakeStreamSpecStorage(schema);

    std::vector<TOutputMessageConstPtr> messages;
    for (auto value : {"x", "y", "x"}) {
        messages.push_back(MakeMessage(specStorage, schema, std::ssize(messages), [&] (TPayloadBuilder& payload) {
            payload.Set<std::string>(value, "category");
        }));
    }

    std::vector<TResolvedColumn> columns{
        Col("category", EClickHouseBaseType::String, /*nullable*/ false, /*lowCardinality*/ true),
    };
    TClickHouseBlockBuilder builder(columns);
    auto block = builder.Build(messages);

    auto lowCardinality = block[0]->As<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    ASSERT_EQ(lowCardinality->Size(), 3u);
    EXPECT_EQ(lowCardinality->At(0), "x");
    EXPECT_EQ(lowCardinality->At(1), "y");
    EXPECT_EQ(lowCardinality->At(2), "x");
}

////////////////////////////////////////////////////////////////////////////////

TOutputMessageConstPtr MakeIntMessage(
    const TComputationStreamSpecStoragePtr& specStorage,
    const TTableSchemaPtr& schema,
    i64 id)
{
    return MakeMessage(specStorage, schema, id, [&] (TPayloadBuilder& payload) {
        payload.Set<i64>(id, "value");
    });
}

TEST(TClickHouseDedupTokenTest, MaxMessageIdRegardlessOfOrder)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", ESimpleLogicalValueType::Int64),
    });
    auto specStorage = MakeStreamSpecStorage(schema);

    std::vector<TOutputMessageConstPtr> ascending{
        MakeIntMessage(specStorage, schema, 0),
        MakeIntMessage(specStorage, schema, 1),
        MakeIntMessage(specStorage, schema, 2),
    };
    std::vector<TOutputMessageConstPtr> shuffled{
        MakeIntMessage(specStorage, schema, 2),
        MakeIntMessage(specStorage, schema, 0),
        MakeIntMessage(specStorage, schema, 1),
    };

    auto expected = std::string(TMessageId(LexicographicallySerialize(i64(2))).Underlying());
    EXPECT_EQ(BuildDedupToken(ascending), expected);
    EXPECT_EQ(BuildDedupToken(shuffled), expected);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseInsertHeaderTest, SettingsBeforeValues)
{
    std::vector<TResolvedColumn> columns{
        Col("a", EClickHouseBaseType::Int64),
        Col("b", EClickHouseBaseType::String),
    };

    auto header = BuildInsertHeader("db", "tbl", columns, /*asyncInsert*/ false, "tok-123");
    EXPECT_EQ(
        header,
        "INSERT INTO `db`.`tbl` (`a`,`b`) SETTINGS insert_deduplication_token='tok-123' VALUES");
}

TEST(TClickHouseInsertHeaderTest, AsyncInsertAddsSettings)
{
    std::vector<TResolvedColumn> columns{Col("a", EClickHouseBaseType::Int64)};

    auto header = BuildInsertHeader("db", "tbl", columns, /*asyncInsert*/ true, "tok");
    EXPECT_EQ(
        header,
        "INSERT INTO `db`.`tbl` (`a`) SETTINGS insert_deduplication_token='tok', "
        "async_insert=1, async_insert_deduplicate=1, wait_for_async_insert=1 VALUES");
}

TEST(TClickHouseInsertHeaderTest, EscapesQuoteInToken)
{
    std::vector<TResolvedColumn> columns{Col("a", EClickHouseBaseType::Int64)};

    auto header = BuildInsertHeader("db", "tbl", columns, /*asyncInsert*/ false, "a'b");
    EXPECT_NE(header.find("insert_deduplication_token='a\\'b'"), std::string::npos);
}

TEST(TClickHouseInsertHeaderTest, NoTokenOmitsSettings)
{
    std::vector<TResolvedColumn> columns{Col("a", EClickHouseBaseType::Int64)};

    auto header = BuildInsertHeader("db", "tbl", columns, /*asyncInsert*/ false, std::nullopt);
    EXPECT_EQ(header, "INSERT INTO `db`.`tbl` (`a`) VALUES");
}

TEST(TClickHouseInsertHeaderTest, AsyncInsertWithoutTokenHasNoDedup)
{
    std::vector<TResolvedColumn> columns{Col("a", EClickHouseBaseType::Int64)};

    auto header = BuildInsertHeader("db", "tbl", columns, /*asyncInsert*/ true, std::nullopt);
    EXPECT_EQ(
        header,
        "INSERT INTO `db`.`tbl` (`a`) SETTINGS async_insert=1, wait_for_async_insert=1 VALUES");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClickHouseEngineTest, BlockDeduplicatingEngines)
{
    EXPECT_TRUE(IsBlockDeduplicatingEngine("ReplicatedMergeTree"));
    EXPECT_TRUE(IsBlockDeduplicatingEngine("ReplicatedReplacingMergeTree"));
    EXPECT_TRUE(IsBlockDeduplicatingEngine("SharedMergeTree"));
    EXPECT_FALSE(IsBlockDeduplicatingEngine("MergeTree"));
    EXPECT_FALSE(IsBlockDeduplicatingEngine("Log"));
}

TEST(TClickHouseEngineTest, PlainMergeTreeEngines)
{
    EXPECT_TRUE(IsPlainMergeTreeEngine("MergeTree"));
    EXPECT_TRUE(IsPlainMergeTreeEngine("ReplacingMergeTree"));
    EXPECT_FALSE(IsPlainMergeTreeEngine("ReplicatedMergeTree"));
    EXPECT_FALSE(IsPlainMergeTreeEngine("Log"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
