#include <yt/yt/flow/lib/serializer/state.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NYsonSerializer {

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TTestYsonStateSub
    : public virtual TYsonStruct
{
    i64 Value;

    REGISTER_YSON_STRUCT(TTestYsonStateSub);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

using TTestYsonStateSubPtr = TIntrusivePtr<TTestYsonStateSub>;

struct TTestYsonState
    : public virtual TYsonStruct
{
    TTestYsonStateSubPtr Simple;
    TTestYsonStateSubPtr Compressed;
    TTestYsonStateSubPtr Packable;

    REGISTER_YSON_STRUCT(TTestYsonState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("simple", &TThis::Simple)
            .DefaultNew()
            .AddOption(EYsonStateValueType::Simple);
        registrar.Parameter("compressed", &TThis::Compressed)
            .DefaultNew()
            .AddOption(EYsonStateValueType::Compressed);
        registrar.Parameter("packable", &TThis::Packable)
            .DefaultNew()
            .AddOption(EYsonStateValueType::Packable);
    }
};

using TTestYsonStatePtr = TIntrusivePtr<TTestYsonState>;

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStateTest, Schema)
{
    auto stateSchema = GetYsonStateSchema<TTestYsonState>();

    auto expectedYsonSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("compressed", ESimpleLogicalValueType::Any).SetRequired(false),
            TColumnSchema("packable", ESimpleLogicalValueType::Any).SetRequired(false),
            TColumnSchema("simple", ESimpleLogicalValueType::Any).SetRequired(false)});

    EXPECT_EQ(*stateSchema->YsonSchema, *expectedYsonSchema);

    auto expectedTableSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("compressed", ESimpleLogicalValueType::String).SetRequired(false),
            TColumnSchema("format", ESimpleLogicalValueType::Any).SetRequired(false),
            TColumnSchema("packable", ESimpleLogicalValueType::String).SetRequired(false),
            TColumnSchema("packable_patch", ESimpleLogicalValueType::String).SetRequired(false),
            TColumnSchema("simple", ESimpleLogicalValueType::Any).SetRequired(false)});

    EXPECT_EQ(*stateSchema->TableSchema, *expectedTableSchema);

    std::vector<std::pair<i64, i64>> expectedMapping = {{0, -1}, {2, 3}, {4, -1}};
    EXPECT_EQ(stateSchema->YsonToTableMapping, expectedMapping);
    THashSet<i64> expectedCompressedColumns = {0, 2};
    EXPECT_EQ(stateSchema->CompressedColumns, expectedCompressedColumns);
    THashSet<i64> expectedCompressedPatchColumns = {3};
    EXPECT_EQ(stateSchema->CompressedPatchColumns, expectedCompressedPatchColumns);
    EXPECT_EQ(stateSchema->FormatColumn, std::optional(1));
}

////////////////////////////////////////////////////////////////////////////////

std::optional<TUnversionedOwningRow> ApplyMutation(const std::optional<TUnversionedOwningRow>& row, const TStateMutation& mutation)
{
    if (auto* update = std::get_if<TUpdateMutation>(&mutation)) {
        THashMap<int, TUnversionedValue> result;
        if (row) {
            for (const auto& value : *row) {
                result[value.Id] = value;
            }
        }
        for (const auto& value : *update) {
            result[value.Id] = value;
        }
        return {TRange(std::move(GetValues(result)))};
    } else if (std::get_if<TEraseMutation>(&mutation)) {
        return std::nullopt;
    } else if (std::get_if<TEmptyMutation>(&mutation)) {
        return row;
    } else {
        Y_ABORT();
    }
}

#define UPDATE_STATE()                                                             \
    auto oldRow = state->GetTableRow();                                            \
    state->SetValue(ysonState);                                                    \
    auto mutation = state->FlushMutation();                                        \
    auto remoteRow = ApplyMutation(oldRow, mutation);                              \
    auto currentRow = state->GetTableRow();                                        \
    if (currentRow && remoteRow) {                                                 \
        THashMap<int, std::pair<TUnversionedValue, TUnversionedValue>> values;     \
        for (const auto& value : *currentRow) {                                    \
            if (value.Type != EValueType::Null) {                                  \
                values[value.Id].first = value;                                    \
            }                                                                      \
        }                                                                          \
        for (const auto& value : *remoteRow) {                                     \
            if (value.Type != EValueType::Null) {                                  \
                values[value.Id].second = value;                                   \
            }                                                                      \
        }                                                                          \
        for (const auto& [k, v] : values) {                                        \
            if (v.first.Type == v.second.Type && IsStringLikeType(v.first.Type)) { \
                EXPECT_EQ(v.first.AsStringBuf(), v.second.AsStringBuf());          \
            } else {                                                               \
                EXPECT_EQ(v.first, v.second);                                      \
            }                                                                      \
        }                                                                          \
    } else {                                                                       \
        EXPECT_EQ(currentRow.has_value(), remoteRow.has_value());                  \
    }                                                                              \
    {                                                                              \
        auto state = New<TState>(stateSchema);                                     \
        state->Init(remoteRow);                                                    \
        auto got = state->GetValueAs<decltype(ysonState)::TUnderlying>();          \
        EXPECT_EQ(*got, *ysonState);                                               \
    }

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStateTest, FullMutation)
{
    auto stateSchema = GetYsonStateSchema<TTestYsonState>();

    auto state = New<TState>(stateSchema);
    EXPECT_EQ(state->GetTableRow(), std::nullopt);
    auto ysonState = state->GetValueAs<TTestYsonState>();
    ysonState->Simple->Value = 123;
    ysonState->Compressed->Value = 234;
    ysonState->Packable->Value = 345;
    state->SetValue(ysonState);
    auto mutation = state->FlushMutation();
    ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
    EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 4);

    auto serialized = state->GetTableRow();

    auto otherState = New<TState>(stateSchema);
    otherState->Init(serialized);
    auto otherYsonState = otherState->GetValueAs<TTestYsonState>();
    EXPECT_EQ(otherYsonState->Simple->Value, 123);
    EXPECT_EQ(otherYsonState->Compressed->Value, 234);
    EXPECT_EQ(otherYsonState->Packable->Value, 345);
}

TEST(TYsonStateTest, RepeatMutation)
{

    auto stateSchema = GetYsonStateSchema<TTestYsonState>();

    auto state = New<TState>(stateSchema);
    EXPECT_EQ(state->GetTableRow(), std::nullopt);
    auto ysonState = state->GetValueAs<TTestYsonState>();
    ysonState->Simple->Value = 123;
    ysonState->Compressed->Value = 234;
    ysonState->Packable->Value = 345;

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(state->GetTableRow().value().GetCount(), 5);
    }
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }

    ysonState->SetDefaults();
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEraseMutation>(&mutation));
    }
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStateTest, Update)
{
    auto stateSchema = GetYsonStateSchema<TTestYsonState>();

    auto state = New<TState>(stateSchema);
    auto ysonState = state->GetValueAs<TTestYsonState>();
    ysonState->Simple->Value = 123;
    ysonState->Compressed->Value = 234;
    ysonState->Packable->Value = 345;

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 4);
    }

    ysonState->Simple->Value = 456;

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 1);
    }

    ysonState->Compressed->Value = 567;

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 1);
    }

    ysonState->Packable->Value = 678;

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 1);
    }

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }

    auto newFormat = New<TFormat>();
    EXPECT_NE(newFormat->Compression, NCompression::ECodec::Lz4);
    newFormat->Compression = NCompression::ECodec::Lz4;
    state->SetFormat(newFormat);

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        EXPECT_EQ(std::get<TUpdateMutation>(mutation).GetCount(), 5);
    }

    ysonState->SetDefaults();
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEraseMutation>(&mutation));
    }

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }

    state->ForceRewrite();
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEraseMutation>(&mutation));
    }
    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TTestDefaultYsonState
    : public virtual TYsonStruct
{
    std::string Simple;
    std::string SimpleWithDefault;
    std::string Packable;
    std::string PackableWithDefault;
    std::string Compressed;
    std::string CompressedWithDefault;

    REGISTER_YSON_STRUCT(TTestDefaultYsonState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("simple", &TThis::Simple)
            .Default()
            .AddOption(EYsonStateValueType::Simple);
        registrar.Parameter("simple_with_default", &TThis::SimpleWithDefault)
            .Default("default1")
            .AddOption(EYsonStateValueType::Simple);
        registrar.Parameter("compressed", &TThis::Compressed)
            .Default()
            .AddOption(EYsonStateValueType::Compressed);
        registrar.Parameter("compressed_with_default", &TThis::CompressedWithDefault)
            .Default("default2")
            .AddOption(EYsonStateValueType::Compressed);
        registrar.Parameter("packable", &TThis::Packable)
            .Default()
            .AddOption(EYsonStateValueType::Packable);
        registrar.Parameter("packable_with_default", &TThis::PackableWithDefault)
            .Default("default3")
            .AddOption(EYsonStateValueType::Packable);
    }
};

using TTestDefaultYsonStatePtr = TIntrusivePtr<TTestDefaultYsonState>;

TEST(TYsonStateTest, Default)
{
    const auto defaultYsonStruct = New<TTestDefaultYsonState>();
    auto stateSchema = GetYsonStateSchema<TTestDefaultYsonState>();
    auto state = New<TState>(stateSchema);
    auto ysonState = state->GetValueAs<TTestDefaultYsonState>();
    EXPECT_EQ(*ysonState, *defaultYsonStruct);
    EXPECT_EQ(state->GetTableRow(), std::nullopt);

    {
        UPDATE_STATE();
        ASSERT_TRUE(std::get_if<TEmptyMutation>(&mutation));
    }

    ysonState->Simple = "simple";
    {
        UPDATE_STATE();
        EXPECT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        auto row = state->GetTableRow();
        auto schema = state->GetTableSchema();
        ASSERT_TRUE(row);
        EXPECT_EQ(FromUnversionedValue<std::optional<TStringBuf>>((*row)[schema->GetColumnIndexOrThrow("simple")]), "simple");
        EXPECT_EQ(FromUnversionedValue<std::optional<TStringBuf>>((*row)[schema->GetColumnIndexOrThrow("simple_with_default")]), "default1");
    }

    ysonState->Simple = "";
    {
        UPDATE_STATE();
        EXPECT_TRUE(std::get_if<TEraseMutation>(&mutation));
        EXPECT_EQ(state->GetTableRow(), std::nullopt);
    }

    ysonState->PackableWithDefault = "123";
    {
        UPDATE_STATE();
        EXPECT_TRUE(std::get_if<TUpdateMutation>(&mutation));
        auto row = state->GetTableRow();
        auto schema = state->GetTableSchema();
        ASSERT_TRUE(row);

        EXPECT_EQ(FromUnversionedValue<std::optional<TStringBuf>>((*row)[schema->GetColumnIndexOrThrow("simple")]), "");
        EXPECT_EQ(FromUnversionedValue<std::optional<TStringBuf>>((*row)[schema->GetColumnIndexOrThrow("simple_with_default")]), "default1");
    }

    ysonState->PackableWithDefault = "default3";
    {
        UPDATE_STATE();
        EXPECT_TRUE(std::get_if<TEraseMutation>(&mutation));
        EXPECT_EQ(state->GetTableRow(), std::nullopt);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NYsonSerializer
