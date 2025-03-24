#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/lib/serializer/proto_yson_struct.h>
#include <yt/yt/flow/lib/serializer/serializer.h>

#include <yt/yt/flow/lib/serializer/unittests/proto/test.pb.h>

namespace NYT::NFlow {

namespace NProto {

bool operator==(const TTestMessage& lhs, const TTestMessage& rhs)
{
    return lhs.GetInt32() == rhs.GetInt32() && lhs.GetString() == rhs.GetString();
}

} // namespace NYT::NFlow::NProto

namespace {

using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TTestSubStruct
    : public TYsonStruct
{
    ui32 Uint;

    bool operator==(const TTestSubStruct& other) const noexcept
    {
        return Uint == other.Uint;
    }

    REGISTER_YSON_STRUCT(TTestSubStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uint", &TThis::Uint)
            .Default();
    }
};

using TTestSubStructPtr = TIntrusivePtr<TTestSubStruct>;

struct TTestSubStructLite
    : public TYsonStructLite
{
    i32 Int;

    bool operator==(const TTestSubStructLite& other) const noexcept
    {
        return Int == other.Int;
    }

    REGISTER_YSON_STRUCT_LITE(TTestSubStructLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int", &TThis::Int)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestYsonStruct
    : public TYsonStruct
{
    TString String;
    TTestSubStructPtr Sub;
    std::vector<TTestSubStructLite> SubList;
    std::vector<TString> StringList;
    std::unordered_map<TString, int> IntMap;
    std::optional<i64> NullableInt;
    unsigned int Uint;
    bool Bool;
    char Char;
    i8 Byte;
    ui8 Ubyte;
    i16 Short;
    ui16 Ushort;
    TProtoYsonStruct<NProto::TTestMessage> Proto;

    REGISTER_YSON_STRUCT(TTestYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string", &TThis::String);
        registrar.Parameter("nullable_sub", &TThis::Sub)
            .DefaultNew();
        registrar.Parameter("sub_list", &TThis::SubList)
            .Default();
        registrar.Parameter("int_map", &TThis::IntMap)
            .Default();
        registrar.Parameter("string_list", &TThis::StringList)
            .Default();
        registrar.Parameter("nullable_int", &TThis::NullableInt)
            .Default();
        registrar.Parameter("uint", &TThis::Uint)
            .Default();
        registrar.Parameter("bool", &TThis::Bool)
            .Default();
        registrar.Parameter("char", &TThis::Char)
            .Default();
        registrar.Parameter("byte", &TThis::Byte)
            .Default();
        registrar.Parameter("ubyte", &TThis::Ubyte)
            .Default();
        registrar.Parameter("short", &TThis::Short)
            .Default();
        registrar.Parameter("ushort", &TThis::Ushort)
            .Default();
        registrar.Parameter("proto", &TThis::Proto)
            .Default();
    }
};

using TTestYsonStructPtr = TIntrusivePtr<TTestYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

struct TTestStructV1
    : public TYsonStruct
{
    i32 Int;

    bool operator==(const TTestStructV1& other) const noexcept
    {
        return Int == other.Int;
    }

    REGISTER_YSON_STRUCT(TTestStructV1);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int", &TThis::Int)
            .Default(5);
    }
};

struct TTestStructV2
    : public TYsonStruct
{
    i32 Int;
    TString String;

    bool operator==(const TTestStructV2& other) const noexcept
    {
        return Int == other.Int && String == other.String;
    }

    REGISTER_YSON_STRUCT(TTestStructV2);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int", &TThis::Int)
            .Default(55);
        registrar.Parameter("string", &TThis::String)
            .Default("default");
    }
};

struct TTestStructV2WithoutDefault
    : public TYsonStruct
{
    i32 Int;
    TString String;

    REGISTER_YSON_STRUCT(TTestStructV2WithoutDefault);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int", &TThis::Int);
        registrar.Parameter("string", &TThis::String);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializer, Simple)
{
    auto ysonStruct = New<TTestYsonStruct>();
    ysonStruct->IntMap["0"] = 0;
    ysonStruct->IntMap["1"] = 1;
    ysonStruct->Proto->SetString("str");
    ysonStruct->Proto->SetInt32(5);
    ysonStruct->Bool = true;
    ysonStruct->SubList.emplace_back().Int = 5;
    ysonStruct->StringList.push_back("a");
    ysonStruct->StringList.push_back("b");

    auto schema = TSerializer::GetSchema(ysonStruct);
    auto row = TSerializer::Serialize(ysonStruct, schema);

    auto copy = New<TTestYsonStruct>();
    TSerializer::Deserialize(copy, row, schema);

    EXPECT_EQ(ysonStruct->IntMap, copy->IntMap);
    // EXPECT_EQ(ysonStruct->Proto, copy->Proto);
    EXPECT_EQ(ysonStruct->Bool, copy->Bool);
    EXPECT_EQ(ysonStruct->SubList, copy->SubList);
    EXPECT_EQ(ysonStruct->StringList, copy->StringList);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializer, AddField)
{
    auto structV1 = New<TTestStructV1>();
    structV1->Int = 5;

    auto schema = TSerializer::GetSchema(structV1);
    auto row = TSerializer::Serialize(structV1, schema);

    auto structV2 = New<TTestStructV2>();
    TSerializer::Deserialize(structV2, row, schema);

    EXPECT_EQ(structV1->Int, structV2->Int);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializer, DeleteField)
{
    auto structV2 = New<TTestStructV2>();
    structV2->Int = 5;
    structV2->String = "abcde";

    auto schema = TSerializer::GetSchema(structV2);
    auto row = TSerializer::Serialize(structV2, schema);

    auto structV1 = New<TTestStructV1>();
    TSerializer::Deserialize(structV1, row, schema);

    EXPECT_EQ(structV1->Int, structV2->Int);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializer, WithoutDefault)
{
    {
        auto structV1 = New<TTestStructV2>();

        auto schema = TSerializer::GetSchema(structV1);
        auto row = TSerializer::Serialize(structV1, schema);

        auto structV2 = New<TTestStructV2WithoutDefault>();
        TSerializer::Deserialize(structV2, row, schema);

        EXPECT_EQ(structV1->Int, structV2->Int);
        EXPECT_EQ(structV1->String, structV2->String);
    }
    {
        auto structV1 = New<TTestStructV2WithoutDefault>();

        auto schema = TSerializer::GetSchema(structV1);
        auto row = TSerializer::Serialize(structV1, schema);

        auto structV2 = New<TTestStructV2>();
        TSerializer::Deserialize(structV2, row, schema);

        EXPECT_EQ(structV1->Int, structV2->Int);
        EXPECT_EQ(structV1->String, structV2->String);
    }
    {
        auto structV1 = New<TTestStructV1>();

        auto schema = TSerializer::GetSchema(structV1);
        auto row = TSerializer::Serialize(structV1, schema);

        auto structV2 = New<TTestStructV2WithoutDefault>();
        EXPECT_THROW(
            TSerializer::Deserialize(structV2, row, schema),
            TErrorException);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSerializer, ToTableSchema)
{
    auto ysonStruct = New<TTestYsonStruct>();
    auto schema = TSerializer::GetSchema(ysonStruct);
    auto tableSchema = ToTableSchema(schema);

    EXPECT_EQ(tableSchema->GetColumnCount(), 14);
    EXPECT_EQ(tableSchema->GetColumn("bool").GetWireType(), EValueType::Boolean);
    EXPECT_EQ(tableSchema->GetColumn("byte").GetWireType(), EValueType::Int64);
    EXPECT_EQ(tableSchema->GetColumn("char").GetWireType(), EValueType::Int64);
    EXPECT_EQ(tableSchema->GetColumn("int_map").GetWireType(), EValueType::Composite);
    EXPECT_EQ(tableSchema->GetColumn("nullable_int").GetWireType(), EValueType::Int64);
    EXPECT_EQ(tableSchema->GetColumn("nullable_sub").GetWireType(), EValueType::Composite);
    EXPECT_EQ(tableSchema->GetColumn("proto").GetWireType(), EValueType::String);
    EXPECT_EQ(tableSchema->GetColumn("short").GetWireType(), EValueType::Int64);
    EXPECT_EQ(tableSchema->GetColumn("string").GetWireType(), EValueType::String);
    EXPECT_EQ(tableSchema->GetColumn("string_list").GetWireType(), EValueType::Composite);
    EXPECT_EQ(tableSchema->GetColumn("sub_list").GetWireType(), EValueType::Composite);
    EXPECT_EQ(tableSchema->GetColumn("ubyte").GetWireType(), EValueType::Uint64);
    EXPECT_EQ(tableSchema->GetColumn("uint").GetWireType(), EValueType::Uint64);
    EXPECT_EQ(tableSchema->GetColumn("ushort").GetWireType(), EValueType::Uint64);

    for (const auto& column : tableSchema->Columns()) {
        if (column.Name().starts_with("nullable_")) {
            EXPECT_FALSE(column.Required()) << "column: " << column.Name();
        } else {
            EXPECT_TRUE(column.Required()) << "column: " << column.Name();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
