#include <yt/yt/flow/lib/serializer/serializer.h>

#include <yt/yt/flow/lib/serializer/unittests/proto/test.pb.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NFlow::NYsonSerializer {

namespace {

using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using google::protobuf::util::MessageDifferencer;

////////////////////////////////////////////////////////////////////////////////

struct TTestSubStruct
    : public virtual TYsonStruct
{
    ui32 Uint;

    REGISTER_YSON_STRUCT(TTestSubStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uint", &TThis::Uint)
            .Default();
    }
};

using TTestSubStructPtr = TIntrusivePtr<TTestSubStruct>;

struct TTestSubStructLite
    : public virtual TYsonStructLite
{
    i32 Int;

    REGISTER_YSON_STRUCT_LITE(TTestSubStructLite);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("int", &TThis::Int)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSchemaYsonStruct
    : public virtual TYsonStruct
{
    std::string String;
    TTestSubStructPtr Sub;
    TTestSubStructLite RequiredSub;
    std::vector<TTestSubStructLite> SubList;
    std::vector<std::string> StringList;
    std::unordered_map<std::string, int> IntMap;
    std::optional<std::unordered_map<std::string, int>> OptionalIntMap;
    std::optional<i64> NullableInt;
    unsigned int Uint;
    bool Bool;
    char Char;
    i8 Byte;
    ui8 Ubyte;
    i16 Short;
    ui16 Ushort;
    TProtoSerializedAsString<NProto::TTestMessage> Proto;
    NProto::TTestMessage YsonProto;
    std::tuple<double, std::string> Tuple;
    std::optional<NYson::TYsonString> Yson;
    TInstant Instant;

    REGISTER_YSON_STRUCT(TTestSchemaYsonStruct);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("string", &TThis::String)
            .Default();
        registrar.Parameter("nullable_sub", &TThis::Sub)
            .Default();
        registrar.Parameter("required_sub", &TThis::RequiredSub)
            .Default();
        registrar.Parameter("sub_list", &TThis::SubList)
            .Default();
        registrar.Parameter("int_map", &TThis::IntMap)
            .Default();
        registrar.Parameter("optional_int_map", &TThis::OptionalIntMap)
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
        registrar.Parameter("yson_proto", &TThis::YsonProto)
            .Default();
        registrar.Parameter("tuple", &TThis::Tuple)
            .Default();
        registrar.Parameter("yson", &TThis::Yson)
            .Default();
        registrar.Parameter("instant", &TThis::Instant)
            .Default();
    }
};

using TTestSchemaYsonStructPtr = TIntrusivePtr<TTestSchemaYsonStruct>;

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonSerializeTest, YsonTableSchema)
{
    auto ysonStruct = New<TTestSchemaYsonStruct>();
    auto schema = GetYsonSchema(ysonStruct);
    auto expectedSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("bool", ESimpleLogicalValueType::Boolean),
            TColumnSchema("byte", ESimpleLogicalValueType::Int8),
            TColumnSchema("char", ESimpleLogicalValueType::Int8),
            TColumnSchema("instant", ESimpleLogicalValueType::Timestamp),
            TColumnSchema("int_map", ESimpleLogicalValueType::Any),
            TColumnSchema("nullable_int", ESimpleLogicalValueType::Int64),
            TColumnSchema("nullable_sub", ESimpleLogicalValueType::Any),
            TColumnSchema("optional_int_map", ESimpleLogicalValueType::Any),
            TColumnSchema("proto", ESimpleLogicalValueType::String),
            TColumnSchema("required_sub", ESimpleLogicalValueType::Any),
            TColumnSchema("short", ESimpleLogicalValueType::Int16),
            TColumnSchema("string", ESimpleLogicalValueType::String),
            TColumnSchema("string_list", ESimpleLogicalValueType::Any),
            TColumnSchema("sub_list", ESimpleLogicalValueType::Any),
            TColumnSchema("tuple", ESimpleLogicalValueType::Any),
            TColumnSchema("ubyte", ESimpleLogicalValueType::Uint8),
            TColumnSchema("uint", ESimpleLogicalValueType::Uint32),
            TColumnSchema("ushort", ESimpleLogicalValueType::Uint16),
            TColumnSchema("yson", ESimpleLogicalValueType::Any),
            TColumnSchema("yson_proto", ESimpleLogicalValueType::Any),
    });
    EXPECT_EQ(*schema, *expectedSchema);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestSerializeYsonSub
    : public virtual TYsonStruct
{
    THashMap<std::string, std::string> Strings;

    REGISTER_YSON_STRUCT(TTestSerializeYsonSub);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("strings", &TThis::Strings)
            .Default();
    }
};

using TTestSerializeYsonSubPtr = TIntrusivePtr<TTestSerializeYsonSub>;

struct TTestSerializeYson
    : public virtual TYsonStruct
{
    ui64 Uint64;
    std::optional<double> OptionalDoubleEmpty;
    std::optional<double> OptionalDouble;
    TTestSerializeYsonSubPtr Sub;
    THashMap<std::string, TTestSerializeYsonSubPtr> Subs;
    std::pair<std::string, i64> Pair;
    std::optional<NYson::TYsonString> Yson;
    TInstant Instant;

    REGISTER_YSON_STRUCT(TTestSerializeYson);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("uint64", &TThis::Uint64)
            .Default();
        registrar.Parameter("optional_double_empty", &TThis::OptionalDoubleEmpty)
            .Default();
        registrar.Parameter("optional_double", &TThis::OptionalDouble)
            .Default();
        registrar.Parameter("sub", &TThis::Sub)
            .Default();
        registrar.Parameter("subs", &TThis::Subs)
            .Default();
        registrar.Parameter("pair", &TThis::Pair)
            .Default();
        registrar.Parameter("yson", &TThis::Yson)
            .Default();
        registrar.Parameter("instant", &TThis::Instant)
            .Default();
    }
};

using TTestSerializeYsonPtr = TIntrusivePtr<TTestSerializeYson>;

TEST(TYsonSerializeTest, YsonSerialize)
{
    auto schema = GetYsonSchema<TTestSerializeYson>();

    auto ysonStruct = New<TTestSerializeYson>();
    ysonStruct->Uint64 = 123456789u;
    ysonStruct->OptionalDoubleEmpty = {};
    ysonStruct->OptionalDouble = 5.0;
    const auto sub = New<TTestSerializeYsonSub>();
    sub->Strings["asbasdf"] = "fdsfds";
    ysonStruct->Sub = sub;
    const auto subKey = New<TTestSerializeYsonSub>();
    subKey->Strings["some_key"] = "some_string";
    ysonStruct->Subs["key"] = subKey;
    ysonStruct->Pair = std::pair{"abra", 345};
    ysonStruct->Yson = ConvertToYsonString(subKey);

    auto now = TInstant::Now();
    ysonStruct->Instant = now;

    auto row = Serialize(ysonStruct, schema);
    // ordered by alphabet
    EXPECT_EQ(schema->Columns()[0].Name(), "instant");
    EXPECT_EQ(FromUnversionedValue<TInstant>(row[0]), now);
    EXPECT_EQ(schema->Columns()[1].Name(), "optional_double");
    EXPECT_EQ(FromUnversionedValue<std::optional<double>>(row[1]), 5.0);
    EXPECT_EQ(schema->Columns()[2].Name(), "optional_double_empty");
    EXPECT_EQ(FromUnversionedValue<std::optional<double>>(row[2]), std::nullopt);

    EXPECT_EQ(schema->Columns()[3].Name(), "pair");
    {
        const auto parsedPair = ConvertTo<std::pair<std::string, i64>>(FromUnversionedValue<TYsonString>(row[3]));
        const auto expectedPair = std::pair{"abra", 345};
        EXPECT_EQ(parsedPair, expectedPair);
    }
    EXPECT_EQ(schema->Columns()[4].Name(), "sub");
    {
        const auto parsedSub = ConvertTo<TTestSerializeYsonSubPtr>(FromUnversionedValue<TYsonString>(row[4]));
        EXPECT_EQ(*parsedSub, *sub);
    }
    EXPECT_EQ(schema->Columns()[5].Name(), "subs");
    {
        const auto raw = ConvertToNode(FromUnversionedValue<TYsonString>(row[5]));
        const auto parsedSubs = ConvertTo<THashMap<std::string, TTestSerializeYsonSubPtr>>(raw);
        const auto expectedSubs = ysonStruct->Subs;
        EXPECT_EQ(parsedSubs.size(), expectedSubs.size());
        for (const auto& [key, value] : parsedSubs) {
            EXPECT_EQ(*value, *expectedSubs.at(key));
        }
    }
    EXPECT_EQ(schema->Columns()[6].Name(), "uint64");
    EXPECT_EQ(FromUnversionedValue<ui64>(row[6]), 123456789u);
    EXPECT_EQ(schema->Columns()[7].Name(), "yson");
    EXPECT_EQ(*ConvertTo<TTestSerializeYsonSubPtr>(FromUnversionedValue<NYson::TYsonString>(row[7])), *subKey);

    auto deserializedStruct = Deserialize<TTestSerializeYson>(row, schema);
    EXPECT_EQ(*deserializedStruct, *ysonStruct);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestPartialSerializeYson
    : public virtual TYsonStruct
{
    std::string Value;
    std::optional<i64> OtherValue;
    std::optional<double> Undefined;
    std::string Unserialized;

    REGISTER_YSON_STRUCT(TTestPartialSerializeYson);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default("default");
        registrar.Parameter("other_value", &TThis::OtherValue)
            .Default();
        registrar.Parameter("undefined", &TThis::Undefined)
            .Default();
        registrar.Parameter("unserialized", &TThis::Unserialized)
            .Default("default_value");
    }
};

using TTestPartialSerializeYsonPtr = TIntrusivePtr<TTestPartialSerializeYson>;

TEST(TYsonSerializeTest, PartialSerialize)
{
    auto ysonStruct = New<TTestPartialSerializeYson>();
    ysonStruct->Value = "some_value";
    ysonStruct->OtherValue = 5;
    ysonStruct->Unserialized = "random_value";
    auto schema = New<TTableSchema>(
        std::vector{
            TColumnSchema("other_value", EValueType::Int64),
            TColumnSchema("unknown", EValueType::Uint64),
            TColumnSchema("value", EValueType::String),
            TColumnSchema("another_unknown", EValueType::String),
            TColumnSchema("undefined", EValueType::Double)
        }
    );
    auto row = Serialize(ysonStruct, schema);
    ASSERT_EQ(row.GetCount(), 5);
    EXPECT_EQ(row[0].Id, 0);
    EXPECT_EQ(row[0].Type, EValueType::Int64);
    EXPECT_EQ(FromUnversionedValue<i64>(row[0]), 5);
    EXPECT_EQ(row[1].Id, 1);
    EXPECT_EQ(row[1].Type, EValueType::Null);
    EXPECT_EQ(row[2].Id, 2);
    EXPECT_EQ(row[2].Type, EValueType::String);
    EXPECT_EQ(FromUnversionedValue<std::string>(row[2]), "some_value");
    EXPECT_EQ(row[3].Id, 3);
    EXPECT_EQ(row[3].Type, EValueType::Null);
    EXPECT_EQ(row[4].Id, 4);
    EXPECT_EQ(row[4].Type, EValueType::Null);

    auto otherStruct = Deserialize<TTestPartialSerializeYson>(row, schema);
    EXPECT_EQ(otherStruct->Value, ysonStruct->Value);
    EXPECT_EQ(otherStruct->OtherValue, ysonStruct->OtherValue);
    EXPECT_EQ(otherStruct->Undefined, ysonStruct->Undefined);
    EXPECT_NE(otherStruct->Unserialized, ysonStruct->Unserialized);
    EXPECT_EQ(otherStruct->Unserialized, "default_value");

    auto fullSchema = GetYsonSchema<TTestPartialSerializeYson>();
    auto fullRow = Serialize(ysonStruct, fullSchema);
    auto fullOtherStruct = Deserialize<TTestPartialSerializeYson>(fullRow, fullSchema);
    EXPECT_EQ(*fullOtherStruct, *ysonStruct);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestYsonStructWithProto
    : public virtual TYsonStruct
{
    NProto::TTestMessage YsonProto;
    TProtoSerializedAsString<NProto::TTestMessage> StringProto;

    REGISTER_YSON_STRUCT(TTestYsonStructWithProto);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("yson_proto", &TThis::YsonProto)
            .Default();
        registrar.Parameter("string_proto", &TThis::StringProto)
            .Default();
    }
};

using TTestYsonStructWithProtoPtr = TIntrusivePtr<TTestYsonStructWithProto>;

TEST(TYsonSerializeTest, ProtoSerialize)
{
    NProto::TTestMessage proto;
    proto.SetInt32(234);
    proto.SetString("abcdef");
    auto ysonStruct = New<TTestYsonStructWithProto>();
    ysonStruct->YsonProto.CopyFrom(proto);
    ysonStruct->StringProto.CopyFrom(proto);

    auto schema = GetYsonSchema(ysonStruct);
    auto expectedSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("string_proto", EValueType::String),
            TColumnSchema("yson_proto", EValueType::Any),
        }
    );
    EXPECT_EQ(*schema, *expectedSchema);
    const auto row = Serialize(ysonStruct, schema);
    NProto::TTestMessage parsedProto;
    parsedProto.ParseFromStringOrThrow(FromUnversionedValue<TStringBuf>(row[0]));
    EXPECT_TRUE(MessageDifferencer::Equals(parsedProto, proto));
    const auto parsedNode = ConvertToNode(FromUnversionedValue<TYsonString>(row[1]));
    const auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("Int32").Value(234)
            .Item("String").Value("abcdef")
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(parsedNode, expectedNode));
    auto otherStruct = Deserialize<TTestYsonStructWithProto>(row, schema);
    EXPECT_EQ(*ysonStruct, *otherStruct);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NYsonSerializer
