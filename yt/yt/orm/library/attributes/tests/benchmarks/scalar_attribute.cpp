#include <yt/yt/orm/library/attributes/tests/proto/scalar_attribute.pb.h>

#include <yt/yt/orm/library/attributes/scalar_attribute.h>

#include <yt/yt/core/misc/random.h>

#include <benchmark/benchmark.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>

#include <library/cpp/yt/memory/new.h>

using namespace NYT::NOrm::NAttributes;
using namespace NTests;

////////////////////////////////////////////////////////////////////////////////

namespace {

static void InitNestedMessage(NProto::TNestedMessage& message)
{
    for (int i = 0; i < 3; ++i) {
        message.add_repeated_int32_field(i);
    }
    message.set_int32_field(15);
    message.mutable_nested_message()->set_int32_field(1);
    for (int i = 0; i < 5; ++i) {
        message.mutable_nested_message()->mutable_repeated_int32_field()->Add(i);
    }
}

static void InitSimpleFields(NProto::TMessage& message)
{
    message.set_int32_field(42);
    message.set_bool_field(true);
    message.set_float_field(3.14);
    message.set_string_field("Hello, world!");
}

static void InitRepeatedSimpleFields(NProto::TMessage& message)
{
    TString s{"abc_"};

    for (int i = 0; i < 10; ++i) {
        message.add_repeated_string_field(s + ToString(i));
    }
    for (int i = 0; i < 30; ++i) {
        message.add_repeated_uint64_field(i + 10);
    }
}

static void InitRepeatedNestedFields(NProto::TMessage& message)
{
    for (int i = 0; i < 10; ++i) {
        auto& nested = *message.add_repeated_nested_message();
        InitNestedMessage(nested);
    }
}

static void InitMapSimpleFields(NProto::TMessage& message)
{
    NYT::TRandomGenerator rng(42);

    auto& map = *message.mutable_string_to_int32_map();
    for (int i = 0; i < 20; ++i) {
        auto key = ToString(rng.Generate<ui32>());
        map.insert({key, i});
    }
}

static void InitMapNestedFields(NProto::TMessage& message)
{
    NYT::TRandomGenerator rng(42);

    auto& map = *message.mutable_nested_message_map();
    for (int i = 0; i < 10; ++i) {
        auto key = ToString(rng.Generate<ui32>());
        auto& message = map[key];
        InitNestedMessage(message);
    }
}

static void InitMessageFields(NProto::TMessage& message)
{
    auto& nested = *message.mutable_nested_message();
    InitNestedMessage(nested);
}

static void Compare(
    benchmark::State& state,
    const NProto::TMessage& m1,
    const NProto::TMessage& m2,
    const TString& path)
{
    for (auto _ : state) {
        AreScalarAttributesEqualByPath(m1, m2, path);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

#define BM_COMPARE_FIELD(test_name)               \
static void BM_Compare ## test_name(              \
    benchmark::State& state,                      \
    const TString& fieldName)                     \
{                                                 \
    NProto::TMessage message;                     \
    Init ## test_name(message);                   \
    Compare(state, message, message, fieldName);  \
}                                                 \

BM_COMPARE_FIELD(SimpleFields);
BM_COMPARE_FIELD(RepeatedSimpleFields);
BM_COMPARE_FIELD(RepeatedNestedFields);
BM_COMPARE_FIELD(MapSimpleFields);
BM_COMPARE_FIELD(MapNestedFields);
BM_COMPARE_FIELD(MessageFields);

#undef BM_COMPARE_FIELD

////////////////////////////////////////////////////////////////////////////////

#define BENCHMARK_ATTRIBUTE(test_name, field_name)   \
BENCHMARK_CAPTURE(                                   \
    BM_Compare ## test_name,                         \
    field_name,                                      \
    "/" + TString(#field_name))                      \

BENCHMARK_ATTRIBUTE(SimpleFields, int32_field);
BENCHMARK_ATTRIBUTE(SimpleFields, string_field);
BENCHMARK_ATTRIBUTE(RepeatedSimpleFields, repeated_string_field);
BENCHMARK_ATTRIBUTE(RepeatedSimpleFields, repeated_uint64_field);
BENCHMARK_ATTRIBUTE(RepeatedNestedFields, repeated_nested_message);
BENCHMARK_ATTRIBUTE(MapSimpleFields, string_to_int32_map);
BENCHMARK_ATTRIBUTE(MapNestedFields, nested_message_map);
BENCHMARK_ATTRIBUTE(MessageFields, nested_message);
BENCHMARK_ATTRIBUTE(MessageFields, nested_message/nested_message/repeated_int32_field);

#undef BENCHMARK_ATTRIBUTE

////////////////////////////////////////////////////////////////////////////////
