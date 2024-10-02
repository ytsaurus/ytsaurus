#include <yt/yt/orm/example/client/native/autogen/config.h>
#include <yt/yt/orm/example/client/native/autogen/client.h>

#include <yt/yt/orm/client/native/client.h>
#include <yt/yt/orm/client/native/connection.h>
#include <yt/yt/orm/client/native/connection_impl.h>
#include <yt/yt/orm/client/native/helpers.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/ytree/fluent.h>

#include <benchmark/benchmark.h>

#include <library/cpp/yt/memory/new.h>

#include <library/cpp/testing/gtest_extensions/assertions.h>

#include <util/generic/singleton.h>
#include <util/system/env.h>

namespace {

using namespace NYT::NConcurrency;
using namespace NYT::NOrm::NExample::NClient::NNative;
using namespace NYT::NOrm::NExample::NClient::NObjects;
using namespace NYT::NOrm::NClient::NNative;
using namespace NYT::NOrm::NClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TClientHolder
{
public:
    TClientHolder()
        : Client_(CreateClient())
    { }

    void Shutdown()
    {
        Client_.Reset();
    }

    TSelectObjectsResult SelectObjects(
        const TString& filter,
        int limit = 100,
        const std::optional<TString>& index = std::nullopt,
        const std::optional<TString>& continuationToken = std::nullopt) const
    {
        return WaitFor(Client_->SelectObjects(
            TObjectTypeValues::Book,
            TAttributeSelector{"/meta/id", "/spec/year"},
            TSelectObjectsOptions{
                .Filter = filter,
                .Limit = limit,
                .ContinuationToken = continuationToken,
                .Index = index,
            }))
            .ValueOrThrow();
    }

    TPayload CreateObject(const NObjects::TObjectTypeValue objectType) const
    {
        return WaitFor(
            Client_->CreateObject(
                objectType,
                MakeObject(objectType),
                TCreateObjectOptions{
                    .Format = EPayloadFormat::Yson,
                }))
            .ValueOrThrow()
            .Meta;
    }

    void UpdateObject(
        const TPayload& identity,
        const NObjects::TObjectTypeValue objectType) const
    {
        WaitFor(
            Client_->UpdateObject(
                identity,
                objectType,
                MakePatch(objectType)))
            .ValueOrThrow();
    }

private:
    IClientPtr Client_;

    IClientPtr CreateClient()
    {
        auto address = GetEnv("EXAMPLE_MASTER_GRPC_INSECURE_ADDR");
        auto connectionConfigNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("secure").Value(false)
                .Item("use_legacy_connection").Value(false)
                .Item("addresses")
                    .BeginList()
                        .Item().Value(address)
                    .EndList()
                .Item("authentication")
                    .BeginMap()
                        .Item("user").Value("root")
                    .EndMap()
            .EndMap();

        auto connectionConfig = NYT::New<NYT::NOrm::NExample::NClient::NNative::TConnectionConfig>();
        connectionConfig->Load(connectionConfigNode);
        return NYT::NOrm::NExample::NClient::NNative::CreateClient(connectionConfig);
    }

    TPayload MakeObject(const NObjects::TObjectTypeValue objectType) const
    {
        switch (objectType) {
            case TObjectTypeValues::Book:
                return BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .BeginMap()
                            .Item("meta")
                                .BeginMap()
                                    .Item("isbn").Value("978-1449355739")
                                    .Item("publisher_id").Value(42)
                                .EndMap()
                            .Item("spec")
                                .BeginMap()
                                    .Item("name").Value("Learning Python")
                                    .Item("year").Value(2014)
                                    .Item("genres")
                                        .BeginList()
                                            .Item().Value("education")
                                            .Item().Value("programming")
                                        .EndList()
                                    .Item("keywords")
                                        .BeginList()
                                            .Item().Value("learning")
                                            .Item().Value("python")
                                        .EndList()
                                    .Item("design")
                                        .BeginMap()
                                            .Item("cover")
                                                .BeginMap()
                                                    .Item("hardcover").Value(true)
                                                    .Item("image").Value("path/to/cover.png")
                                                .EndMap()
                                            .Item("color").Value("white")
                                            .Item("illustrations")
                                                .BeginList()
                                                    .Item().Value("image1.jpeg")
                                                    .Item().Value("image2.jpeg")
                                                .EndList()
                                        .EndMap()
                                    .Item("digital_data")
                                        .BeginMap()
                                            .Item("store_rating").Value(4.9)
                                            .Item("available_formats")
                                                .BeginList()
                                                    .Item()
                                                        .BeginMap()
                                                            .Item("format").Value("epub")
                                                            .Item("size").Value(11778382)
                                                        .EndMap()
                                                    .Item()
                                                        .BeginMap()
                                                            .Item("format").Value("pdf")
                                                            .Item("size").Value(12778383)
                                                        .EndMap()
                                                .EndList()
                                        .EndMap()
                                    .Item("page_count").Value(256)
                                .EndMap()
                        .EndMap();
                }));
            case TObjectTypeValues::Author:
                return BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .BeginMap()
                            .Item("spec")
                                .BeginMap()
                                    .Item("name").Value("Mark Lutz")
                                .EndMap()
                        .EndMap();
                }));
            default:
                YT_ABORT();
        }
    }

    std::vector<TUpdate> MakePatch(const NObjects::TObjectTypeValue objectType) const
    {
        switch (objectType) {
        case TObjectTypeValues::Book:
            return std::vector<TUpdate>{TSetUpdate{
                "/spec/year",
                BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(2015);
                }))}};
        case TObjectTypeValues::Author:
            return std::vector<TUpdate>{TSetUpdate{
                "/spec/name",
                BuildYsonPayload(BIND([&] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value("Markus Lutz");
                }))}};
        default:
            YT_ABORT();
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void BM_SelectObjects(
    benchmark::State& state,
    const std::optional<TString>& index,
    const TString& filter,
    size_t objectCount,
    bool withContinuationToken = false)
{
    std::optional<TString> continuationToken;
    int limit = objectCount;
    if (withContinuationToken) {
        auto firstBatch = Singleton<TClientHolder>()->SelectObjects(filter, /*limit*/ 10, index);
        ASSERT_EQ(firstBatch.Results.size(), 10ul);
        ASSERT_TRUE(firstBatch.ContinuationToken);

        continuationToken = firstBatch.ContinuationToken;
        objectCount -= 10;
    }
    for (auto _ : state) {
        ASSERT_EQ(
            Singleton<TClientHolder>()->SelectObjects(filter, limit, index, continuationToken).Results.size(),
            objectCount);
    }
}

void BM_CreateObject(benchmark::State& state, const NObjects::TObjectTypeValue objectType)
{
    for (auto _ : state) {
        Singleton<TClientHolder>()->CreateObject(objectType);
    }
}

void BM_UpdateObject(benchmark::State& state, const NObjects::TObjectTypeValue objectType)
{
    auto identity  = Singleton<TClientHolder>()->CreateObject(objectType);
    for (auto _ : state) {
        Singleton<TClientHolder>()->UpdateObject(identity, objectType);
    }
}

void BM_WarmingUp(benchmark::State& state)
{
    for (auto _ : state) {
        Singleton<TClientHolder>()->SelectObjects("[/spec/year]>0 and [/spec/year]<100");
    }
}

void BM_Shutdown(benchmark::State& state)
{
    Singleton<TClientHolder>()->Shutdown();
    for (auto _ : state) {
    }
}

////////////////////////////////////////////////////////////////////////////////

#define BENCHMARK_RUN(repetitions, ...)     \
    BENCHMARK_CAPTURE(__VA_ARGS__)          \
        ->Unit(benchmark::kMillisecond)     \
        ->UseRealTime()                     \
        ->Repetitions(repetitions)          \
        ->DisplayAggregatesOnly(true);

#define BENCHMARK_INDEX(name, index, ...)   \
    BENCHMARK_RUN(                          \
        7,                                  \
        BM_SelectObjects,                   \
        name ## __use_index__ ## index,     \
        std::optional<TString>(#index),     \
        __VA_ARGS__)                        \
    BENCHMARK_RUN(                          \
        7,                                  \
        BM_SelectObjects,                   \
        name ## __no_index,                 \
        std::nullopt,                       \
        __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

BENCHMARK(BM_WarmingUp);

BENCHMARK_INDEX(scalar_unique, books_by_year, "[/spec/year]=2013", 1)
BENCHMARK_INDEX(scalar_rare, books_by_year, "[/spec/year]=1000", 50)
BENCHMARK_INDEX(
    scalar_rare_continuation_token,
    books_by_year,
    "[/spec/year]=1000",
    50,
    /*withContinuationToken*/ true)
BENCHMARK_INDEX(scalar_normal, books_by_year, "[/spec/year]=100", 100)
BENCHMARK_INDEX(scalar_frequent, books_by_year, "[/spec/year]=10", 100)
BENCHMARK_INDEX(scalar_multikey, books_by_year_and_font, "[/spec/year]=2013", 1)

BENCHMARK_INDEX(
    scalar_inequality_hash_expression,
    books_by_year,
    "[/spec/year]>2012 and [/spec/year]<2014",
    1)
BENCHMARK_INDEX(
    scalar_inequality_plain,
    books_by_year_and_font,
    "[/spec/year]>2012 and [/spec/year]<2014",
    1)

BENCHMARK_INDEX(
    scalar_one_to_many_unique,
    books_by_editor,
    "[/spec/editor_id]=\"editor_id_unique\"",
    1)
BENCHMARK_INDEX(
    scalar_one_to_many_frequent,
    books_by_editor,
    "[/spec/editor_id]=\"editor_id_frequent\"",
    100)

BENCHMARK_INDEX(
    scalar_subfield_unique,
    books_by_store_rating,
    "[/spec/digital_data/store_rating] >= 2.013 AND [/spec/digital_data/store_rating] <= 2.0131",
    1)
BENCHMARK_INDEX(
    scalar_subfield_frequent,
    books_by_store_rating,
    "[/spec/digital_data/store_rating] >= 0.01 AND [/spec/digital_data/store_rating] <= 0.0101",
    100)

BENCHMARK_INDEX(
    repeated_unique,
    books_by_keywords,
    "list_contains([/spec/keywords], \"created_in_2013\")",
    1)
BENCHMARK_INDEX(
    repeated_frequent,
    books_by_keywords,
    "list_contains([/spec/keywords], \"created_in_10\")",
    100)

BENCHMARK_RUN(11, BM_CreateObject, book__with_indexs, TObjectTypeValues::Book)
BENCHMARK_RUN(11, BM_CreateObject, author__no_index, TObjectTypeValues::Author)

BENCHMARK_RUN(11, BM_UpdateObject, book__with_indexs, TObjectTypeValues::Book)
BENCHMARK_RUN(11, BM_UpdateObject, author__no_index, TObjectTypeValues::Author)

BENCHMARK(BM_Shutdown);
