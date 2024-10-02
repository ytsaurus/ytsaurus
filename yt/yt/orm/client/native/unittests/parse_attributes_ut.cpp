#include <yt/yt/orm/server/objects/proto/continuation_token.pb.h>

#include <yt/yt/orm/client/native/response.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NClient::NNative::NTests {
namespace {

using namespace NServer::NObjects::NProto;

////////////////////////////////////////////////////////////////////////////////

std::vector<TPayload> GetInvalidPayloads()
{
    auto ysonString = NYTree::BuildYsonStringFluently()
        .BeginMap()
            .Item("tablet_count").Value(5)
            .Item("my_favorite_field_xxx").Value(50)
            .Item("event_offsets")
                .BeginList()
                    .Item()
                        .BeginMap()
                            .Item("row_index").Value(10)
                            .Item("event_index").Value(20)
                            .Item("tablet_index").Value(30)
                        .EndMap()
                .EndList()
        .EndMap();

    TYsonPayload ysonPayload = {
        .Yson = std::move(ysonString)
    };
    return { std::move(ysonPayload) };
}

TEST(TParseAttributesTest, AllFieldsAreKnown)
{
    std::vector<TPayload> payloads;
    {
        auto ysonString = NYTree::BuildYsonStringFluently()
            .BeginMap()
                .Item("tablet_count").Value(5)
                .Item("event_offsets")
                    .BeginList()
                        .Item()
                            .BeginMap()
                                .Item("row_index").Value(10)
                                .Item("event_index").Value(20)
                                .Item("tablet_index").Value(30)
                            .EndMap()
                        .Item()
                            .BeginMap()
                                .Item("row_index").Value(100)
                                .Item("event_index").Value(200)
                                .Item("tablet_index").Value(300)
                            .EndMap()
                    .EndList()
            .EndMap();

        TYsonPayload ysonPayload = {
            .Yson = std::move(ysonString)
        };
        payloads = { std::move(ysonPayload) };
    }

    TWatchQueryContinuationToken continuationToken;
    ParsePayloads(payloads, TParsePayloadsOptions{}, &continuationToken);

    EXPECT_EQ(5, continuationToken.tablet_count());
    EXPECT_EQ(2, continuationToken.event_offsets().size());

    EXPECT_EQ(10, continuationToken.event_offsets()[0].row_index());
    EXPECT_EQ(20, continuationToken.event_offsets()[0].event_index());
    EXPECT_EQ(30, continuationToken.event_offsets()[0].tablet_index());

    EXPECT_EQ(100, continuationToken.event_offsets()[1].row_index());
    EXPECT_EQ(200, continuationToken.event_offsets()[1].event_index());
    EXPECT_EQ(300, continuationToken.event_offsets()[1].tablet_index());
}

TEST(TParseAttributesTest, FailOnUnknownFields)
{
    auto payloads = GetInvalidPayloads();

    TWatchQueryContinuationToken continuationToken;
    EXPECT_THROW(ParsePayloads(payloads, TParsePayloadsOptions{}, &continuationToken), TErrorException);
}

TEST(TParseAttributesTest, SkipUnknownFields)
{
    auto payloads = GetInvalidPayloads();

    TWatchQueryContinuationToken continuationToken;
    TParsePayloadsOptions options = {
        .UnknownFieldMode = EUnknownFieldMode::Skip,
    };
    ParsePayloads(payloads, options, &continuationToken);

    EXPECT_EQ(5, continuationToken.tablet_count());
    EXPECT_EQ(1, continuationToken.event_offsets().size());

    EXPECT_EQ(10, continuationToken.event_offsets()[0].row_index());
    EXPECT_EQ(20, continuationToken.event_offsets()[0].event_index());
    EXPECT_EQ(30, continuationToken.event_offsets()[0].tablet_index());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NNative::NTests
