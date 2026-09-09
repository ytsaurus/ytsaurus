#include <yt/yt/flow/extensions/http/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

TAsyncHttpSinkParametersPtr ParseStatic(TStringBuf yson)
{
    return ConvertTo<TAsyncHttpSinkParametersPtr>(TYsonStringBuf(yson));
}

TDynamicAsyncHttpSinkParametersPtr ParseDynamic(TStringBuf yson)
{
    return ConvertTo<TDynamicAsyncHttpSinkParametersPtr>(TYsonStringBuf(yson));
}

TEST(TAsyncHttpSinkParametersTest, ParsesStaticCombinations)
{
    auto defaults = ParseStatic(R"({url="https://example.test/post";payload_column="payload"})");
    EXPECT_TRUE(defaults->KeepAlive);
    EXPECT_TRUE(defaults->Headers.empty());
    EXPECT_EQ(defaults->IdempotencyHeader, "Idempotency-Key");
    EXPECT_EQ(defaults->MaxRedirectCount, 0);
    EXPECT_EQ(defaults->MaxIdleConnections, 8);

    auto http = ParseStatic(R"({url="http://example.test";payload_column="body";keep_alive=%false})");
    EXPECT_FALSE(http->KeepAlive);

    auto headers = ParseStatic(R"({url="https://example.test";payload_column="data";headers={Content-Type="application/octet-stream"}})");
    EXPECT_EQ(headers->Headers.at("Content-Type"), "application/octet-stream");

    auto path = ParseStatic(R"({url="https://example.test/a?b=c";payload_column="value";headers={X-Test="opaque"};keep_alive=%true})");
    EXPECT_EQ(path->Url, "https://example.test/a?b=c");

    auto transport = ParseStatic(R"({url="https://example.test";payload_column="value";idempotency_header="X-Flow-Message-Id";max_redirect_count=2;max_idle_connections=17})");
    EXPECT_EQ(transport->IdempotencyHeader, "X-Flow-Message-Id");
    EXPECT_EQ(transport->MaxRedirectCount, 2);
    EXPECT_EQ(transport->MaxIdleConnections, 17);

    auto disabled = ParseStatic(R"({url="https://example.test";payload_column="value";idempotency_header=""})");
    EXPECT_TRUE(disabled->IdempotencyHeader.empty());
}

TEST(TAsyncHttpSinkParametersTest, ParsesDynamicDefaults)
{
    auto parameters = ParseDynamic("{}");
    EXPECT_EQ(parameters->RequestTimeout, TDuration::Minutes(1));
    EXPECT_EQ(parameters->AttemptTimeout, TDuration::Seconds(10));
    EXPECT_EQ(parameters->RetryInitialDelay, TDuration::Seconds(1));
    EXPECT_EQ(parameters->RetryMinimumDelay, TDuration::MilliSeconds(100));
    EXPECT_DOUBLE_EQ(parameters->RetryMultiplier, 2.0);
    EXPECT_EQ(parameters->RetryMaximumDelay, TDuration::Seconds(30));
    EXPECT_DOUBLE_EQ(parameters->RetryJitterRatio, 0.2);
    EXPECT_EQ(parameters->MaxAttemptCount, 5);
}

TEST(TAsyncHttpSinkParametersTest, RejectsInvalidStaticParameters)
{
    for (auto yson : {
            R"({payload_column="payload"})",
            R"({url="";payload_column="payload"})",
            R"({url="https://example.test"})",
            R"({url="https://example.test";payload_column=""})",
            R"({url="ftp://example.test";payload_column="payload"})",
            R"({url="https:///path";payload_column="payload"})",
            R"({url="https://example.test";payload_column="payload";headers={""="value"}})",
            R"({url="https://example.test";payload_column="payload";headers={"Bad Header"="value"}})",
            R"({url="https://example.test";payload_column="payload";headers={X-Test="line\nbreak"}})",
            R"({url="https://example.test";payload_column="payload";headers={X-Test="line\rbreak"}})",
            R"({url="https://example.test";payload_column="payload";headers={Connection="close"}})",
            R"({url="https://example.test";payload_column="payload";headers={Content-Length="1"}})",
            R"({url="https://example.test";payload_column="payload";headers={Host="receiver.test"}})",
            R"({url="https://example.test";payload_column="payload";headers={Content-Type="application/json";content-type="application/octet-stream"}})",
            R"({url="https://example.test";payload_column="payload";headers={Idempotency-Key="fixed"}})",
            R"({url="https://example.test";payload_column="payload";idempotency_header="Bad Header"})",
            R"({url="https://example.test";payload_column="payload";idempotency_header="Content-Length"})",
            R"({url="https://example.test";payload_column="payload";max_redirect_count=-1})",
            R"({url="https://example.test";payload_column="payload";max_idle_connections=-1})",
            R"({url="https://example.test";payload_column="payload";at_most_once_strategy={enabled=%true}})",
         }) {
        EXPECT_THROW(ParseStatic(yson), std::exception) << yson;
    }
}

TEST(TAsyncHttpSinkParametersTest, RejectsInvalidDynamicParameters)
{
    for (auto yson : {
            R"({request_timeout="0s"})",
            R"({attempt_timeout="0s"})",
            R"({retry_initial_delay="0s"})",
            R"({retry_minimum_delay="0s"})",
            R"({retry_maximum_delay="0s"})",
            R"({request_timeout="1s";attempt_timeout="2s"})",
            R"({retry_minimum_delay="2s";retry_initial_delay="1s"})",
            R"({retry_initial_delay="2s";retry_maximum_delay="1s"})",
            R"({retry_multiplier=1.0})",
            R"({retry_multiplier=100.1})",
            R"({retry_jitter_ratio=-0.1})",
            R"({retry_jitter_ratio=1.1})",
            R"({max_attempt_count=0})",
            R"({max_attempt_count=101})",
         }) {
        EXPECT_THROW(ParseDynamic(yson), std::exception) << yson;
    }
}

TEST(TAsyncHttpSinkParametersTest, RejectsAtMostOnceDynamicConfiguration)
{
    auto disabled = ParseDynamic(R"({at_most_once_strategy={enabled=%false}})");
    ASSERT_TRUE(disabled->AtMostOnceStrategy->GetLocalUnrecognized());
    EXPECT_TRUE(disabled->AtMostOnceStrategy->GetLocalUnrecognized()->FindChild("enabled"));

    EXPECT_THROW_WITH_SUBSTRING(
        ParseDynamic(R"({at_most_once_strategy={enabled=%true}})"),
        "Async HTTP sink does not support at_most_once_strategy");
}

} // namespace
} // namespace NYT::NFlow
