#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/http_proxy/config.h>
#include <yt/yt/server/http_proxy/framing.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/string/builder.h>

namespace NYT::NHttpProxy {
namespace {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TProxyTest, FramingOutputStream)
{
    constexpr char DataFrameTag = '\x01';
    constexpr char KeepAliveFrameTag = '\x02';

    TStringStream stringStream;
    {
        auto asyncStream = CreateAsyncAdapter(static_cast<IOutputStream*>(&stringStream));
        auto framingStream = New<TFramingAsyncOutputStream>(asyncStream, GetCurrentInvoker());
        auto frame1 = std::string("abc");
        auto frame2 = std::string("");
        auto frame3 = std::string("123 456" "\x00" "789 ABC"sv);
        WaitFor(framingStream->WriteDataFrame(TSharedRef::FromString(frame1)))
            .ThrowOnError();
        WaitFor(framingStream->WriteKeepAliveFrame())
            .ThrowOnError();
        WaitFor(framingStream->WriteKeepAliveFrame())
            .ThrowOnError();
        WaitFor(framingStream->WriteDataFrame(TSharedRef::FromString(frame2)))
            .ThrowOnError();
        WaitFor(framingStream->WriteDataFrame(TSharedRef::FromString(frame2)))
            .ThrowOnError();
        WaitFor(framingStream->WriteDataFrame(TSharedRef::FromString(frame3)))
            .ThrowOnError();
        WaitFor(framingStream->Close())
            .ThrowOnError();
        WaitFor(asyncStream->Close())
            .ThrowOnError();
    }
    EXPECT_EQ(stringStream.Str(),
        ::TStringBuilder() << DataFrameTag << "\x03\x00\x00\x00" "abc"sv
        << KeepAliveFrameTag
        << KeepAliveFrameTag
        << DataFrameTag << "\x00\x00\x00\x00"sv
        << DataFrameTag << "\x00\x00\x00\x00"sv
        << DataFrameTag << "\x0f\x00\x00\x00" "123 456" "\x00" "789 ABC"sv);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TProxyTest, SolomonProxyEndpointProviderNames)
{
    struct TProvider
    {
        EClusterComponentType ComponentType;
        std::optional<std::string> Name;
    };

    auto parseProviders = [] (const std::vector<TProvider>& providers) {
        auto config = BuildYsonStringFluently()
            .BeginMap()
                .Item("endpoint_providers").DoListFor(providers, [] (auto item, const TProvider& provider) {
                    item.Item()
                        .BeginMap()
                            .OptionalItem("name", provider.Name)
                            .Item("component_type").Value(provider.ComponentType)
                            .Item("monitoring_port").Value(10000)
                        .EndMap();
                })
            .EndMap();
        return ConvertTo<TSolomonProxyConfigPtr>(config);
    };

    // Distinct component types are named apart by default.
    EXPECT_NO_THROW(parseProviders({{EClusterComponentType::Scheduler}, {EClusterComponentType::HttpProxy}}));

    // One component type twice requires a name, which must not collide with another default name.
    EXPECT_THROW_WITH_SUBSTRING(
        parseProviders({{EClusterComponentType::Scheduler}, {EClusterComponentType::Scheduler}}),
        "Duplicate endpoint provider name");
    EXPECT_THROW_WITH_SUBSTRING(
        parseProviders({{EClusterComponentType::HttpProxy}, {EClusterComponentType::Scheduler, "http_proxy"}}),
        "Duplicate endpoint provider name");
    EXPECT_NO_THROW(parseProviders({{EClusterComponentType::Scheduler}, {EClusterComponentType::Scheduler, "timbertruck"}}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHttpProxy
