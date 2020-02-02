#include <yt/core/test_framework/framework.h>

#include <yt/server/http_proxy/compression.h>
#include <yt/server/http_proxy/framing.h>

#include <yt/core/concurrency/scheduler_api.h>

#include <util/string/builder.h>

namespace NYT::NHttpProxy {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(THttpProxy, CompressionStreamFlush)
{
    constexpr int IterationCount = 10;
    for (const auto& compression : GetSupportedCompressions()) {
        if (compression == IdentityContentEncoding) {
            continue;
        }
        TStringStream stringStream;
        auto asyncStream = CreateAsyncAdapter(static_cast<IOutputStream*>(&stringStream));
        auto compressionStream = CreateCompressingAdapter(asyncStream, compression);
        auto previousLength = stringStream.Size();
        for (int i = 0; i < IterationCount; ++i) {
            WaitFor(compressionStream->Write(TSharedRef("x", 1, nullptr)))
                .ThrowOnError();
            WaitFor(compressionStream->Flush())
                .ThrowOnError();
            EXPECT_GT(stringStream.Size(), previousLength)
                << "Output for stream " << compression << " has not grown on iteration " << i;
            previousLength = stringStream.Size();
        }
        WaitFor(compressionStream->Close())
            .ThrowOnError();
        WaitFor(asyncStream->Close())
            .ThrowOnError();
    }
}

TEST(THttpProxy, FramingOutputStream)
{
    constexpr char DataFrameTag = '\x01';
    constexpr char KeepAliveFrameTag = '\x02';

    TStringStream stringStream;
    {
        auto asyncStream = CreateAsyncAdapter(static_cast<IOutputStream*>(&stringStream));
        auto framingStream = New<TFramingAsyncOutputStream>(asyncStream);
        auto frame1 = TString("abc");
        auto frame2 = TString("");
        auto frame3 = TString(AsStringBuf("123 456" "\x00" "789 ABC"));
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
        ::TStringBuilder() << DataFrameTag << AsStringBuf("\x03\x00\x00\x00" "abc")
        << KeepAliveFrameTag
        << KeepAliveFrameTag
        << DataFrameTag << AsStringBuf("\x00\x00\x00\x00")
        << DataFrameTag << AsStringBuf("\x00\x00\x00\x00")
        << DataFrameTag << AsStringBuf("\x0f\x00\x00\x00" "123 456" "\x00" "789 ABC"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHttpProxy



