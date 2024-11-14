#include "profiling.h"

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NFormats;
using namespace NHttp;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

class TProfilingOutputStream
    : public IFlushableAsyncOutputStream
{
public:
    TProfilingOutputStream(
        IResponseWriterPtr underlying,
        TApiPtr api,
        const std::string& user,
        const TNetworkAddress& clientAddress,
        const std::optional<TFormat>& outputFormat,
        const std::optional<TContentEncoding>& outputCompression)
        : Underlying_(std::move(underlying))
        , Api_(api)
        , User_(user)
        , ClientAddress_(clientAddress)
        , OutputFormat_(outputFormat)
        , OutputCompression_(outputCompression)
        , PrevWriteByteCount_(Underlying_->GetWriteByteCount())
    { }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return Underlying_->Write(buffer)
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] {
                    auto writeByteCount = Underlying_->GetWriteByteCount();
                    Api_->IncrementBytesOutProfilingCounters(
                        User_,
                        ClientAddress_,
                        writeByteCount - PrevWriteByteCount_,
                        OutputFormat_,
                        OutputCompression_);
                    PrevWriteByteCount_ = writeByteCount;
                }));
    }

    TFuture<void> Flush() override
    {
        return Underlying_->Flush();
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

private:
    const IResponseWriterPtr Underlying_;
    const TApiPtr Api_;
    const std::string User_;
    const TNetworkAddress ClientAddress_;
    const std::optional<NFormats::TFormat> OutputFormat_;
    const std::optional<TContentEncoding> OutputCompression_;

    i64 PrevWriteByteCount_ = 0;
};

class TProfilingInputStream
    : public IAsyncZeroCopyInputStream
{
public:
    TProfilingInputStream(
        IRequestPtr underlying,
        TApiPtr api,
        const std::string& user,
        const TNetworkAddress& clientAddress,
        const std::optional<NFormats::TFormat>& inputFormat,
        const std::optional<TContentEncoding>& inputCompression)
        : Underlying_(std::move(underlying))
        , Api_(api)
        , User_(user)
        , ClientAddress_(clientAddress)
        , InputFormat_(inputFormat)
        , InputCompression_(inputCompression)
        , PrevReadByteCount_(Underlying_->GetReadByteCount())
    { }

    TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] (const TSharedRef& data) {
                    auto readByteCount = Underlying_->GetReadByteCount();
                    Api_->IncrementBytesInProfilingCounters(
                        User_,
                        ClientAddress_,
                        readByteCount - PrevReadByteCount_,
                        InputFormat_,
                        InputCompression_);
                    PrevReadByteCount_ = readByteCount;
                    return data;
                }));
    }

private:
    const IRequestPtr Underlying_;
    const TApiPtr Api_;
    const std::string User_;
    const TNetworkAddress ClientAddress_;
    const std::optional<NFormats::TFormat> InputFormat_;
    const std::optional<TContentEncoding> InputCompression_;

    i64 PrevReadByteCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IFlushableAsyncOutputStreamPtr CreateProfilingOutpuStream(
    IResponseWriterPtr underlying,
    TApiPtr api,
    const std::string& user,
    const TNetworkAddress& clientAddress,
    const std::optional<NFormats::TFormat>& outputFormat,
    const std::optional<TContentEncoding>& outputCompression)
{
    return New<TProfilingOutputStream>(
        std::move(underlying),
        std::move(api),
        user,
        clientAddress,
        outputFormat,
        outputCompression);
}

IAsyncZeroCopyInputStreamPtr CreateProfilingInputStream(
    IRequestPtr underlying,
    TApiPtr api,
    const std::string& user,
    const TNetworkAddress& clientAddress,
    const std::optional<NFormats::TFormat>& inputFormat,
    const std::optional<TContentEncoding>& inputCompression)
{
    return New<TProfilingInputStream>(
        std::move(underlying),
        std::move(api),
        user,
        clientAddress,
        inputFormat,
        inputCompression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
