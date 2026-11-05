#include "profiling.h"

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/http.h>

#include <atomic>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NFormats;
using namespace NHttp;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Monotonically raises |accounted| up to |current| and returns the unaccounted
// delta. Safe against concurrent calls: parallel callers race to bump the
// counter, and only the one that actually advances it reports a positive delta;
// others get zero. Guarantees the returned value is non-negative.
i64 AccountByteCountDelta(std::atomic<i64>& accounted, i64 current)
{
    auto accountedValue = accounted.load();
    while (accountedValue < current &&
        !accounted.compare_exchange_weak(accountedValue, current))
    { }
    return std::max<i64>(0, current - accountedValue);
}

} // namespace

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
        , AccountedWriteByteCount_(Underlying_->GetWriteByteCount())
    { }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return Underlying_->Write(buffer)
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] {
                    auto delta = AccountByteCountDelta(AccountedWriteByteCount_, Underlying_->GetWriteByteCount());
                    if (delta > 0) {
                        Api_->IncrementBytesOutProfilingCounters(
                            User_,
                            ClientAddress_,
                            delta,
                            OutputFormat_,
                            OutputCompression_);
                    }
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

    std::atomic<i64> AccountedWriteByteCount_;
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
        , AccountedReadByteCount_(Underlying_->GetReadByteCount())
    { }

    TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] (const TSharedRef& data) {
                    auto delta = AccountByteCountDelta(AccountedReadByteCount_, Underlying_->GetReadByteCount());
                    if (delta > 0) {
                        Api_->IncrementBytesInProfilingCounters(
                            User_,
                            ClientAddress_,
                            delta,
                            InputFormat_,
                            InputCompression_);
                    }
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

    std::atomic<i64> AccountedReadByteCount_;
};

////////////////////////////////////////////////////////////////////////////////

IFlushableAsyncOutputStreamPtr CreateProfilingOutputStream(
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
