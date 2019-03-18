#include "journal_reader.h"

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRpcJournalReader
    : public IJournalReader
{
private:
    using TRows = std::vector<TSharedRef>;

public:
    TRpcJournalReader(
        TApiServiceProxy::TReqCreateJournalReaderPtr request)
        : Request_(std::move(request))
    {
        YCHECK(Request_);
    }

    virtual TFuture<void> Open() override
    {
        auto guard = Guard(SpinLock_);

        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateInputStreamAdapter(Request_)
                .Apply(BIND([=, this_ = MakeStrong(this)] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
                    Underlying_ = inputStream;
                }));
        }

        return OpenResult_;
    }

    virtual TFuture<TRows> Read() override
    {
        ValidateOpened();

        auto promise = NewPromise<TSharedRef>();
        {
            auto guard = Guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture<TRows>(Error_);
            }
            ReaderQueue_.push(promise);
        }

        MaybeStartReading();

        return promise.ToFuture().Apply(BIND ([] (const TSharedRef& packedRows) {
            TRows rows;
            if (packedRows) {
                UnpackRefs(packedRows, &rows, true);
            }
            return rows;
        }));
    }

private:
    TApiServiceProxy::TReqCreateJournalReaderPtr Request_;
    IAsyncZeroCopyInputStreamPtr Underlying_;
    TFuture<void> OpenResult_;

    TRingQueue<TPromise<TSharedRef>> ReaderQueue_;
    std::atomic<bool> ReadInProgress_ = {false};
    TError Error_;

    TSpinLock SpinLock_;

    void ValidateOpened()
    {
        auto guard = Guard(SpinLock_);
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't read from an unopened journal reader");
        }
        OpenResult_.Get().ThrowOnError();
    }

    void MaybeStartReading() {
        if (ReadInProgress_.exchange(true)) {
            return;
        }

        auto guard = Guard(SpinLock_);
        if (ReaderQueue_.empty()) {
            ReadInProgress_ = false;
            return;
        }
        auto promise = std::move(ReaderQueue_.front());
        ReaderQueue_.pop();
        guard.Release();

        Underlying_->Read()
            .Apply(BIND([promise, this, weakThis = MakeWeak(this)] (const TErrorOr<TSharedRef>& refOrError) mutable {
                auto strongThis = weakThis.Lock();
                if (!strongThis) {
                    return;
                }

                if (refOrError.IsOK()) {
                    auto ref = refOrError.ValueOrThrow();
                    promise.Set(ref);
                    ReadInProgress_ = false;
                    MaybeStartReading();
                } else {
                    std::vector<TPromise<TSharedRef>> promises;
                    {
                        auto guard = Guard(SpinLock_);
                        Error_ = static_cast<TError>(refOrError);
                        while (!ReaderQueue_.empty()) {
                            promises.push_back(std::move(ReaderQueue_.front()));
                            ReaderQueue_.pop();
                        }
                    }

                    for (auto& promise : promises) {
                        promise.Set(Error_);
                    }

                    ReadInProgress_ = false;
                }
            }));
    }
};

IJournalReaderPtr CreateRpcJournalReader(
    TApiServiceProxy::TReqCreateJournalReaderPtr request)
{
    return New<TRpcJournalReader>(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

