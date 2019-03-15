#include "journal_reader.h"

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRpcJournalReader
    : public IJournalReader
{
public:
    TRpcJournalReader(
        TApiServiceProxy::TReqCreateJournalReaderPtr request)
        : Request_(std::move(request))
        , LastReadResult_(MakeFuture(std::vector<TSharedRef>()))
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

    virtual TFuture<std::vector<TSharedRef>> Read() override
    {
        ValidateOpened();

        auto guard = Guard(SpinLock_);
        LastReadResult_ = LastReadResult_.Apply(
            BIND([=, this_ = MakeStrong(this)] (const std::vector<TSharedRef>&) {
                return DoRead();
            }));

        return LastReadResult_;
    }

private:
    TApiServiceProxy::TReqCreateJournalReaderPtr Request_;
    IAsyncZeroCopyInputStreamPtr Underlying_;
    TFuture<void> OpenResult_;
    TFuture<std::vector<TSharedRef>> LastReadResult_;

    TSpinLock SpinLock_;

    void ValidateOpened()
    {
        auto guard = Guard(SpinLock_);
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't read from an unopened journal reader");
        }
        OpenResult_.Get().ThrowOnError();
    }

    TFuture<std::vector<TSharedRef>> DoRead()
    {
        return Underlying_->Read().Apply(BIND([=] (const TSharedRef& ref) {
            if (!ref) {
                return std::vector<TSharedRef>();
            }
            
            std::vector<TSharedRef> parts;
            UnpackRefs(ref, &parts, true);
            return parts;
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

