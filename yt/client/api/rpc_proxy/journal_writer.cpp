#include "journal_reader.h"

#include <yt/client/api/journal_writer.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRpcJournalWriter
    : public IJournalWriter
{
public:
    TRpcJournalWriter(
        TApiServiceProxy::TReqCreateJournalWriterPtr request):
        Request_(request)
    {
        YCHECK(Request_);
    }

    virtual TFuture<void> Open() override
    {
        ValidateNotClosed();

        auto guard = Guard(SpinLock_);
        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateOutputStreamAdapter(Request_, NRpc::EWriterFeedbackStrategy::OnlyPositive)
                .Apply(BIND([=, this_ = MakeStrong(this)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
                    Underlying_ = outputStream;
                })).As<void>();
        }

        return OpenResult_;
    }

    virtual TFuture<void> Write(TRange<TSharedRef> rows) override
    {
        ValidateOpened();

        auto guard = Guard(SpinLock_);
        if (rows.Empty()) {
            return VoidFuture;
        }

        return Underlying_->Write(PackRefs(rows));
    }

    virtual TFuture<void> Close() override
    {
        ValidateOpened();
        ValidateNotClosed();

        Closed_ = true;
        return Underlying_->Close();
    }

private:
    TApiServiceProxy::TReqCreateJournalWriterPtr Request_;
    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> OpenResult_;
    std::atomic<bool> Closed_{false};
    TSpinLock SpinLock_;

    void ValidateOpened()
    {
        auto guard = Guard(SpinLock_);
        if (!OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't write into an unopened journal writer");
        }
        OpenResult_.Get().ThrowOnError();
    }

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("File writer is closed");
        }
    }
};

IJournalWriterPtr CreateRpcJournalWriter(
    TApiServiceProxy::TReqCreateJournalWriterPtr request)
{
    return New<TRpcJournalWriter>(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
