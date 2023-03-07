#include "journal_writer.h"

#include <yt/client/api/journal_writer.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TJournalWriter
    : public IJournalWriter
{
public:
    TJournalWriter(
        TApiServiceProxy::TReqWriteJournalPtr request)
        : Request_(std::move(request))
    {
        YT_VERIFY(Request_);
    }

    virtual TFuture<void> Open() override
    {
        ValidateNotClosed();

        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateRpcClientOutputStream(Request_, true)
                .Apply(BIND([=, this_ = MakeStrong(this)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
                    Underlying_ = outputStream;
                })).As<void>();
        }

        return OpenResult_;
    }

    virtual TFuture<void> Write(TRange<TSharedRef> rows) override
    {
        ValidateOpened();
        ValidateNotClosed();

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
    const TApiServiceProxy::TReqWriteJournalPtr Request_;

    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> OpenResult_;
    bool Closed_ = false;

    void ValidateOpened()
    {
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Cannot write into an unopened journal writer");
        }
        OpenResult_.Get().ThrowOnError();
    }

    void ValidateNotClosed()
    {
        if (Closed_) {
            THROW_ERROR_EXCEPTION("Journal writer is closed");
        }
    }
};

IJournalWriterPtr CreateJournalWriter(
    TApiServiceProxy::TReqWriteJournalPtr request)
{
    return New<TJournalWriter>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

