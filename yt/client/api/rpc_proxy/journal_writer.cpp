#include "journal_reader.h"

#include <yt/client/api/journal_writer.h>

namespace NYT::NApi::NRpcProxy {

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
        InvokeResult_ = Request_->Invoke().As<void>();

        Opened_ = Request_->GetResponseAttachmentsStream()->Read() // TODOKETE this should be a separated function??
            .Apply(BIND ([] (const TErrorOr<TSharedRef>& refOrError) {
                const auto& ref = refOrError.ValueOrThrow();
                if (ref.Size() != 0) {
                    THROW_ERROR_EXCEPTION("Failed to open a journal writer: expected an empty ref")
                        << TErrorAttribute("attachment_size", ref.Size());
                }
            }));

        return Opened_;
    }

    virtual TFuture<void> Write(TRange<TSharedRef> rows) override
    {
        ValidateOpened();

        auto guard = Guard(SpinLock_);
        if (rows.Empty()) {
            return VoidFuture;
        }

        auto outputStream = Request_->GetRequestAttachmentsStream();
        for (size_t rowIndex = 0; rowIndex + 1 < rows.Size(); ++rowIndex) {
            outputStream->Write(rows[rowIndex]);
        }

        // TODO(kiselyovp) this future is set to OK when we can send more data, not when our rows
        // are actually written to journal. If an error occurs, the client will get to know about it later.
        // Is this fine?
        return outputStream->Write(rows.Back());
    }

    virtual TFuture<void> Close() override
    {
        ValidateOpened();
        Request_->GetRequestAttachmentsStream()->Close();
        return InvokeResult_;
    }

private:
    TApiServiceProxy::TReqCreateJournalWriterPtr Request_;
    TFuture<void> Opened_;
    TFuture<void> InvokeResult_;
    TSpinLock SpinLock_;

    void ValidateOpened()
    {
        if (!Opened_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't write into an unopened journal writer");
        }
        Opened_.Get().ThrowOnError();
    }
};

IJournalWriterPtr CreateRpcJournalWriter(
    TApiServiceProxy::TReqCreateJournalWriterPtr request)
{
    return New<TRpcJournalWriter>(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
