#include "file_writer.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/client/api/file_writer.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcFileWriter
    : public IFileWriter
{
public:
    TRpcFileWriter(
        TApiServiceProxy::TReqCreateFileWriterPtr request):
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
                    THROW_ERROR_EXCEPTION("Failed to open a file writer: expected an empty ref")
                        << TErrorAttribute("attachment_size", ref.Size());
                }
            }));

        return Opened_;
    }

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateOpened();
        // TODO(kiselyovp) this future is set to OK when we can send more data, not when our blob
        // is actually written to file. If an error occurs, the client will get to know about it later.
        // Is this fine?
        return Request_->GetRequestAttachmentsStream()->Write(data);
    }

    virtual TFuture<void> Close() override
    {
        ValidateOpened();
        Request_->GetRequestAttachmentsStream()->Close();
        return InvokeResult_;
    }

private:
    TApiServiceProxy::TReqCreateFileWriterPtr Request_;
    TFuture<void> Opened_;
    TFuture<void> InvokeResult_;

    void ValidateOpened()
    {
        if (!Opened_.IsSet()) {
            THROW_ERROR_EXCEPTION("Can't write into an unopened file writer");
        }
        Opened_.Get().ThrowOnError();
    }
};

IFileWriterPtr CreateRpcFileWriter(
    TApiServiceProxy::TReqCreateFileWriterPtr request)
{
    return New<TRpcFileWriter>(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
