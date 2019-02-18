#include "file_reader.h"

#include <yt/client/api/file_reader.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcFileReader
    : public IFileReader
{
public:
    TRpcFileReader(
        TApiServiceProxy::TReqCreateFileReaderPtr request,
        ui64 revision):
        Request_(request),
        Revision_(revision)
    {
        YCHECK(Request_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return Request_->GetResponseAttachmentsStream()->Read();
    }

    virtual ui64 GetRevision() const override
    {
        return Revision_;
    }

    ~TRpcFileReader() {
        // TODO(kiselyovp) doing work in destructor but there's no better place?
        NConcurrency::WaitFor(Request_->GetRequestAttachmentsStream()->Close());
    }

private:
    TApiServiceProxy::TReqCreateFileReaderPtr Request_;
    ui64 Revision_;
};

TFuture<IFileReaderPtr> CreateRpcFileReader(
    TApiServiceProxy::TReqCreateFileReaderPtr request)
{
    request->Invoke();

    return request->GetResponseAttachmentsStream()->Read()
        .Apply(BIND([request] (const TErrorOr<TSharedRef>& refOrError) {
            // TODO(kiselyovp) there should be a better way to do this, maybe find a way to send protobufs
            const auto& revisionRef = refOrError.ValueOrThrow();
            if (revisionRef.Size() != sizeof(ui64)) {
                THROW_ERROR_EXCEPTION("Attachment size mismatch")
                    << TErrorAttribute("actual_size", revisionRef.Size())
                    << TErrorAttribute("expected_size", sizeof(ui64));
            }

            ui64 revision = *reinterpret_cast<const ui64*>(revisionRef.Begin());

            return New<TRpcFileReader>(request, revision);
        })).As<IFileReaderPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
