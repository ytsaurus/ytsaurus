#include "file_writer.h"

#include <yt/client/api/file_writer.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TFileWriter
    : public IFileWriter
{
public:
    TFileWriter(
        TApiServiceProxy::TReqWriteFilePtr request)
        : Request_(std::move(request))
    {
        YT_VERIFY(Request_);
    }

    virtual TFuture<void> Open() override
    {
        ValidateNotClosed();

        if (!OpenResult_) {
            OpenResult_ = NRpc::CreateRpcClientOutputStream(Request_)
                .Apply(BIND([=, this_ = MakeStrong(this)] (const IAsyncZeroCopyOutputStreamPtr& outputStream) {
                    Underlying_ = outputStream;
                })).As<void>();
        }

        return OpenResult_;
    }

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        ValidateOpened();
        ValidateNotClosed();

        if (!data) {
            return VoidFuture;
        }

        // Returned future might be set instantly, and the user can modify #data right after that.
        struct TTag { };
        auto dataCopy = TSharedMutableRef::MakeCopy<TTag>(data);
        return Underlying_->Write(dataCopy);
    }

    virtual TFuture<void> Close() override
    {
        ValidateOpened();
        ValidateNotClosed();

        Closed_ = true;
        return Underlying_->Close();
    }

private:
    const TApiServiceProxy::TReqWriteFilePtr Request_;

    IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> OpenResult_;
    bool Closed_ = false;

    void ValidateOpened()
    {
        if (!OpenResult_ || !OpenResult_.IsSet()) {
            THROW_ERROR_EXCEPTION("Cannot write into an unopened file writer");
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

IFileWriterPtr CreateFileWriter(
    TApiServiceProxy::TReqWriteFilePtr request)
{
    return New<TFileWriter>(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

