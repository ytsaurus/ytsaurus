#include "job_input_reader.h"
#include "public.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcJobInputReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TRpcJobInputReader(
        TApiServiceProxy::TReqGetJobInputPtr request):
        Request_(request)
    {
        YCHECK(Request_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return Request_->GetResponseAttachmentsStream()->Read();
    }

    ~TRpcJobInputReader() {
        // TODO(kiselyovp) doing work in destructor but there's no better place?
        NConcurrency::WaitFor(Request_->GetRequestAttachmentsStream()->Close());
    }

private:
    TApiServiceProxy::TReqGetJobInputPtr Request_;
};

TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateRpcJobInputReader(
    TApiServiceProxy::TReqGetJobInputPtr request)
{
    request->Invoke();

    return request->GetResponseAttachmentsStream()->Read() // TODOKETE this should be a separated function?? (ExpectHandshake)
        .Apply(BIND ([request] (const TErrorOr<TSharedRef>& refOrError) {
            const auto& ref = refOrError.ValueOrThrow();
            if (ToString(ref) != JobInputReaderHandshake) {
                THROW_ERROR_EXCEPTION("Failed to create a job input reader: handshake mismatch");
            }

            return New<TRpcJobInputReader>(request);
        })).As<NConcurrency::IAsyncZeroCopyInputStreamPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
