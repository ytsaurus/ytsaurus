#include "file_reader.h"

#include <yt/client/api/file_reader.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRpcFileReader
    : public IFileReader
{
public:
    TRpcFileReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        ui64 revision):
        Underlying_(std::move(underlying)),
        Revision_(revision)
    {
        YCHECK(Underlying_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read();
    }

    virtual ui64 GetRevision() const override
    {
        return Revision_;
    }

private:
    IAsyncZeroCopyInputStreamPtr Underlying_;
    ui64 Revision_;
};

TFuture<IFileReaderPtr> CreateRpcFileReader(
    TApiServiceProxy::TReqCreateFileReaderPtr request)
{
    return NRpc::CreateInputStreamAdapter(request)
        .Apply(BIND([=] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
            return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
                NApi::NRpcProxy::NProto::TMetaCreateFileReader meta;
                if (!TryDeserializeProto(&meta, metaRef)) {
                    THROW_ERROR_EXCEPTION("Failed to deserialize file reader revision");
                }

                return New<TRpcFileReader>(inputStream, meta.revision());
            })).As<IFileReaderPtr>();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
