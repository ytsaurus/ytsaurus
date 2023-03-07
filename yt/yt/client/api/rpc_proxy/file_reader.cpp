#include "file_reader.h"

#include <yt/client/api/file_reader.h>

#include <yt/client/hydra/public.h>

#include <yt/core/rpc/stream.h>

namespace NYT::NApi::NRpcProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public IFileReader
{
public:
    TFileReader(
        IAsyncZeroCopyInputStreamPtr underlying,
        NHydra::TRevision revision)
        : Underlying_(std::move(underlying))
        , Revision_(revision)
    {
        YT_VERIFY(Underlying_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        return Underlying_->Read();
    }

    virtual NHydra::TRevision GetRevision() const override
    {
        return Revision_;
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const NHydra::TRevision Revision_;
};

TFuture<IFileReaderPtr> CreateFileReader(
    TApiServiceProxy::TReqReadFilePtr request)
{
    return NRpc::CreateRpcClientInputStream(std::move(request))
        .Apply(BIND([=] (const IAsyncZeroCopyInputStreamPtr& inputStream) {
            return inputStream->Read().Apply(BIND([=] (const TSharedRef& metaRef) {
                NApi::NRpcProxy::NProto::TReadFileMeta meta;
                if (!TryDeserializeProto(&meta, metaRef)) {
                    THROW_ERROR_EXCEPTION("Failed to deserialize file stream header");
                }

                return New<TFileReader>(inputStream, meta.revision());
            })).As<IFileReaderPtr>();
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

