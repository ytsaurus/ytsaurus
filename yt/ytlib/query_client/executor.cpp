#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include "query_service_proxy.h"

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>

#include <ytlib/chunk_client/async_reader.h>

namespace NYT {
namespace NQueryClient {

using namespace NRpc;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TRemoteReader
    : public IAsyncReader
{
public:
    TRemoteReader(TFuture<TQueryServiceProxy::TRspExecutePtr> response)
        : Response_(std::move(response))
    { }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override
    {
        return Response_.Apply(BIND(
            &TRemoteReader::ReadBlocks,
            blockIndexes));
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) override
    {
        return Response_.Apply(BIND(
            &TRemoteReader::GetChunkMeta,
            partitionTag,
            MakeNullable(tags)));
    }

    virtual TChunkId GetChunkId() const override
    {
        return NullChunkId;
    }

private:
    TFuture<TQueryServiceProxy::TRspExecutePtr> Response_;

    static TReadResult ReadBlocks(
        const std::vector<int>& blockIndexes,
        TQueryServiceProxy::TRspExecutePtr rsp)
    {
        if (!rsp->IsOK()) {
            return rsp->GetError();
        }
        std::vector<TSharedRef> blocks;
        for (auto index : blockIndexes) {
            YCHECK(index < rsp->Attachments().size());
            blocks.push_back(rsp->Attachments()[index]);
        }
        return std::move(blocks);
    }

    static TGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>> extensionTags,
        TQueryServiceProxy::TRspExecutePtr rsp)
    {
        if (!rsp->IsOK()) {
            return rsp->GetError();
        }
        return rsp->chunk_meta();
    }

};

////////////////////////////////////////////////////////////////////////////////

IReaderPtr DelegateToPeer(const TQueryFragment& fragment, IChannelPtr channel)
{
    TQueryServiceProxy proxy(channel);

    auto req = proxy.Execute();
    ToProto(req->mutable_fragment(), fragment);

    return CreateChunkReader(
        New<TChunkReaderConfig>(),
        New<TRemoteReader>(req->Invoke()));
}

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public IExecutor
{
public:
    TEvaluator(IEvaluateCallbacks* callbacks)
        : Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TQueryFragment& fragment,
        TWriterPtr writer) override
    {
        return MakeFuture(
            TEvaluateController(Callbacks_, fragment).Run(std::move(writer)));
    }

private:
    IEvaluateCallbacks* Callbacks_;

};

IExecutorPtr CreateEvaluator(IEvaluateCallbacks* callbacks)
{
    return New<TEvaluator>(callbacks);
}

////////////////////////////////////////////////////////////////////////////////

class TCoordinator
    : public IExecutor
{
public:
    TCoordinator(ICoordinateCallbacks* callbacks)
        : Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TQueryFragment& fragment,
        TWriterPtr writer) override
    {
        return MakeFuture(
            TCoordinateController(Callbacks_, fragment).Run(std::move(writer)));
    }

private:
    ICoordinateCallbacks* Callbacks_;

};

IExecutorPtr CreateCoordinator(ICoordinateCallbacks* callbacks)
{
    return New<TCoordinator>(callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

