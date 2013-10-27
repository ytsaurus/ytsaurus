#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include "query_service_proxy.h"

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/reader.h>

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

class TRemoteExecutor
    : public IExecutor
{
public:
    TRemoteExecutor(TChunkReaderConfigPtr config, IChannelPtr channel)
        : Config_(std::move(config))
        , Channel_(std::move(channel))
    { }

    virtual IReaderPtr Execute(const TQueryFragment& fragment) override
    {
        TQueryServiceProxy proxy(Channel_);
        auto req = proxy.Execute();
        ToProto(req->mutable_fragment(), fragment);

        return CreateChunkReader(Config_, New<TRemoteReader>(req->Invoke()));
    }

private:
    TChunkReaderConfigPtr Config_;
    IChannelPtr Channel_;

};

IExecutorPtr CreateRemoteExecutor(IChannelPtr channel)
{
    return New<TRemoteExecutor>(
        New<TChunkReaderConfig>(),
        std::move(channel));
}

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public IExecutor
{
public:
    TEvaluator(IEvaluateCallbacks* callbacks)
        : Callbacks_(callbacks)
    { }

    virtual IReaderPtr Execute(const TQueryFragment& fragment) override
    {
        return TEvaluateController(Callbacks_, fragment).GetReader();
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

    virtual IReaderPtr Execute(const TQueryFragment& fragment) override
    {
        return TCoordinateController(Callbacks_, fragment).GetReader();
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

