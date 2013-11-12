#include "executor.h"

#include "private.h"

#include "coordinate_controller.h"
#include "evaluate_controller.h"

#include "query_service_proxy.h"

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/async_reader.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/writer.h>

namespace NYT {
namespace NQueryClient {

using namespace NNodeTrackerClient;
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
    explicit TRemoteReader(TFuture<TQueryServiceProxy::TRspExecutePtr> response)
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

IReaderPtr DelegateToPeer(
    const TPlanFragment& planFragment,
    TNodeDirectoryPtr nodeDirectory,
    IChannelPtr channel)
{
    TQueryServiceProxy proxy(channel);

    auto req = proxy.Execute();

    nodeDirectory->DumpTo(req->mutable_node_directory());
    ToProto(req->mutable_plan_fragment(), planFragment);

    return CreateChunkReader(
        New<TChunkReaderConfig>(),
        New<TRemoteReader>(req->Invoke()));
}

////////////////////////////////////////////////////////////////////////////////

template <class TController, class TCallbacks>
class TControlledExecutor
    : public IExecutor
{
public:
    TControlledExecutor(IInvokerPtr invoker, TCallbacks* callbacks)
        : Invoker_(std::move(invoker))
        , Callbacks_(callbacks)
    { }

    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) override
    {
        auto controller = New<TController>(
            Callbacks_,
            fragment,
            std::move(writer));
        return BIND(&TController::Run, controller)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    IInvokerPtr Invoker_;
    TCallbacks* Callbacks_;

};

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks)
{
    typedef TControlledExecutor<TEvaluateController, IEvaluateCallbacks> TExecutor;
    return New<TExecutor>(std::move(invoker), callbacks);
}

IExecutorPtr CreateCoordinator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks)
{
    typedef TControlledExecutor<TCoordinateController, ICoordinateCallbacks> TExecutor;
    return New<TExecutor>(std::move(invoker), callbacks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

