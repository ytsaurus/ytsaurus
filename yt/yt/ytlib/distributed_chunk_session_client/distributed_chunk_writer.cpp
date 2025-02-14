#include "distributed_chunk_writer.h"

#include "config.h"
#include "distributed_chunk_session_service_proxy.h"

#include <yt/yt/ytlib/table_client/schemaless_block_writer.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NTableClient;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkWriter
    : public IDistributedChunkWriter
{
public:
    TDistributedChunkWriter(
        TNodeDescriptor coordinatorNode,
        TSessionId sessionId,
        IConnectionPtr connection,
        TDistributedChunkWriterConfigPtr config,
        IInvokerPtr invoker)
        : CoordinatorNode_(std::move(coordinatorNode))
        , SessionId_(sessionId)
        , Connection_(std::move(connection))
        , Config_(std::move(config))
        , Invoker_(std::move(invoker))
    {
        const auto& channelFactory = Connection_->GetChannelFactory();
        Channel_ = channelFactory->CreateChannel(
            CoordinatorNode_.GetAddressOrThrow(Connection_->GetNetworks()));
    }

    TFuture<void> Write(TSharedRange<TUnversionedRow> rows) final
    {
        return BIND(
            &TDistributedChunkWriter::DoWrite,
            MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(rows);
    }

private:
    const TNodeDescriptor CoordinatorNode_;
    const TSessionId SessionId_;
    const IConnectionPtr Connection_;
    const TDistributedChunkWriterConfigPtr Config_;
    const IInvokerPtr Invoker_;

    IChannelPtr Channel_;

    void DoWrite(TSharedRange<TUnversionedRow> rows)
    {
        THorizontalBlockWriter blockWriter(New<TTableSchema>());

        i64 dataWeight = 0;

        for (const auto& row : rows) {
            blockWriter.WriteRow(row);

            dataWeight += 1;
            for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                dataWeight += GetDataWeight(row[index]);
            }
        }

        auto block = blockWriter.FlushBlock();

        TDistributedChunkSessionServiceProxy proxy(Channel_);
        auto req = proxy.SendBlocks();
        ToProto(req->mutable_session_id(), SessionId_);

        auto* blocksMiscMeta = req->mutable_blocks_misc_meta();
        blocksMiscMeta->set_data_weight(dataWeight);
        blocksMiscMeta->set_row_count(std::ssize(rows));

        block.Meta.set_chunk_row_count(0);
        block.Meta.set_block_index(0);
        *req->add_data_block_metas() = block.Meta;

        req->Attachments().push_back(
            MergeRefsToRef<TDefaultBlobTag>(block.Data));

        req->SetTimeout(Config_->RpcTimeout);

        WaitFor(req->Invoke())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkWriterPtr CreateDistributedChunkWriter(
    TNodeDescriptor coordinatorNode,
    TSessionId sessionId,
    IConnectionPtr connection,
    TDistributedChunkWriterConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkWriter>(
        std::move(coordinatorNode),
        sessionId,
        std::move(connection),
        std::move(config),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
