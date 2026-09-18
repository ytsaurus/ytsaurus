#include "session_writer.h"

#include "config.h"
#include "service_proxy.h"

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkWriter
    : public IDistributedChunkWriter
{
public:
    TDistributedChunkWriter(
        TNodeDescriptor sequencerNode,
        TSessionId sessionId,
        IConnectionPtr connection,
        TDistributedChunkWriterConfigPtr config)
        : SequencerNode_(std::move(sequencerNode))
        , SessionId_(sessionId)
        , Connection_(std::move(connection))
        , Config_(std::move(config))
        , Channel_(Connection_->GetChannelFactory()->CreateChannel(
            SequencerNode_.GetAddressOrThrow(Connection_->GetNetworks())))
    { }

    TFuture<void> WriteRecord(
        TSharedRef record,
        TDistributedChunkSessionWriteStatistics statistics) final
    {
        YT_VERIFY(statistics.DataWeight > 0);
        YT_VERIFY(statistics.UncompressedDataSize > 0);
        YT_VERIFY(statistics.RowCount > 0);

        TDistributedChunkSessionServiceProxy proxy(Channel_);
        auto req = proxy.WriteRecord();
        ToProto(req->mutable_session_id(), SessionId_);
        ToProto(req->mutable_statistics(), statistics);

        req->Attachments().push_back(std::move(record));
        req->SetTimeout(Config_->RpcTimeout);

        return req->Invoke().AsVoid();
    }

private:
    const TNodeDescriptor SequencerNode_;
    const TSessionId SessionId_;
    const IConnectionPtr Connection_;
    const TDistributedChunkWriterConfigPtr Config_;
    const IChannelPtr Channel_;
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkWriterPtr CreateDistributedChunkWriter(
    TNodeDescriptor coordinatorNode,
    TSessionId sessionId,
    IConnectionPtr connection,
    TDistributedChunkWriterConfigPtr config)
{
    return New<TDistributedChunkWriter>(
        std::move(coordinatorNode),
        sessionId,
        std::move(connection),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
