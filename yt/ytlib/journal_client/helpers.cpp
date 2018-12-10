#include "helpers.h"
#include "private.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/session_id.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/client/misc/workload.h>

#include <yt/core/misc/string.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT::NJournalClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilder* builder, const TChunkReplicaDescriptor& replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v@%v", replica.NodeDescriptor, replica.MediumIndex);
}

TString ToString(const TChunkReplicaDescriptor& replica)
{
    return ToStringViaBuilder(replica);
}

////////////////////////////////////////////////////////////////////////////////

class TQuorumSessionBase
    : public TRefCounted
{
public:
    TQuorumSessionBase(
        const TChunkId& chunkId,
        const std::vector<TChunkReplicaDescriptor>& replicas,
        TDuration timeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : ChunkId_(chunkId)
        , Replicas_(replicas)
        , Timeout_(timeout)
        , Quorum_(quorum)
        , ChannelFactory_(std::move(channelFactory))
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

protected:
    const TChunkId ChunkId_;
    const std::vector<TChunkReplicaDescriptor> Replicas_;
    const TDuration Timeout_;
    const int Quorum_;
    const INodeChannelFactoryPtr ChannelFactory_;

    NLogging::TLogger Logger = JournalClientLogger;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortSessionsQuorumSession
    : public TQuorumSessionBase
{
public:
    TAbortSessionsQuorumSession(
        const TChunkId& chunkId,
        const std::vector<TChunkReplicaDescriptor>& replicas,
        TDuration timeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : TQuorumSessionBase(chunkId, replicas, timeout, quorum, channelFactory)
        , ChunkId_(chunkId)
    { }

    TFuture<void> Run()
    {
        BIND(&TAbortSessionsQuorumSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    const TChunkId ChunkId_;

    int SuccessCounter_ = 0;
    int ResponseCounter_ = 0;

    std::vector<TError> InnerErrors_;

    TPromise<void> Promise_ = NewPromise<void>();


    void DoRun()
    {
        LOG_INFO("Aborting journal chunk session quorum (Replicas: %v)",
            Replicas_);

        if (Replicas_.size() < Quorum_) {
            auto error = TError("Unable to abort sessions quorum for journal chunk %v: too few replicas known, %v given, %v needed",
                ChunkId_,
                Replicas_.size(),
                Quorum_);
            Promise_.Set(error);
            return;
        }

        for (const auto& replica : Replicas_) {
            auto channel = ChannelFactory_->CreateChannel(replica.NodeDescriptor);
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Timeout_);

            auto req = proxy.FinishChunk();
            ToProto(req->mutable_session_id(), TSessionId(ChunkId_, replica.MediumIndex));
            req->Invoke().Subscribe(BIND(&TAbortSessionsQuorumSession::OnResponse, MakeStrong(this), replica)
                .Via(GetCurrentInvoker()));
        }
    }

    void OnResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspFinishChunkPtr& rspOrError)
    {
        ++ResponseCounter_;

        // NB: Missing session is also OK.
        if (rspOrError.IsOK() || rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchSession) {
            ++SuccessCounter_;
            LOG_INFO("Journal chunk session aborted successfully (Replica: %v)",
                replica);

        } else {
            InnerErrors_.push_back(rspOrError);
            LOG_WARNING(rspOrError, "Failed to abort journal chunk session (Replica: %v)",
                replica);
        }

        if (SuccessCounter_ == Quorum_) {
            LOG_INFO("Journal chunk session quorum aborted successfully");
            Promise_.TrySet();
        }

        if (ResponseCounter_ == Replicas_.size()) {
            auto combinedError = TError("Unable to abort sessions quorum for journal chunk %v",
                ChunkId_)
                << InnerErrors_;
            Promise_.TrySet(combinedError);
        }
    }
};

TFuture<void> AbortSessionsQuorum(
    const TChunkId& chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    INodeChannelFactoryPtr channelFactory)
{
    return New<TAbortSessionsQuorumSession>(chunkId, replicas, timeout, quorum, std::move(channelFactory))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TComputeQuorumInfoSession
    : public TQuorumSessionBase
{
public:
    using TQuorumSessionBase::TQuorumSessionBase;

    TFuture<TMiscExt> Run()
    {
        BIND(&TComputeQuorumInfoSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    std::vector<TMiscExt> Infos_;
    std::vector<TError> InnerErrors_;

    TPromise<TMiscExt> Promise_ = NewPromise<TMiscExt>();


    void DoRun()
    {
        if (Replicas_.size() < Quorum_) {
            auto error = TError("Unable to compute quorum info for journal chunk %v: too few replicas known, %v given, %v needed",
                ChunkId_,
                Replicas_.size(),
                Quorum_);
            Promise_.Set(error);
            return;
        }

        LOG_INFO("Computing quorum info for journal chunk (Replicas: %v)",
            Replicas_);

        std::vector<TFuture<void>> asyncResults;
        for (const auto& replica : Replicas_) {
            auto channel = ChannelFactory_->CreateChannel(replica.NodeDescriptor);
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Timeout_);

            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), ChunkId_);
            req->set_medium_index(replica.MediumIndex);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::SystemTabletRecovery));
            asyncResults.push_back(req->Invoke().Apply(
                BIND(&TComputeQuorumInfoSession::OnResponse, MakeStrong(this), replica)
                    .AsyncVia(GetCurrentInvoker())));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TComputeQuorumInfoSession::OnComplete, MakeStrong(this))
                .Via(GetCurrentInvoker()));
    }

    void OnResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            auto miscExt = GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());

            Infos_.push_back(miscExt);

            LOG_INFO("Received info for journal chunk (Address: %v, MediumIndex: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
                replica.NodeDescriptor.GetDefaultAddress(),
                replica.MediumIndex,
                miscExt.row_count(),
                miscExt.uncompressed_data_size(),
                miscExt.compressed_data_size());
        } else {
            InnerErrors_.push_back(rspOrError);

            LOG_WARNING(rspOrError, "Failed to get journal info (Replica: %v)",
                replica);
        }
    }

    void OnComplete(const TError&)
    {
        if (Infos_.size() < Quorum_) {
            auto error = TError("Unable to compute quorum info for journal chunk %v: too few replicas alive, %v found, %v needed",
                ChunkId_,
                Infos_.size(),
                Quorum_)
                << InnerErrors_;
            Promise_.Set(error);
            return;
        }

        std::sort(
            Infos_.begin(),
            Infos_.end(),
            [] (const TMiscExt& lhs, const TMiscExt& rhs) {
                return lhs.row_count() < rhs.row_count();
            });

        const auto& quorumInfo = Infos_[Quorum_ - 1];

        LOG_INFO("Quorum info for journal chunk computed successfully (RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            quorumInfo.row_count(),
            quorumInfo.uncompressed_data_size(),
            quorumInfo.compressed_data_size());

        Promise_.Set(quorumInfo);
    }
};

TFuture<TMiscExt> ComputeQuorumInfo(
    const TChunkId& chunkId,
    const std::vector<TChunkReplicaDescriptor>& replicas,
    TDuration timeout,
    int quorum,
    INodeChannelFactoryPtr channelFactory)
{
    auto session = New<TComputeQuorumInfoSession>(
        chunkId,
        replicas,
        timeout,
        quorum,
        std::move(channelFactory));
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

