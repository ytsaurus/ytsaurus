#include "helpers.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/journal_client/proto/format.pb.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>

namespace NYT::NJournalClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

void ValidateJournalAttributes(
    NErasure::ECodec codecId,
    int replicationFactor,
    int readQuorum,
    int writeQuorum)
{
    if (readQuorum < 1) {
        THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be less than 1");
    }
    if (writeQuorum < 1) {
        THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be less than 1");
    }
    if (codecId == NErasure::ECodec::None) {
        ValidateReplicationFactor(replicationFactor);
        if (readQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (writeQuorum > replicationFactor) {
            THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than \"replication_factor\"");
        }
        if (readQuorum + writeQuorum <= replicationFactor) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum <= replication_factor");
        }
    } else {
        auto* codec = NErasure::GetCodec(codecId);
        if (!codec->IsBytewise()) {
            THROW_ERROR_EXCEPTION("%Qlv codec is not suitable for erasure journals",
                codecId);
        }
        if (replicationFactor != 1) {
            THROW_ERROR_EXCEPTION("\"replication_factor\" must be 1 for erasure journals",
                codec->GetGuaranteedRepairablePartCount());
        }
        if (readQuorum > codec->GetTotalPartCount()) {
            THROW_ERROR_EXCEPTION("\"read_quorum\" cannot be greater than total part count");
        }
        if (writeQuorum > codec->GetTotalPartCount()) {
            THROW_ERROR_EXCEPTION("\"write_quorum\" cannot be greater than total part count");
        }
        int quorumThreshold = 2 * codec->GetTotalPartCount() - codec->GetGuaranteedRepairablePartCount() - 1;
        if (readQuorum + writeQuorum <= quorumThreshold) {
            THROW_ERROR_EXCEPTION("Read/write quorums are not safe: read_quorum + write_quorum <= 2 * total_parts - guraranteed_repairable_parts - 1");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

i64 GetPaddingSize(i64 size, i64 alignment)
{
    auto remainder = size % alignment;
    return remainder == 0 ? 0 : alignment - remainder;
}

i64 AlignSize(i64 size, i64 alignment)
{
    return size + GetPaddingSize(size, alignment);
}

} // namespace

class TJournalErasureBuffer
{
public:
    template <class TTag>
    TJournalErasureBuffer(i64 size, TTag)
        : Buffer_(TSharedMutableRef::Allocate<TTag>(size))
        , Current_(Buffer_.Begin())
    { }

    void Write(const void* src, i64 size)
    {
        std::copy(static_cast<const char*>(src), static_cast<const char*>(src) + size, Current_);
        Current_ += size;
    }

    void WritePadding(i64 size)
    {
        std::fill(Current_, Current_ + size, 0);
        Current_ += size;
    }

    void WriteAlignment(i64 alignment)
    {
        WritePadding(GetPaddingSize(Current_ - Buffer_.Begin(), alignment));
    }

    char* GetCurrent() const
    {
        return Current_;
    }

    bool IsFull() const
    {
        return Current_ == Buffer_.End();
    }

    TSharedMutableRef SliceFrom(char* from) const
    {
        return Buffer_.Slice(from, Current_);
    }

private:
    TSharedMutableRef Buffer_;
    char* Current_;
};

std::vector<std::vector<TSharedRef>> EncodeErasureJournalRows(
    NErasure::ICodec* codec,
    const std::vector<TSharedRef>& rows)
{
    int dataPartCount = codec->GetDataPartCount();
    int totalPartCount = codec->GetTotalPartCount();

    auto getRowPaddingSize = [&] (const TSharedRef& row) {
        return GetPaddingSize(sizeof(TErasureRowHeader) + row.Size(), dataPartCount);
    };

    auto getRowPartSize = [&] (const TSharedRef& row) {
        return AlignSize(sizeof(TErasureRowHeader) + row.Size(), dataPartCount) / dataPartCount;
    };

    i64 bufferSize = 0;
    for (const auto& row : rows) {
        bufferSize += getRowPartSize(row);
    }
    bufferSize *= dataPartCount;

    struct TEncodedRowsBufferTag { };
    TJournalErasureBuffer buffer(bufferSize, TEncodedRowsBufferTag());

    std::vector<TSharedRef> dataParts;
    dataParts.reserve(dataPartCount);
    for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
        char* partBegin = buffer.GetCurrent();
        for (const auto& row : rows) {
            i64 rowPartSize = getRowPartSize(row);
            if (partIndex == 0) {
                TErasureRowHeader header{
                    .PaddingSize = static_cast<i8>(getRowPaddingSize(row))
                };
                buffer.Write(&header, sizeof(header));
                buffer.Write(row.Begin(), rowPartSize - sizeof(header));
            } else {
                i64 rowBeginOffset = partIndex * rowPartSize - sizeof(TErasureRowHeader);
                i64 rowEndOffset = std::min(static_cast<i64>(row.Size()), rowBeginOffset + rowPartSize);
                i64 rowCopySize = std::max(rowEndOffset - rowBeginOffset, static_cast<i64>(0));
                buffer.Write(row.Begin() + rowBeginOffset, rowCopySize);
                buffer.WritePadding(rowPartSize - rowCopySize);
            }
        }
        dataParts.push_back(buffer.SliceFrom(partBegin));
    }
    YT_VERIFY(buffer.IsFull());

    auto parityParts = codec->Encode(dataParts);

    std::vector<std::vector<TSharedRef>> encodedRowLists;
    encodedRowLists.resize(totalPartCount);
    for (int partIndex = 0; partIndex < totalPartCount; ++partIndex) {
        auto& partRows = encodedRowLists[partIndex];
        const auto& part = partIndex < dataPartCount
            ? dataParts[partIndex]
            : parityParts[partIndex - dataPartCount];
        i64 partOffset = 0;
        for (i64 rowIndex = 0; rowIndex < static_cast<i64>(rows.size()); ++rowIndex) {
            i64 rowPartSize = getRowPartSize(rows[rowIndex]);
            partRows.push_back(part.Slice(partOffset, partOffset + rowPartSize));
            partOffset += rowPartSize;
        }
    }

    return encodedRowLists;
}

std::vector<TSharedRef> EncodeErasureJournalRow(
    NErasure::ICodec* codec,
    const TSharedRef& row)
{
    return EncodeErasureJournalRows(codec, {row})[0];
}

std::vector<TSharedRef> DecodeErasureJournalRows(
    NErasure::ICodec* codec,
    const std::vector<std::vector<TSharedRef>>& encodedRowLists)
{
    int dataPartCount = codec->GetDataPartCount();

    YT_VERIFY(dataPartCount == std::ssize(encodedRowLists));

    i64 rowCount = Max<i64>();
    for (const auto& encodedRows : encodedRowLists) {
        rowCount = std::min(rowCount, static_cast<i64>(encodedRows.size()));
    }

    i64 bufferSize = 0;
    for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        bufferSize += encodedRowLists[0][rowIndex].Size();
    }
    bufferSize *= dataPartCount;

    struct TDecodedRowsBufferTag { };
    TJournalErasureBuffer buffer(bufferSize, TDecodedRowsBufferTag());

    std::vector<TSharedRef> decodedRows;
    decodedRows.reserve(rowCount);

    for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        const auto* header = reinterpret_cast<const TErasureRowHeader*>(encodedRowLists[0][rowIndex].Begin());
        YT_VERIFY(header->PaddingSize >= 0);
        i64 bytesRemaining =
            static_cast<i64>(encodedRowLists[0][rowIndex].size()) * dataPartCount -
            header->PaddingSize -
            sizeof(TErasureRowHeader);
        char* decodedRowBegin = buffer.GetCurrent();
        for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
            const auto& encodedRow = encodedRowLists[partIndex][rowIndex];
            const char* encodedRowBegin = encodedRow.Begin();
            const char* encodedRowEnd = encodedRow.End();
            if (partIndex == 0) {
                encodedRowBegin += sizeof(TErasureRowHeader);
            }
            i64 bytesToCopy = std::min(bytesRemaining, encodedRowEnd - encodedRowBegin);
            buffer.Write(encodedRowBegin, bytesToCopy);
            bytesRemaining -= bytesToCopy;
        }
        decodedRows.push_back(buffer.SliceFrom(decodedRowBegin));
    }

    return decodedRows;
}

std::vector<std::vector<TSharedRef>> RepairErasureJournalRows(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<std::vector<TSharedRef>>& repairRowLists)
{
    if (erasedIndices.empty()) {
        return {};
    }

    i64 rowCount = repairRowLists[0].size();
    for (const auto& repairRows : repairRowLists) {
        YT_VERIFY(std::ssize(repairRows) == rowCount);
    }

    i64 bufferSize = 0;
    for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        bufferSize += repairRowLists[0][rowIndex].Size();
    }
    bufferSize *= repairRowLists.size();

    struct TRepairRowsBufferTag { };
    TJournalErasureBuffer buffer(bufferSize, TRepairRowsBufferTag());

    std::vector<TSharedRef> repairParts;
    repairParts.reserve(repairRowLists.size());
    for (const auto& repairRows : repairRowLists) {
        char* partBegin = buffer.GetCurrent();
        for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            const auto& repairRow = repairRows[rowIndex];
            YT_VERIFY(repairRow.Size() == repairRowLists[0][rowIndex].size());
            buffer.Write(repairRow.Begin(), repairRow.Size());
        }
        repairParts.push_back(buffer.SliceFrom(partBegin));
    }
    YT_VERIFY(buffer.IsFull());

    auto erasedParts = codec->Decode(repairParts, erasedIndices);

    std::vector<std::vector<TSharedRef>> erasedRowLists;
    erasedRowLists.reserve(erasedIndices.size());
    for (const auto& erasedPart : erasedParts) {
        i64 partOffset = 0;
        auto& erasedRows = erasedRowLists.emplace_back();
        erasedRows.reserve(repairRowLists[0].size());
        for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            const auto& repairRow = repairRowLists[0][rowIndex];
            erasedRows.push_back(erasedPart.Slice(partOffset, partOffset + repairRow.Size()));
            partOffset += repairRow.Size();
        }
    }

    return erasedRowLists;
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkReplicaDescriptor& replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.NodeDescriptor);
    if (replica.ReplicaIndex != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.ReplicaIndex);
    }
    if (replica.MediumIndex == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.MediumIndex != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.MediumIndex);
    }
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
        TChunkId chunkId,
        std::vector<TChunkReplicaDescriptor> replicas,
        TDuration requestTimeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : ChunkId_(chunkId)
        , Replicas_(std::move(replicas))
        , RequestTimeout_(requestTimeout)
        , Quorum_(quorum)
        , ChannelFactory_(std::move(channelFactory))
        , Logger(JournalClientLogger.WithTag("ChunkId: %v", ChunkId_))
    { }

protected:
    const TChunkId ChunkId_;
    const std::vector<TChunkReplicaDescriptor> Replicas_;
    const TDuration RequestTimeout_;
    const int Quorum_;
    const INodeChannelFactoryPtr ChannelFactory_;

    const NLogging::TLogger Logger;

    const IInvokerPtr Invoker_ = CreateSerializedInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker());
};

////////////////////////////////////////////////////////////////////////////////

class TAbortSessionsQuorumSession
    : public TQuorumSessionBase
{
public:
    TAbortSessionsQuorumSession(
        TChunkId chunkId,
        std::vector<TChunkReplicaDescriptor> replicas,
        TDuration abortRequestTimeout,
        TDuration quorumSessionDelay,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : TQuorumSessionBase(
            chunkId,
            std::move(replicas),
            abortRequestTimeout,
            quorum,
            std::move(channelFactory))
        , QuorumSessionDelay_(quorumSessionDelay)
    { }

    TFuture<std::vector<TChunkReplicaDescriptor>> Run()
    {
        YT_UNUSED_FUTURE(BIND(&TAbortSessionsQuorumSession::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run());
        return Promise_;
    }

private:
    const TDuration QuorumSessionDelay_;

    int SuccessCounter_ = 0;
    int ResponseCounter_ = 0;
    NErasure::TPartIndexSet SuccessPartIndexes_;

    std::vector<TError> InnerErrors_;
    std::vector<TChunkReplicaDescriptor> AbortedReplicas_;

    const TPromise<std::vector<TChunkReplicaDescriptor>> Promise_ = NewPromise<std::vector<TChunkReplicaDescriptor>>();

    bool QuorumSessionDelayReached_ = false;

    void DoRun()
    {
        YT_LOG_DEBUG("Aborting journal chunk session quorum (Replicas: %v, Quorum: %v)",
            Replicas_,
            Quorum_);

        if (std::ssize(Replicas_) < Quorum_) {
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
            proxy.SetDefaultTimeout(RequestTimeout_);

            auto chunkIdWithIndex = TChunkIdWithIndex(ChunkId_, replica.ReplicaIndex);
            auto sessionId = TSessionId(EncodeChunkId(chunkIdWithIndex), replica.MediumIndex);

            auto req = proxy.FinishChunk();
            ToProto(req->mutable_session_id(), sessionId);
            req->set_ignore_missing_session(true);

            req->Invoke().Subscribe(BIND(&TAbortSessionsQuorumSession::OnResponse, MakeStrong(this), replica)
                .Via(Invoker_));
        }

        TDelayedExecutor::Submit(
            BIND(&TAbortSessionsQuorumSession::OnQuorumSessionDelayReached, MakeWeak(this))
                .Via(Invoker_),
            QuorumSessionDelay_);
    }

    void OnResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspFinishChunkPtr& rspOrError)
    {
        ++ResponseCounter_;

        // NB: Missing session is also OK.
        if (rspOrError.IsOK() || rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchSession) {
            ++SuccessCounter_;
            AbortedReplicas_.push_back(replica);
            if (replica.ReplicaIndex != GenericChunkReplicaIndex) {
                SuccessPartIndexes_.set(replica.ReplicaIndex);
            }

            YT_LOG_DEBUG("Journal chunk session aborted successfully (Replica: %v)",
                replica);
        } else {
            YT_LOG_WARNING(rspOrError, "Failed to abort journal chunk session (Replica: %v)",
                replica);
            InnerErrors_.push_back(rspOrError);
        }

        CheckCompleted();
    }

    bool IsQuorumReached()
    {
        return IsErasureChunkId(ChunkId_)
            ? static_cast<ssize_t>(SuccessPartIndexes_.count()) >= Quorum_
            : SuccessCounter_ >= Quorum_;
    }

    void OnQuorumSessionDelayReached()
    {
        YT_LOG_DEBUG("Quorum session delay reached (QuorumSessionDelay: %v, AbortedReplicaCount: %v)",
            QuorumSessionDelay_,
            AbortedReplicas_.size());

        QuorumSessionDelayReached_ = true;
        CheckCompleted();
    }

    void CheckCompleted()
    {
        if (IsQuorumReached()) {
            if (ResponseCounter_ != std::ssize(Replicas_) && !QuorumSessionDelayReached_) {
                return;
            }
            if (Promise_.TrySet(AbortedReplicas_)) {
                YT_LOG_DEBUG("Journal chunk session quorum aborted successfully");
            }
        } else if (ResponseCounter_ == std::ssize(Replicas_)) {
            auto combinedError = TError("Unable to abort sessions quorum for journal chunk %v",
                ChunkId_)
                << InnerErrors_;
            Promise_.TrySet(combinedError);
        }
    }
};

TFuture<std::vector<TChunkReplicaDescriptor>> AbortSessionsQuorum(
    TChunkId chunkId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration abortRequestTimeout,
    TDuration quorumSessionDelay,
    int quorum,
    INodeChannelFactoryPtr channelFactory)
{
    return
        New<TAbortSessionsQuorumSession>(
            chunkId,
            std::move(replicas),
            abortRequestTimeout,
            quorumSessionDelay,
            quorum,
            std::move(channelFactory))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TComputeQuorumInfoSession
    : public TQuorumSessionBase
{
public:
    TComputeQuorumInfoSession(
        TChunkId chunkId,
        bool overlayed,
        NErasure::ECodec codecId,
        int quorum,
        i64 replicaLagLimit,
        std::vector<TChunkReplicaDescriptor> replicas,
        TDuration requestTimeout,
        INodeChannelFactoryPtr channelFactory)
        : TQuorumSessionBase(
            chunkId,
            std::move(replicas),
            requestTimeout,
            quorum,
            std::move(channelFactory))
        , Overlayed_(overlayed)
        , CodecId_(codecId)
        , ReplicaLagLimit_(replicaLagLimit)
    { }

    TFuture<TChunkQuorumInfo> Run()
    {
        YT_UNUSED_FUTURE(BIND(&TComputeQuorumInfoSession::DoRun, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run());
        return Promise_;
    }

private:
    const bool Overlayed_;
    const NErasure::ECodec CodecId_;
    const i64 ReplicaLagLimit_;

    struct TChunkMetaResult
    {
        TString Address;
        TMiscExt MiscExt;
        TChunkLocationUuid LocationUuid;
    };
    std::vector<TChunkMetaResult> ChunkMetaResults_;

    std::optional<NJournalClient::NProto::TOverlayedJournalChunkHeader> Header_;

    std::vector<TError> ChunkMetaInnerErrors_;
    std::vector<TError> HeaderBlockInnerErrors_;

    const TPromise<TChunkQuorumInfo> Promise_ = NewPromise<TChunkQuorumInfo>();


    void DoRun()
    {
        if (!IsJournalChunkId(ChunkId_)) {
            auto error = TError("Unable to compute quorum info for non-journal chunk %v",
                ChunkId_);
            Promise_.Set(error);
            return;
        }

        YT_VERIFY(Quorum_ > 0);

        if (std::ssize(Replicas_) < Quorum_) {
            auto error = TError("Unable to compute quorum info for journal chunk %v: too few replicas known, %v given, %v needed",
                ChunkId_,
                Replicas_.size(),
                Quorum_);
            Promise_.Set(error);
            return;
        }

        YT_LOG_DEBUG("Computing quorum info for journal chunk (Replicas: %v, Quorum: %v, Codec: %v)",
            Replicas_,
            Quorum_,
            CodecId_);

        std::vector<TFuture<void>> futures;
        for (const auto& replica : Replicas_) {
            auto channel = ChannelFactory_->CreateChannel(replica.NodeDescriptor);
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(RequestTimeout_);

            auto chunkIdWithIndex = TChunkIdWithIndex(ChunkId_, replica.ReplicaIndex);
            auto partChunkId = EncodeChunkId(chunkIdWithIndex);

            // Request chunk meta.
            {
                auto req = proxy.GetChunkMeta();
                SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::SystemTabletRecovery));
                ToProto(req->mutable_chunk_id(), partChunkId);
                req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
                req->set_supported_chunk_features(ToUnderlying(GetSupportedChunkFeatures()));

                futures.push_back(req->Invoke().Apply(
                    BIND(&TComputeQuorumInfoSession::OnGetChunkMetaResponse, MakeStrong(this), replica)
                        .AsyncVia(Invoker_)));
            }

            // Request header block.
            if (Overlayed_) {
                auto req = proxy.GetBlockRange();
                SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::SystemTabletRecovery));
                ToProto(req->mutable_chunk_id(), partChunkId);
                req->set_first_block_index(0);
                req->set_block_count(1);

                futures.push_back(req->Invoke().Apply(
                    BIND(&TComputeQuorumInfoSession::OnGetHeaderBlockResponse, MakeStrong(this), replica)
                        .AsyncVia(Invoker_)));
            }
        }

        AllSucceeded(futures).Subscribe(
            BIND(&TComputeQuorumInfoSession::OnComplete, MakeStrong(this))
                .Via(Invoker_));
    }

    void OnGetChunkMetaResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to get journal chunk meta (Replica: %v)",
                replica);
            ChunkMetaInnerErrors_.push_back(TError("Failed to get chunk meta for replica %v", replica)
                << rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        const auto& address = replica.NodeDescriptor.GetDefaultAddress();
        auto miscExt = GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());
        auto locationUuid = FromProto<TChunkLocationUuid>(rsp->location_uuid());

        ChunkMetaResults_.push_back({
            address,
            miscExt,
            locationUuid,
        });

        YT_LOG_DEBUG("Received chunk meta for journal chunk (Replica: %v, LocationUuid: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            replica,
            locationUuid,
            miscExt.row_count(),
            miscExt.uncompressed_data_size(),
            miscExt.compressed_data_size());
    }

    void OnGetHeaderBlockResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspGetBlockRangePtr& rspOrError)
    {
        if (Header_) {
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Failed to get journal chunk header block (Replica: %v)",
                replica);
            HeaderBlockInnerErrors_.push_back(TError("Failed to get journal chunk header block for replica %v", replica)
                << rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();

        if (rsp->Attachments().size() < 1) {
            YT_LOG_DEBUG(rspOrError, "Journal replica did not return chunk header block (Replica: %v)",
                replica);
            HeaderBlockInnerErrors_.push_back(TError("Journal replica %v did not return chunk header block", replica));
            return;
        }

        NJournalClient::NProto::TOverlayedJournalChunkHeader header;
        if (!TryDeserializeProto(&header, rsp->Attachments()[0])) {
            YT_LOG_WARNING(rspOrError, "Error parsing journal chunk header block (Replica: %v)",
                replica);
            ChunkMetaInnerErrors_.push_back(TError("Error parsing journal chunk header block received from replica %v", replica)
                << rspOrError);
            return;
        }

        if (!header.has_first_row_index()) {
            YT_LOG_WARNING(rspOrError, "Received journal chunk header block without first row index (Replica: %v)",
                replica);
            ChunkMetaInnerErrors_.push_back(TError("Received journal chunk header block without first row index from replica %v", replica)
                << rspOrError);
            return;
        }

        YT_LOG_DEBUG("Received header block for journal chunk (Replica: %v, FirstOverlayedRowIndex: %v)",
            replica,
            header.first_row_index());

        Header_.emplace(std::move(header));
    }

    void OnComplete(const TError& /*error*/)
    {
        THashMap<TChunkLocationUuid, TString> locationUuidToAddress;
        for (const auto& result : ChunkMetaResults_) {
            if (!result.LocationUuid) {
                continue;
            }
            auto it = locationUuidToAddress.find(result.LocationUuid);
            if (it == locationUuidToAddress.end()) {
                YT_VERIFY(locationUuidToAddress.emplace(result.LocationUuid, result.Address).second);
            } else if (it->second != result.Address) {
                Promise_.Set(TError("Coinciding location uuid %v reported by nodes %v and %v",
                    result.LocationUuid,
                    result.Address,
                    it->second));
                return;
            }
        }

        if (std::ssize(ChunkMetaResults_) < Quorum_) {
            Promise_.Set(TError("Unable to compute quorum info for journal chunk %v: too few replicas alive, %v found, %v needed",
                ChunkId_,
                ChunkMetaResults_.size(),
                Quorum_)
                << ChunkMetaInnerErrors_);
            return;
        }

        const auto& quorumResult = GetQuorumResult();
        const auto& miscExt = quorumResult.MiscExt;

        TChunkQuorumInfo result;
        if (Overlayed_ && miscExt.row_count() > 0) {
            if (!Header_) {
                Promise_.Set(TError("Could not receive successful response to any header request for overlayed journal chunk %v",
                    ChunkId_)
                    << HeaderBlockInnerErrors_);
                return;
            }
            result.FirstOverlayedRowIndex = Header_->first_row_index();
        }

        result.RowCount = GetLogicalChunkRowCount(miscExt.row_count(), Overlayed_);
        result.UncompressedDataSize = miscExt.uncompressed_data_size();
        result.CompressedDataSize = miscExt.compressed_data_size();

        for (const auto& replicaResult : ChunkMetaResults_) {
            if (GetLogicalChunkRowCount(replicaResult.MiscExt.row_count(), Overlayed_) >= result.RowCount) {
                ++result.RowCountConfirmedReplicaCount;
            }
        }

        YT_LOG_DEBUG("Quorum info for journal chunk computed successfully (PhysicalRowCount: %v, LogicalRowCount: %v, "
            "FirstOverlayedRowIndex: %v, UncompressedDataSize: %v, CompressedDataSize: %v, RowCountConfirmedReplicaCount: %v)",
            miscExt.row_count(),
            result.RowCount,
            result.FirstOverlayedRowIndex,
            result.UncompressedDataSize,
            result.CompressedDataSize,
            result.RowCountConfirmedReplicaCount);

        Promise_.Set(result);
    }

    const TChunkMetaResult& GetQuorumResult()
    {
        return IsErasureChunkId(ChunkId_)
            ? GetErasureQuorumResult()
            : GetRegularQuorumResult();
    }

    const TChunkMetaResult& GetErasureQuorumResult()
    {
        std::sort(
            ChunkMetaResults_.begin(),
            ChunkMetaResults_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.MiscExt.row_count() > rhs.MiscExt.row_count();
            });

        YT_VERIFY(!ChunkMetaResults_.empty());
        ValidateReplicaLag(ChunkMetaResults_.back(), ChunkMetaResults_.front());

        auto* codec = NErasure::GetCodec(CodecId_);
        return ChunkMetaResults_[codec->GetGuaranteedRepairablePartCount() - 1];
    }

    const TChunkMetaResult& GetRegularQuorumResult()
    {
        std::sort(
            ChunkMetaResults_.begin(),
            ChunkMetaResults_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.MiscExt.row_count() < rhs.MiscExt.row_count();
            });

        YT_VERIFY(!ChunkMetaResults_.empty());
        ValidateReplicaLag(ChunkMetaResults_.front(), ChunkMetaResults_.back());

        return ChunkMetaResults_[Quorum_ - 1];
    }

    void ValidateReplicaLag(
        const TChunkMetaResult& shortestReplica,
        const TChunkMetaResult& longestReplica)
    {
        i64 longestReplicaRowCount = GetLogicalChunkRowCount(longestReplica.MiscExt.row_count(), Overlayed_);
        i64 shortestReplicaRowCount = GetLogicalChunkRowCount(shortestReplica.MiscExt.row_count(), Overlayed_);
        if (longestReplicaRowCount - shortestReplicaRowCount > ReplicaLagLimit_) {
            YT_LOG_ALERT("Replica lag limit violated "
                "(ShortestReplicaAddress: %v, ShortestReplicaRowCount: %v, "
                "LongestReplicaAddress: %v, LongestReplicaRowCount: %v, ReplicaLagLimit: %v)",
                shortestReplica.Address,
                shortestReplicaRowCount,
                longestReplica.Address,
                longestReplicaRowCount,
                ReplicaLagLimit_);
        }
    }
};

TFuture<TChunkQuorumInfo> ComputeQuorumInfo(
    TChunkId chunkId,
    bool overlayed,
    NErasure::ECodec codecId,
    int quorum,
    i64 replicaLagLimit,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration requestTimeout,
    INodeChannelFactoryPtr channelFactory)
{
    return
        New<TComputeQuorumInfoSession>(
            chunkId,
            overlayed,
            codecId,
            quorum,
            replicaLagLimit,
            std::move(replicas),
            requestTimeout,
            std::move(channelFactory))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

i64 GetPhysicalChunkRowCount(i64 logicalRowCount, bool overlayed)
{
    if (logicalRowCount == 0) {
        // NB: Return zero even if the chunk is overlayed as it may be lacking its header record.
        return 0;
    }

    if (overlayed) {
        // NB: Plus one accounts for the header record.
        return logicalRowCount + 1;
    } else {
        return logicalRowCount;
    }
}

i64 GetLogicalChunkRowCount(i64 physicalRowCount, bool overlayed)
{
    if (physicalRowCount == 0) {
        return 0;
    }

    if (overlayed) {
        // Discount header row.
        return physicalRowCount - 1;
    } else {
        return physicalRowCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetJournalRowCount(
    i64 previousJournalRowCount,
    std::optional<i64> lastChunkFirstRowIndex,
    i64 lastChunkRowCount)
{
    return lastChunkFirstRowIndex
        ? *lastChunkFirstRowIndex + lastChunkRowCount
        : previousJournalRowCount + lastChunkRowCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
