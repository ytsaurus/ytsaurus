#include "helpers.h"
#include "private.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/ytlib/chunk_client/session_id.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/client/misc/workload.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/misc/string.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/library/erasure/codec.h>

namespace NYT::NJournalClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;

using NChunkClient::TSessionId; // Suppress ambiguity with NProto::TSessionId.

////////////////////////////////////////////////////////////////////////////////

void ValidateJournalAttributes(
    NErasure::ECodec erasureCodecId,
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
    if (erasureCodecId == NErasure::ECodec::None) {
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
        // These are "bytewise" codecs: the i-th byte of any parity part depends only on
        // the i-th bytes of data parts. Not all codecs obey this property, e.g. all jerasure-based
        // codecs are unsuitable.
        // BEWARE: Changing this list requires master reign promotion.
        static const std::vector<NErasure::ECodec> BytewiseCodecIds{
            NErasure::ECodec::IsaLrc_12_2_2,
            NErasure::ECodec::IsaReedSolomon_3_3,
            NErasure::ECodec::IsaReedSolomon_6_3
        };
        if (Find(BytewiseCodecIds, erasureCodecId) == BytewiseCodecIds.end()) {
            THROW_ERROR_EXCEPTION("%Qlv codec is not suitable for erasure journals",
                erasureCodecId);
        }
        auto* codec = NErasure::GetCodec(erasureCodecId);
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

#pragma pack(push, 1)

struct TErasureRowHeader
{
    i8 PaddingSize;
};

#pragma pack(pop)

i64 GetPaddingSize(i64 size, i64 alignment)
{
    auto remainder = size % alignment;
    return remainder == 0 ? 0 : alignment - remainder;
}

i64 AlignSize(i64 size, i64 alignment)
{
    return size + GetPaddingSize(size, alignment);
}

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
    NErasure::ECodec codecId,
    const std::vector<TSharedRef>& rows)
{
    auto* codec = NErasure::GetCodec(codecId);
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

std::vector<TSharedRef> DecodeErasureJournalRows(
    NErasure::ECodec codecId,
    const std::vector<std::vector<TSharedRef>>& encodedRowLists)
{
    auto* codec = NErasure::GetCodec(codecId);
    int dataPartCount = codec->GetDataPartCount();

    YT_VERIFY(dataPartCount == encodedRowLists.size());

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
    NErasure::ECodec codecId,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<std::vector<TSharedRef>>& repairRowLists)
{
    i64 rowCount = Max<i64>();
    for (const auto& repairRows : repairRowLists) {
        rowCount = std::min(rowCount, static_cast<i64>(repairRows.size()));
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
            YT_ASSERT(repairRow.Size() == repairRowLists[0][rowIndex].size());
            buffer.Write(repairRow.Begin(), repairRow.Size());
        }
        repairParts.push_back(buffer.SliceFrom(partBegin));
    }
    YT_VERIFY(buffer.IsFull());

    auto* codec = NErasure::GetCodec(codecId);
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
        TDuration timeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : ChunkId_(chunkId)
        , Replicas_(std::move(replicas))
        , Timeout_(timeout)
        , Quorum_(quorum)
        , ChannelFactory_(std::move(channelFactory))
        , Logger(NLogging::TLogger(JournalClientLogger)
            .AddTag("ChunkId: %v", ChunkId_))
    { }

protected:
    const TChunkId ChunkId_;
    const std::vector<TChunkReplicaDescriptor> Replicas_;
    const TDuration Timeout_;
    const int Quorum_;
    const INodeChannelFactoryPtr ChannelFactory_;

    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

class TAbortSessionsQuorumSession
    : public TQuorumSessionBase
{
public:
    TAbortSessionsQuorumSession(
        TChunkId chunkId,
        std::vector<TChunkReplicaDescriptor> replicas,
        TDuration timeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : TQuorumSessionBase(
            chunkId,
            std::move(replicas),
            timeout,
            quorum,
            std::move(channelFactory))
    { }

    TFuture<void> Run()
    {
        BIND(&TAbortSessionsQuorumSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    int SuccessCounter_ = 0;
    int ResponseCounter_ = 0;
    NErasure::TPartIndexSet SuccessPartIndexes_;

    std::vector<TError> InnerErrors_;

    const TPromise<void> Promise_ = NewPromise<void>();

    void DoRun()
    {
        YT_LOG_DEBUG("Aborting journal chunk session quorum (Replicas: %v, Quorum: %v)",
            Replicas_,
            Quorum_);

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

            auto chunkIdWithIndex = TChunkIdWithIndex(ChunkId_, replica.ReplicaIndex);
            auto sessionId = TSessionId(EncodeChunkId(chunkIdWithIndex), replica.MediumIndex);

            auto req = proxy.FinishChunk();
            ToProto(req->mutable_session_id(), sessionId);

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
            if (replica.ReplicaIndex != GenericChunkReplicaIndex) {
                SuccessPartIndexes_.set(replica.ReplicaIndex);
            }
    
            YT_LOG_DEBUG("Journal chunk session aborted successfully (Replica: %v)",
                replica);
        } else {
            InnerErrors_.push_back(rspOrError);
            YT_LOG_WARNING(rspOrError, "Failed to abort journal chunk session (Replica: %v)",
                replica);
        }

        if (IsSuccess()) {
            if (Promise_.TrySet()) {
                YT_LOG_DEBUG("Journal chunk session quorum aborted successfully");
            }
        } else if (ResponseCounter_ == Replicas_.size()) {
            auto combinedError = TError("Unable to abort sessions quorum for journal chunk %v",
                ChunkId_)
                << InnerErrors_;
            Promise_.TrySet(combinedError);
        }
    }

    bool IsSuccess()
    {
        return IsErasureChunkId(ChunkId_)
            ? SuccessPartIndexes_.count() >= Quorum_
            : SuccessCounter_ >= Quorum_;
    }
};

TFuture<void> AbortSessionsQuorum(
    TChunkId chunkId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration timeout,
    int quorum,
    INodeChannelFactoryPtr channelFactory)
{
    return
        New<TAbortSessionsQuorumSession>(
            chunkId,
            std::move(replicas),
            timeout,
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
        NErasure::ECodec codecId,
        std::vector<TChunkReplicaDescriptor> replicas,
        TDuration timeout,
        int quorum,
        INodeChannelFactoryPtr channelFactory)
        : TQuorumSessionBase(
            chunkId,
            std::move(replicas),
            timeout,
            quorum,
            std::move(channelFactory))
        , CodecId_(codecId)
    { }

    TFuture<TMiscExt> Run()
    {
        BIND(&TComputeQuorumInfoSession::DoRun, MakeStrong(this))
            .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker())
            .Run();
        return Promise_;
    }

private:
    const NErasure::ECodec CodecId_;

    struct TResult
    {
        TString Address;
        TMiscExt MiscExt;
        TLocationUuid LocationUuid;
    };
    std::vector<TResult> Results_;
    std::vector<TError> InnerErrors_;

    const TPromise<TMiscExt> Promise_ = NewPromise<TMiscExt>();


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

        YT_LOG_DEBUG("Computing quorum info for journal chunk (Replicas: %v, Quorum: %v, Codec: %v)",
            Replicas_,
            Quorum_,
            CodecId_);

        std::vector<TFuture<void>> futures;
        for (const auto& replica : Replicas_) {
            auto channel = ChannelFactory_->CreateChannel(replica.NodeDescriptor);
            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Timeout_);

            auto chunkIdWithIndex = TChunkIdWithIndex(ChunkId_, replica.ReplicaIndex);
            auto partChunkId = EncodeChunkId(chunkIdWithIndex);

            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), partChunkId);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            ToProto(req->mutable_workload_descriptor(), TWorkloadDescriptor(EWorkloadCategory::SystemTabletRecovery));
            
            futures.push_back(req->Invoke().Apply(
                BIND(&TComputeQuorumInfoSession::OnResponse, MakeStrong(this), replica)
                    .AsyncVia(GetCurrentInvoker())));
        }

        AllSucceeded(futures).Subscribe(
            BIND(&TComputeQuorumInfoSession::OnComplete, MakeStrong(this))
                .Via(GetCurrentInvoker()));
    }

    void OnResponse(
        const TChunkReplicaDescriptor& replica,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError)
    {
        if (rspOrError.IsOK()) {
            const auto& rsp = rspOrError.Value();
            const auto& address = replica.NodeDescriptor.GetDefaultAddress();
            auto miscExt = GetProtoExtension<TMiscExt>(rsp->chunk_meta().extensions());
            auto locationUuid = FromProto<TLocationUuid>(rsp->location_uuid());

            Results_.push_back({
                address,
                miscExt,
                locationUuid,
            });

            YT_LOG_DEBUG("Received info for journal chunk (Replica: %v, LocationUuid: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
                replica,
                locationUuid,
                miscExt.row_count(),
                miscExt.uncompressed_data_size(),
                miscExt.compressed_data_size());
        } else {
            InnerErrors_.push_back(rspOrError);

            YT_LOG_WARNING(rspOrError, "Failed to get journal info (Replica: %v)",
                replica);
        }
    }

    void OnComplete(const TError& /*error*/)
    {
        THashMap<TLocationUuid, TString> locationUuidToAddress;
        for (const auto& result : Results_) {
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

        if (Results_.size() < Quorum_) {
            Promise_.Set(TError("Unable to compute quorum info for journal chunk %v: too few replicas alive, %v found, %v needed",
                ChunkId_,
                Results_.size(),
                Quorum_)
                << InnerErrors_);
            return;
        }

        const auto& quorumResult = GetQuorumResult();
        const auto& miscExt = quorumResult.MiscExt;

        YT_LOG_DEBUG("Quorum info for journal chunk computed successfully (RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            miscExt.row_count(),
            miscExt.uncompressed_data_size(),
            miscExt.compressed_data_size());

        Promise_.Set(miscExt);
    }

    const TResult& GetQuorumResult()
    {
        return IsErasureChunkId(ChunkId_)
            ? GetErasureQuorumResult()
            : GetRegularQuorumResult();
    }

    const TResult& GetErasureQuorumResult()
    {
        std::sort(
            Results_.begin(),
            Results_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.MiscExt.row_count() > rhs.MiscExt.row_count();
            });
        auto* codec = NErasure::GetCodec(CodecId_);
        return Results_[codec->GetGuaranteedRepairablePartCount() - 1];
    }

    const TResult& GetRegularQuorumResult()
    {
        std::sort(
            Results_.begin(),
            Results_.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.MiscExt.row_count() < rhs.MiscExt.row_count();
            });
        return Results_[Quorum_ - 1];
    }
};

TFuture<TMiscExt> ComputeQuorumInfo(
    TChunkId chunkId,
    NErasure::ECodec codecId,
    std::vector<TChunkReplicaDescriptor> replicas,
    TDuration timeout,
    int quorum,
    INodeChannelFactoryPtr channelFactory)
{
    return
        New<TComputeQuorumInfoSession>(
            chunkId,
            codecId,
            std::move(replicas),
            timeout,
            quorum,
            std::move(channelFactory))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient

