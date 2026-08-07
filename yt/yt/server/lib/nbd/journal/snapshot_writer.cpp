#include "snapshot_writer.h"

#include <yt/yt/server/lib/nbd/journal/records/snapshot_block.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/hunks.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/table_upload_options.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NNbd::NJournal {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

const std::vector<std::string>& GetSnapshotTableUploadOptionsAttributeKeys()
{
    static const std::vector<std::string> Result = [] {
        auto result = GetTableUploadOptionsAttributeKeys();
        result.insert(result.end(), {
            "account",
            "primary_medium",
            "replication_factor",
            "vital",
        });
        return result;
    }();
    return Result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TSerializableDeviceParams::Register(TRegistrar registrar)
{
    registrar.Parameter("device_size", &TThis::DeviceSize);
    registrar.Parameter("block_size", &TThis::BlockSize);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSnapshotWriter
    : public ISnapshotWriter
{
public:
    TSnapshotWriter(
        NNative::IClientPtr client,
        TUserObject userObject,
        NLogging::TLogger logger)
        : Client_(std::move(client))
        , UserObject_(std::move(userObject))
        , Logger(std::move(logger))
        , IdMapping_(NRecords::TSnapshotBlockDescriptor::Get()->GetIdMapping())
        , Schema_(NRecords::TSnapshotBlockDescriptor::Get()->GetSchema())
    { }

    TFuture<void> Open() final
    {
        return BIND(&TSnapshotWriter::DoOpen, MakeStrong(this))
            .AsyncVia(Client_->GetConnection()->GetInvoker())
            .Run();
    }

    TFuture<void> WriteBlocks(TRange<TSnapshotBlock> blocks) final
    {
        struct TRowBufferTag
        { };
        auto rowBuffer = New<TRowBuffer>(TRowBufferTag());
        std::vector<TUnversionedRow> rows;
        rows.reserve(blocks.size());
        for (const auto& block : blocks) {
            auto hunkRef = WriteHunkValue(rowBuffer->GetPool(), TGlobalRefHunkValue{
                .ChunkId = block.Ref.ChunkId,
                .ErasureCodec = NErasure::ECodec::None,
                .BlockIndex = block.Ref.RecordIndex,
                .BlockOffset = block.Ref.RecordOffset,
                .Length = block.Ref.PayloadLength,
            });

            TUnversionedRowBuilder builder;
            builder.AddValue(MakeUnversionedInt64Value(block.Index, IdMapping_.BlockIndex));
            builder.AddValue(MakeUnversionedStringValue(hunkRef.ToStringBuf(), IdMapping_.Payload, EValueFlags::Hunk));
            rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
            ReferencedChunkIds_.insert(block.Ref.ChunkId);
        }

        return ChunkWriter_->Write(rows)
            ? OKFuture
            : ChunkWriter_->GetReadyEvent();
    }

    std::vector<TChunkId> GetReferencedChunkIds() const final
    {
        return {ReferencedChunkIds_.begin(), ReferencedChunkIds_.end()};
    }

    TFuture<void> Close() final
    {
        return BIND(&TSnapshotWriter::DoClose, MakeStrong(this))
            .AsyncVia(Client_->GetConnection()->GetInvoker())
            .Run();
    }

private:
    const NNative::IClientPtr Client_;
    const TUserObject UserObject_;
    const NLogging::TLogger Logger;
    const NRecords::TSnapshotBlockDescriptor::TIdMapping IdMapping_;
    const TTableSchemaPtr Schema_;

    //! Accumulated by #WriteBlocks; the set the close attaches. Guarded only by the documented
    //! single-threaded use.
    THashSet<TChunkId> ReferencedChunkIds_;

    TTransactionId TransactionId_;
    NYPath::TYPath Path_;
    NYPath::TYPath ObjectIdPath_;
    TCellTag NativeCellTag_;
    TCellTag ExternalCellTag_;
    TTableUploadOptions TableUploadOptions_;
    TTransactionId UploadTransactionId_;
    ITransactionPtr UploadTransaction_;
    TChunkListId HunkChunkListId_;
    ISchemalessMultiChunkWriterPtr ChunkWriter_;

    void DoOpen()
    {
        YT_VERIFY(UserObject_.TransactionId);
        TransactionId_ = *UserObject_.TransactionId;
        Path_ = UserObject_.GetPath();
        auto objectId = UserObject_.ObjectId;
        NativeCellTag_ = CellTagFromId(objectId);
        ExternalCellTag_ = UserObject_.ExternalCellTag;
        ObjectIdPath_ = FromObjectId(objectId);

        YT_LOG_INFO("Writing snapshot (Path: %v)", Path_);

        IAttributeDictionaryPtr attributes;
        {
            auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower, NativeCellTag_);
            auto req = TCypressYPathProxy::Get(ObjectIdPath_ + "/@");
            AddCellTagToSyncWith(req, objectId);
            ToProto(req->mutable_attributes()->mutable_keys(), GetSnapshotTableUploadOptionsAttributeKeys());
            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            attributes = ConvertToAttributes(TYsonString(rsp->value()));
        }
        TableUploadOptions_ = GetTableUploadOptions(
            NYPath::TRichYPath(Path_),
            *attributes,
            Schema_,
            /*rowCount*/ 0);

        // Begin upload.

        TMasterTableSchemaId chunkSchemaId;
        {
            auto proxy = CreateObjectServiceWriteProxy(Client_, NativeCellTag_);
            auto req = TTableYPathProxy::BeginUpload(ObjectIdPath_);
            req->set_update_mode(ToProto(TableUploadOptions_.UpdateMode));
            req->set_lock_mode(ToProto(TableUploadOptions_.LockMode));
            ToProto(req->mutable_table_schema(), Schema_);
            req->set_schema_mode(ToProto(TableUploadOptions_.SchemaMode));
            req->set_upload_transaction_title(Format("Writing NBD snapshot to %v", Path_));
            if (ExternalCellTag_ != NativeCellTag_) {
                req->add_upload_transaction_secondary_cell_tags(ToProto(ExternalCellTag_));
            }
            req->set_upload_transaction_timeout(
                ToProto(Client_->GetNativeConnection()->GetConfig()->UploadTransactionTimeout));
            SetTransactionId(req, TransactionId_);
            GenerateMutationId(req);

            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            UploadTransactionId_ = FromProto<TTransactionId>(rsp->upload_transaction_id());
            chunkSchemaId = FromProto<TMasterTableSchemaId>(rsp->upload_chunk_schema_id());
        }

        TTransactionAttachOptions attachOptions;
        attachOptions.AutoAbort = true;
        attachOptions.Ping = true;
        attachOptions.PingAncestors = true;
        UploadTransaction_ = Client_->AttachTransaction(UploadTransactionId_, attachOptions);

        // Fetch the main and hunk chunk lists.
        TChunkListId mainChunkListId;

        {
            auto proxy = CreateObjectServiceReadProxy(Client_, EMasterChannelKind::Follower, ExternalCellTag_);
            auto req = TTableYPathProxy::GetUploadParams(ObjectIdPath_);
            req->set_fetch_hunk_chunk_list_id(true);
            SetTransactionId(req, UploadTransactionId_);

            auto rsp = WaitFor(proxy.Execute(req))
                .ValueOrThrow();
            mainChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
            if (!rsp->has_hunk_chunk_list_id()) {
                THROW_ERROR_EXCEPTION("Table %v has no hunk chunk list and cannot hold a journal snapshot",
                    Path_);
            }
            HunkChunkListId_ = FromProto<TChunkListId>(rsp->hunk_chunk_list_id());
        }

        auto writerConfig = New<NTableClient::TTableWriterConfig>();

        auto writerOptions = New<NTableClient::TTableWriterOptions>();
        writerOptions->Account = attributes->Get<std::string>("account");
        writerOptions->MediumName = attributes->Get<std::string>("primary_medium");
        writerOptions->ReplicationFactor = attributes->Get<int>("replication_factor");
        writerOptions->ChunksVital = attributes->Get<bool>("vital");
        writerOptions->CompressionCodec = TableUploadOptions_.CompressionCodec;
        writerOptions->ErasureCodec = TableUploadOptions_.ErasureCodec;
        writerOptions->OptimizeFor = TableUploadOptions_.OptimizeFor;
        writerOptions->ChunkFormat = TableUploadOptions_.ChunkFormat;

        TDataSink dataSink;
        dataSink.SetPath(Path_);
        dataSink.SetObjectId(objectId);
        dataSink.SetAccount(writerOptions->Account);

        ChunkWriter_ = CreateSchemalessMultiChunkWriter(
            writerConfig,
            writerOptions,
            NRecords::TSnapshotBlockDescriptor::Get()->GetNameTable(),
            Schema_,
            /*lastKey*/ TLegacyOwningKey(),
            Client_,
            std::string(NNet::GetLocalHostName()),
            ExternalCellTag_,
            UploadTransactionId_,
            chunkSchemaId,
            dataSink,
            /*writeBlocksOptions*/ {},
            mainChunkListId);
    }

    void DoClose()
    {
        auto hunkChunkIds = GetReferencedChunkIds();

        WaitFor(ChunkWriter_->Close())
            .ThrowOnError();
        auto dataStatistics = ChunkWriter_->GetDataStatistics();

        // Attach the journal (hunk) chunks to the table's hunk chunk list.
        {
            TChunkServiceProxy proxy(Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Leader,
                ExternalCellTag_));
            auto batchReq = proxy.ExecuteBatch();
            GenerateMutationId(batchReq);
            SetTransactionId(batchReq, UploadTransactionId_);
            SetSuppressUpstreamSync(&batchReq->Header(), true);

            auto hunkReq = batchReq->add_attach_chunk_trees_subrequests();
            ToProto(hunkReq->mutable_parent_id(), HunkChunkListId_);
            ToProto(hunkReq->mutable_child_ids(), hunkChunkIds);
            hunkReq->set_request_statistics(true);

            auto batchRspOrError = WaitFor(batchReq->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(
                GetCumulativeError(batchRspOrError),
                "Error attaching hunk chunks to %v",
                Path_);
            const auto& subRsp = batchRspOrError.Value()->attach_chunk_trees_subresponses(0);
            dataStatistics += subRsp.statistics();
        }

        // End upload.
        {
            auto proxy = CreateObjectServiceWriteProxy(Client_, NativeCellTag_);
            auto req = TTableYPathProxy::EndUpload(ObjectIdPath_);
            *req->mutable_statistics() = dataStatistics;
            if (TableUploadOptions_.ChunkFormat) {
                req->set_chunk_format(ToProto(*TableUploadOptions_.ChunkFormat));
            }
            req->set_compression_codec(ToProto(TableUploadOptions_.CompressionCodec));
            req->set_erasure_codec(ToProto(TableUploadOptions_.ErasureCodec));
            req->set_optimize_for(ToProto(TableUploadOptions_.OptimizeFor));
            SetTransactionId(req, UploadTransactionId_);
            GenerateMutationId(req);
            WaitFor(proxy.Execute(req))
                .ThrowOnError();
        }

        UploadTransaction_->Detach();

        YT_LOG_INFO("Snapshot written (Path: %v)", Path_);
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ISnapshotWriterPtr CreateSnapshotWriter(
    NNative::IClientPtr client,
    TUserObject userObject,
    NLogging::TLogger logger)
{
    return New<TSnapshotWriter>(
        std::move(client),
        std::move(userObject),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
