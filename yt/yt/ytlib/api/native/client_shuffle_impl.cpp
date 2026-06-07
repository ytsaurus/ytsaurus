#include "client_impl.h"
#include "config.h"

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/partitioner.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/shuffle_client/shuffle_service_proxy.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/partition_reader.h>
#include <yt/yt/ytlib/push_based_shuffle_client/session_provider.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/helpers.h>

#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/row_batch_writer.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NShuffleClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NPushBasedShuffleClient;
using namespace NRpc;

using NChunkClient::NProto::TChunkSpec;
using NTableClient::TTableReaderOptions;
using NTableClient::TTableWriterOptions;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

IChannelPtr BuildShuffleServiceChannel(
    const NNative::IConnectionPtr& connection,
    const std::string& coordinatorAddress)
{
    auto rawChannel = connection->CreateChannelByAddress(coordinatorAddress);
    // Retries are bounded by RetryAttempts (config default); the per-attempt
    // timeout is set on each request. RetryTimeout (the overall retry budget) is
    // intentionally left unset so it does not collapse onto a single attempt.
    auto retryingChannelConfig = New<TRetryingChannelConfig>();
    return CreateRetryingChannel(std::move(retryingChannelConfig), std::move(rawChannel));
}

////////////////////////////////////////////////////////////////////////////////

//! Mapper-side write-session provider that resolves sessions through the shuffle
//! service's GetPartitionWriteSession RPC. It is shuffle-service-specific, hence
//! it lives here rather than in the use-case-agnostic push_based_shuffle_client.
class TRemotePartitionWriteSessionProvider
    : public IPartitionWriteSessionProvider
{
public:
    TRemotePartitionWriteSessionProvider(
        IChannelPtr channel,
        TShuffleHandlePtr shuffleHandle,
        TDuration rpcTimeout)
        : Channel_(std::move(channel))
        , ShuffleHandle_(std::move(shuffleHandle))
        , RpcTimeout_(rpcTimeout)
    { }

    TFuture<TSessionDescriptor> GetSession(
        int partitionIndex,
        std::optional<TSessionId> excludedSessionId) override
    {
        TShuffleServiceProxy proxy(Channel_);
        auto req = proxy.GetPartitionWriteSession();
        req->SetTimeout(RpcTimeout_);
        req->set_shuffle_handle(ToProto(ConvertToYsonString(ShuffleHandle_)));
        req->set_partition_index(partitionIndex);
        if (excludedSessionId) {
            ToProto(req->mutable_excluded_session_id(), *excludedSessionId);
        }
        return req->Invoke()
            .Apply(BIND_NO_PROPAGATE([] (const TShuffleServiceProxy::TRspGetPartitionWriteSessionPtr& rsp) {
                const auto& session = rsp->session();
                return TSessionDescriptor{
                    .SessionId = FromProto<TSessionId>(session.session_id()),
                    .SequencerNode = FromProto<TNodeDescriptor>(session.sequencer_node()),
                };
            }));
    }

private:
    const IChannelPtr Channel_;
    const TShuffleHandlePtr ShuffleHandle_;
    const TDuration RpcTimeout_;
};

////////////////////////////////////////////////////////////////////////////////

class TPullBasedShuffleWriter
    : public IRowBatchWriter
{
public:
    TPullBasedShuffleWriter(
        ISchemalessMultiChunkWriterPtr writer,
        TClientPtr client,
        TShuffleHandlePtr shuffleHandle,
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData)
        : Writer_(std::move(writer))
        , Client_(std::move(client))
        , ShuffleHandle_(std::move(shuffleHandle))
        , WriterIndex_(writerIndex)
        , OverwriteExistingWriterData_(overwriteExistingWriterData)
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        return Writer_->Write(rows);
    }

    TFuture<void> GetReadyEvent() override
    {
        return Writer_->GetReadyEvent();
    }

    TFuture<void> Close() override
    {
        return Writer_->Close().Apply(BIND([this, this_ = MakeStrong(this)]() {
            return Client_->RegisterShuffleChunks(
                ShuffleHandle_,
                Writer_->GetWrittenChunkSpecs(),
                WriterIndex_,
                /*options*/ {.OverwriteExistingWriterData = OverwriteExistingWriterData_});
        }));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Writer_->GetNameTable();
    }

private:
    const ISchemalessMultiChunkWriterPtr Writer_;
    const TClientPtr Client_;
    const TShuffleHandlePtr ShuffleHandle_;
    const std::optional<int> WriterIndex_;
    const bool OverwriteExistingWriterData_;
};

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleWriterAdapter
    : public IRowBatchWriter
{
public:
    TPushBasedShuffleWriterAdapter(
        IPushBasedShuffleWriterPtr writer,
        TNameTablePtr nameTable,
        TTableSchemaPtr schema)
        : Writer_(std::move(writer))
        , NameTable_(std::move(nameTable))
        , Schema_(std::move(schema))
    { }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        try {
            ValidateRows(rows);
        } catch (const std::exception& ex) {
            WriteFuture_ = MakeFuture(TError(ex));
            return false;
        }
        WriteFuture_ = Writer_->Write(rows);
        auto result = WriteFuture_.TryGet();
        return result && result->IsOK();
    }

    TFuture<void> GetReadyEvent() override
    {
        return WriteFuture_ ? WriteFuture_ : OKFuture;
    }

    TFuture<void> Close() override
    {
        return Writer_->Close();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const IPushBasedShuffleWriterPtr Writer_;
    const TNameTablePtr NameTable_;
    const TTableSchemaPtr Schema_;
    TFuture<void> WriteFuture_;

    void ValidateRows(TRange<TUnversionedRow> rows)
    {
        // The name table is derived from the schema, so a value id equals its
        // schema column index. A column outside the schema (a parser may intern
        // one) gets an id beyond the schema columns and is rejected; values are
        // also type-checked against the schema.
        int columnCount = std::ssize(Schema_->Columns());
        for (auto row : rows) {
            for (const auto& value : row) {
                if (value.Id >= columnCount) {
                    THROW_ERROR_EXCEPTION("Unexpected column %Qv", NameTable_->GetNameOrThrow(value.Id));
                }
                ValidateValueType(value, *Schema_, value.Id, /*typeAnyAcceptsAllValues*/ false);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchWriterPtr> CreatePushBasedShuffleWriterImpl(
    const TClientPtr& client,
    TShuffleHandlePtr handle,
    const std::string& partitionColumn,
    std::optional<int> writerIndex,
    const TShuffleWriterOptions& options)
{
    THROW_ERROR_EXCEPTION_IF(!handle->Schema, "Push-based shuffle handle is missing a schema");

    auto connection = client->GetNativeConnection();
    auto rpcTimeout = connection->GetConfig()->DefaultShuffleServiceTimeout;
    auto channel = BuildShuffleServiceChannel(connection, handle->CoordinatorAddress);

    TShuffleServiceProxy proxy(channel);
    auto registerReq = proxy.RegisterMapper();
    registerReq->SetTimeout(rpcTimeout);
    registerReq->set_shuffle_handle(ToProto(ConvertToYsonString(handle)));
    if (writerIndex) {
        registerReq->set_writer_index(*writerIndex);
    }
    registerReq->set_overwrite_existing_writer_data(options.OverwriteExistingWriterData);

    return registerReq->Invoke()
        .Apply(BIND_NO_PROPAGATE([
            client,
            handle,
            partitionColumn,
            channel,
            rpcTimeout
        ] (const TShuffleServiceProxy::TRspRegisterMapperPtr& rsp) -> IRowBatchWriterPtr {
            i32 mapperId = rsp->mapper_id();

            THashMap<int, TSessionDescriptor> seededSessions;
            seededSessions.reserve(rsp->ready_sessions_size());
            for (const auto& readySession : rsp->ready_sessions()) {
                const auto& session = readySession.session();
                seededSessions.emplace(
                    readySession.partition_index(),
                    TSessionDescriptor{
                        .SessionId = FromProto<TSessionId>(session.session_id()),
                        .SequencerNode = FromProto<TNodeDescriptor>(session.sequencer_node()),
                    });
            }

            // The shuffle schema is the single source of the column name-to-id
            // mapping. The name table is derived from it, so value ids equal
            // schema column indices for both writer and reader.
            const auto& schema = handle->Schema;
            auto nameTable = TNameTable::FromSchema(*schema);
            auto partitioner = CreateColumnBasedPartitioner(
                handle->PartitionCount,
                nameTable->GetIdOrThrow(partitionColumn));

            auto sessionProvider = New<TRemotePartitionWriteSessionProvider>(channel, handle, rpcTimeout);

            auto pushConfig = handle->PushConfig
                ? ConvertTo<TPushShuffleConfigPtr>(*handle->PushConfig)->WriterConfig
                : New<TShuffleWriterConfig>();

            auto pushBasedWriter = CreatePushBasedShuffleWriter(
                pushConfig,
                sessionProvider,
                partitioner,
                client->GetNativeConnection(),
                mapperId,
                client->GetConnection()->GetInvoker(),
                std::move(seededSessions));

            return New<TPushBasedShuffleWriterAdapter>(
                std::move(pushBasedWriter),
                std::move(nameTable),
                schema);
        }));
}

////////////////////////////////////////////////////////////////////////////////

class TPullBasedShuffleReader
    : public IRowBatchReader
{
public:
    explicit TPullBasedShuffleReader(ISchemalessMultiChunkReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Reader_->Read(options);
    }

    TFuture<void> GetReadyEvent() const override
    {
        return Reader_->GetReadyEvent();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Reader_->GetNameTable();
    }

private:
    const ISchemalessMultiChunkReaderPtr Reader_;
};

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleReader
    : public IRowBatchReader
{
public:
    TPushBasedShuffleReader(
        IPushBasedPartitionReaderPtr reader,
        TNameTablePtr nameTable)
        : Reader_(std::move(reader))
        , NameTable_(std::move(nameTable))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        if (Drained_) {
            return nullptr;
        }
        if (!PendingBatch_) {
            PendingBatch_ = Reader_->Read();
        }
        if (!PendingBatch_.IsSet()) {
            return CreateEmptyUnversionedRowBatch();
        }

        auto batch = PendingBatch_.GetOrCrash()
            .ValueOrThrow();
        PendingBatch_.Reset();
        bool finished = batch->Finished;

        // Value ids are schema column indices for every writer (the name table
        // is derived from the shuffle schema), so rows need no remapping.
        i64 batchRowCount = 0;
        for (const auto& record : batch->Records) {
            batchRowCount += std::ssize(record.Rows);
        }
        std::vector<TUnversionedRow> outRows;
        outRows.reserve(batchRowCount);

        for (const auto& record : batch->Records) {
            // A lost-ACK resend duplicates a whole record; dedup by (mapper_id, start_row).
            // Duplicates are rare, hence [[likely]].
            if (SeenRecords_.emplace(record.Header.MapperId, record.Header.StartRow).second) [[likely]] {
                outRows.insert(outRows.end(), record.Rows.begin(), record.Rows.end());
            }
        }

        if (finished && outRows.empty()) {
            Drained_ = true;
            return nullptr;
        }
        Drained_ = finished;
        return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(outRows), std::move(batch)));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return PendingBatch_ ? PendingBatch_.template As<void>() : OKFuture;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const IPushBasedPartitionReaderPtr Reader_;
    const TNameTablePtr NameTable_;
    TFuture<TShuffleReadBatchPtr> PendingBatch_;
    // Records already emitted, keyed by (mapper_id, start_row). Retained for the reader's
    // lifetime; bounded by the record count in the partition.
    THashSet<std::pair<i32, i64>> SeenRecords_;
    bool Drained_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchReaderPtr> CreatePushBasedShuffleReaderImpl(
    const TClientPtr& client,
    TShuffleHandlePtr handle,
    int partitionIndex,
    std::optional<IShuffleClient::TIndexRange> writerIndexRange)
{
    THROW_ERROR_EXCEPTION_IF(!handle->Schema, "Push-based shuffle handle is missing a schema");

    auto connection = client->GetNativeConnection();
    auto rpcTimeout = connection->GetConfig()->DefaultShuffleServiceTimeout;
    auto channel = BuildShuffleServiceChannel(connection, handle->CoordinatorAddress);

    TShuffleServiceProxy proxy(channel);
    auto fetchReq = proxy.FetchChunks();
    fetchReq->SetTimeout(rpcTimeout);
    fetchReq->set_shuffle_handle(ToProto(ConvertToYsonString(handle)));
    fetchReq->set_partition_index(partitionIndex);
    if (writerIndexRange) {
        auto* range = fetchReq->mutable_writer_index_range();
        range->set_begin(writerIndexRange->first);
        range->set_end(writerIndexRange->second);
    }

    return fetchReq->Invoke()
        .Apply(BIND_NO_PROPAGATE([
            client,
            handle
        ] (const TShuffleServiceProxy::TRspFetchChunksPtr& rsp) -> IRowBatchReaderPtr {
            auto chunkSpecs = FromProto<std::vector<TChunkSpec>>(rsp->chunk_specs());

            auto validIds = THashSet<i32>(rsp->valid_mapper_ids().begin(), rsp->valid_mapper_ids().end());
            TRecordHeaderFilter filter = [validIds = std::move(validIds)] (const TRecordHeader& header) {
                return validIds.contains(header.MapperId);
            };

            auto readerConfig = handle->PushConfig
                ? ConvertTo<TPushShuffleConfigPtr>(*handle->PushConfig)->ReaderConfig
                : New<TPartitionReaderConfig>();

            // The reader must use the same read quorum the controller created the
            // journal chunks with; both derive it from the replication factor.
            int readQuorum = ComputeDefaultJournalQuorums(
                handle->ReplicationFactor).ReadQuorum;

            auto partitionReader = CreatePushBasedPartitionReader(
                readerConfig,
                client,
                New<TChunkReaderHost>(client),
                readQuorum,
                client->GetConnection()->GetInvoker(),
                std::move(filter));

            for (const auto& chunkSpec : chunkSpecs) {
                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                auto replicas = FromProto<TChunkReplicaWithMediumList>(chunkSpec.replicas());
                partitionReader->AddChunk(chunkId, replicas, /*startRecordIndex*/ 0, /*rangeEndRecordIndex*/ {});
            }
            partitionReader->SetNoMoreChunks();

            // The shuffle schema is the single source of the column name-to-id
            // mapping, so the output name table is derived directly from it and
            // record value ids need no remapping.
            return New<TPushBasedShuffleReader>(
                std::move(partitionReader),
                TNameTable::FromSchema(*handle->Schema));
        }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSignedShuffleHandlePtr TClient::DoStartShuffle(
    const std::string& account,
    int partitionCount,
    TTransactionId parentTransactionId,
    const TStartShuffleOptions& options)
{
    auto channel = GetNativeConnection()->GetShuffleServiceChannelOrThrow();
    TShuffleServiceProxy shuffleProxy(std::move(channel));

    auto req = shuffleProxy.StartShuffle();
    req->SetTimeout(options.Timeout.value_or(GetNativeConnection()->GetConfig()->DefaultShuffleServiceTimeout));

    req->set_account(account);
    req->set_partition_count(partitionCount);
    ToProto(req->mutable_parent_transaction_id(), parentTransactionId);
    if (options.Medium) {
        req->set_medium(*options.Medium);
    }
    if (options.ReplicationFactor) {
        req->set_replication_factor(*options.ReplicationFactor);
    }
    if (options.UsePushBasedShuffle) {
        req->set_use_push_based_shuffle(true);
    }
    if (options.Schema) {
        ToProto(req->mutable_schema(), options.Schema);
    }
    if (options.PushConfig) {
        req->set_push_config(ToProto(*options.PushConfig));
    }

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    const auto& signatureGenerator = GetNativeConnection()->GetSignatureGenerator();
    return TSignedShuffleHandlePtr(signatureGenerator->Sign(rsp->shuffle_handle()));
}

void TClient::DoRegisterShuffleChunks(
    const TShuffleHandlePtr& shuffleHandle,
    const std::vector<TChunkSpec>& chunkSpecs,
    std::optional<int> writerIndex,
    const TRegisterShuffleChunksOptions& options)
{
    auto shuffleConnection = GetNativeConnection()->CreateChannelByAddress(shuffleHandle->CoordinatorAddress);

    TShuffleServiceProxy shuffleProxy(shuffleConnection);

    auto req = shuffleProxy.RegisterChunks();
    req->SetTimeout(options.Timeout.value_or(GetNativeConnection()->GetConfig()->DefaultShuffleServiceTimeout));

    req->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));
    ToProto(req->mutable_chunk_specs(), chunkSpecs);
    if (writerIndex) {
        req->set_writer_index(*writerIndex);
    }
    req->set_overwrite_existing_writer_data(options.OverwriteExistingWriterData);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

std::vector<TChunkSpec> TClient::DoFetchShuffleChunks(
    const TShuffleHandlePtr& shuffleHandle,
    int partitionIndex,
    std::optional<std::pair<int, int>> writerIndexRange,
    const TFetchShuffleChunksOptions& options)
{
    auto shuffleConnection = GetNativeConnection()->CreateChannelByAddress(shuffleHandle->CoordinatorAddress);

    TShuffleServiceProxy shuffleProxy(shuffleConnection);

    auto req = shuffleProxy.FetchChunks();
    req->SetTimeout(options.Timeout.value_or(GetNativeConnection()->GetConfig()->DefaultShuffleServiceTimeout));

    req->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));
    req->set_partition_index(partitionIndex);
    if (writerIndexRange) {
        auto* writerIndexRangeProto = req->mutable_writer_index_range();
        writerIndexRangeProto->set_begin(writerIndexRange->first);
        writerIndexRangeProto->set_end(writerIndexRange->second);
    }

    auto rsp = WaitFor(req->Invoke())
        .ValueOrThrow();

    return FromProto<std::vector<TChunkSpec>>(rsp->chunk_specs());
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchReaderPtr> TClient::CreateShuffleReader(
    const TSignedShuffleHandlePtr& signedShuffleHandle,
    int partitionIndex,
    std::optional<TIndexRange> writerIndexRange,
    const TShuffleReaderOptions& options)
{
    // TODO(pavook): friendly YSON wrapper.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));
    if (shuffleHandle->UsePushBasedShuffle) {
        return CreatePushBasedShuffleReaderImpl(
            MakeStrong(this),
            std::move(shuffleHandle),
            partitionIndex,
            writerIndexRange);
    }
    return FetchShuffleChunks(
        shuffleHandle,
        partitionIndex,
        writerIndexRange,
        TFetchShuffleChunksOptions{})
        .AsUnique().Apply(BIND([=, this, this_ = MakeStrong(this)] (std::vector<TChunkSpec>&& chunkSpecs) mutable {
            auto dataSourceDirectory = New<TDataSourceDirectory>();
            dataSourceDirectory->DataSources().emplace_back(New<TDataSource>(
                EDataSourceType::UnversionedTable,
                /*path*/ "",
                New<TTableSchema>(),
                /*virtualKeyPrefixLength*/ 0,
                /*columns*/ std::nullopt,
                /*omittedInaccessibleColumns*/ std::vector<std::string>{},
                /*timestamp*/ NullTimestamp,
                /*retentionTimestamp*/ NullTimestamp,
                /*columnRenameDescriptors*/ TColumnRenameDescriptors{}));

            std::vector<TDataSliceDescriptor> dataSlices;
            dataSlices.reserve(chunkSpecs.size());
            for (auto& chunk : chunkSpecs) {
                dataSlices.emplace_back(std::move(chunk));
            }

            auto reader = CreateSchemalessSequentialMultiReader(
                options.Config,
                New<TTableReaderOptions>(),
                New<TMultiChunkReaderHost>(New<TChunkReaderHost>(this)),
                dataSourceDirectory,
                dataSlices,
                /*hintKeyPrefixes*/ std::nullopt,
                New<TNameTable>(),
                TClientChunkReadOptions(),
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                /*columnFilter*/ {},
                TPartitionTags{partitionIndex});

            return New<TPullBasedShuffleReader>(std::move(reader));
        }))
        .As<IRowBatchReaderPtr>();
}

TFuture<IRowBatchWriterPtr> TClient::CreateShuffleWriter(
    const TSignedShuffleHandlePtr& signedShuffleHandle,
    const std::string& partitionColumn,
    std::optional<int> writerIndex,
    const TShuffleWriterOptions& options)
{
    // TODO(pavook): friendly YSON wrapper.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(signedShuffleHandle.Underlying()->Payload()));
    if (shuffleHandle->UsePushBasedShuffle) {
        return CreatePushBasedShuffleWriterImpl(
            MakeStrong(this),
            std::move(shuffleHandle),
            partitionColumn,
            writerIndex,
            options);
    }

    // The partition column index must be preserved for the partitioner.
    // However, the row is partitioned after the row value ids are mapped to
    // the chunk name table. As a result, the partition column id may differ
    // from the one specified in the partitioner. To prevent this issue, it is
    // necessary to specify the table schema with the partition column, as it
    // guaranteed that the chunk name table always coincides with the column
    // index in the schema (because the chunk name table is initialized from the
    // schema columns).
    // TODO(apollo1321): Carry a schema on the shuffle handle for pull-based too
    // and use it directly instead of synthesizing this single-column schema.
    auto schema = New<TTableSchema>(
        std::vector{TColumnSchema(partitionColumn, ESimpleLogicalValueType::Int64)},
        /*strict*/ false);
    auto nameTable = TNameTable::FromSchema(*schema);

    auto partitioner = CreateColumnBasedPartitioner(
        shuffleHandle->PartitionCount,
        nameTable->GetId(partitionColumn));

    // TODO(apollo1321): Carry the writer/reader config on the shuffle handle (set once at
    // start_shuffle, shared by all writers and readers) for both push and pull, and drop the
    // per-call options.Config — push already ignores it; pull still consumes it per call.
    auto tableWriterOptions = New<TTableWriterOptions>();
    tableWriterOptions->EvaluateComputedColumns = false;
    tableWriterOptions->Account = shuffleHandle->Account;
    tableWriterOptions->ReplicationFactor = shuffleHandle->ReplicationFactor;
    tableWriterOptions->MediumName = shuffleHandle->Medium;

    auto writer = CreatePartitionMultiChunkWriter(
        options.Config,
        std::move(tableWriterOptions),
        std::move(nameTable),
        std::move(schema),
        this,
        /*localHostName*/ "",
        CellTagFromId(shuffleHandle->TransactionId),
        shuffleHandle->TransactionId,
        NullTableSchemaId,
        NullChunkListId,
        std::move(partitioner),
        /*dataSink*/ {},
        /*writeBlocksOptions*/ {});

    return MakeFuture(New<TPullBasedShuffleWriter>(
        std::move(writer),
        this,
        std::move(shuffleHandle),
        writerIndex,
        options.OverwriteExistingWriterData))
        .As<IRowBatchWriterPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
