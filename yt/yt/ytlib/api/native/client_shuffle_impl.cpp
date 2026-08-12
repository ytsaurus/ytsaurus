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

TFuture<TShuffleServiceProxy::TRspFetchChunksPtr> FetchShuffleChunks(
    IChannelPtr channel,
    const TShuffleHandlePtr& shuffleHandle,
    int partitionIndex,
    std::optional<IShuffleClient::TIndexRange> logicalWriterIndexRange,
    TDuration rpcTimeout)
{
    TShuffleServiceProxy proxy(std::move(channel));
    auto req = proxy.FetchChunks();
    req->SetTimeout(rpcTimeout);
    req->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));
    req->set_partition_index(partitionIndex);
    if (logicalWriterIndexRange) {
        auto* range = req->mutable_logical_writer_index_range();
        range->set_begin(logicalWriterIndexRange->first);
        range->set_end(logicalWriterIndexRange->second);
    }
    return req->Invoke();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RegisterShuffleChunks(
    const IConnectionPtr& connection,
    const TShuffleHandlePtr& shuffleHandle,
    const std::vector<TChunkSpec>& chunkSpecs,
    std::optional<int> logicalWriterIndex,
    bool overwriteExistingWriterData)
{
    auto channel = connection->CreateChannelByAddress(shuffleHandle->CoordinatorAddress);
    TShuffleServiceProxy proxy(std::move(channel));
    auto req = proxy.RegisterChunks();
    req->SetTimeout(connection->GetConfig()->DefaultShuffleServiceTimeout);
    req->set_shuffle_handle(ToProto(ConvertToYsonString(shuffleHandle)));
    ToProto(req->mutable_chunk_specs(), chunkSpecs);
    if (logicalWriterIndex) {
        req->set_logical_writer_index(*logicalWriterIndex);
    }
    req->set_overwrite_existing_writer_data(overwriteExistingWriterData);
    return req->Invoke().AsVoid();
}

////////////////////////////////////////////////////////////////////////////////

//! Writer-side write-session provider that resolves sessions through the shuffle
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
        IConnectionPtr connection,
        TShuffleHandlePtr shuffleHandle,
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData)
        : Writer_(std::move(writer))
        , Connection_(std::move(connection))
        , ShuffleHandle_(std::move(shuffleHandle))
        , LogicalWriterIndex_(logicalWriterIndex)
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
            return RegisterShuffleChunks(
                Connection_,
                ShuffleHandle_,
                Writer_->GetWrittenChunkSpecs(),
                LogicalWriterIndex_,
                OverwriteExistingWriterData_);
        }));
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Writer_->GetNameTable();
    }

private:
    const ISchemalessMultiChunkWriterPtr Writer_;
    const IConnectionPtr Connection_;
    const TShuffleHandlePtr ShuffleHandle_;
    const std::optional<int> LogicalWriterIndex_;
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
    std::optional<int> logicalWriterIndex,
    const TShuffleWriterOptions& options)
{
    THROW_ERROR_EXCEPTION_IF(!handle->Schema, "Push-based shuffle handle is missing a schema");

    auto connection = client->GetNativeConnection();
    auto rpcTimeout = connection->GetConfig()->DefaultShuffleServiceTimeout;
    auto channel = BuildShuffleServiceChannel(connection, handle->CoordinatorAddress);

    TShuffleServiceProxy proxy(channel);
    // COMPAT(apollo1321): Switch to RegisterWriter and remove RegisterMapper
    // after the 26.2 branch is created.
    auto registerReq = proxy.RegisterMapper();
    registerReq->SetTimeout(rpcTimeout);
    registerReq->set_shuffle_handle(ToProto(ConvertToYsonString(handle)));
    if (logicalWriterIndex) {
        registerReq->set_logical_writer_index(*logicalWriterIndex);
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
            i32 writerId = rsp->writer_id();

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
                writerId,
                client->GetConnection()->GetInvoker(),
                std::move(seededSessions));

            return New<TPushBasedShuffleWriterAdapter>(
                std::move(pushBasedWriter),
                std::move(nameTable),
                schema);
        }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchWriterPtr> CreatePullBasedShuffleWriterImpl(
    const TClientPtr& client,
    TShuffleHandlePtr handle,
    const std::string& partitionColumn,
    std::optional<int> logicalWriterIndex,
    const TShuffleWriterOptions& options)
{
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
        handle->PartitionCount,
        nameTable->GetId(partitionColumn));

    // TODO(apollo1321): Carry the writer/reader config on the shuffle handle (set once at
    // start_shuffle, shared by all writers and readers) for both push and pull, and drop the
    // per-call options.Config — push already ignores it; pull still consumes it per call.
    auto tableWriterOptions = New<TTableWriterOptions>();
    tableWriterOptions->EvaluateComputedColumns = false;
    tableWriterOptions->Account = handle->Account;
    tableWriterOptions->ReplicationFactor = handle->ReplicationFactor;
    tableWriterOptions->MediumName = handle->Medium;

    auto writer = CreatePartitionMultiChunkWriter(
        options.Config,
        std::move(tableWriterOptions),
        std::move(nameTable),
        std::move(schema),
        client,
        /*localHostName*/ "",
        CellTagFromId(handle->TransactionId),
        handle->TransactionId,
        NullTableSchemaId,
        NullChunkListId,
        std::move(partitioner),
        /*dataSink*/ {},
        /*writeBlocksOptions*/ {});

    return MakeFuture(New<TPullBasedShuffleWriter>(
        std::move(writer),
        client->GetNativeConnection(),
        std::move(handle),
        logicalWriterIndex,
        options.OverwriteExistingWriterData))
        .As<IRowBatchWriterPtr>();
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
            outRows.insert(outRows.end(), record.Rows.begin(), record.Rows.end());
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
    bool Drained_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchReaderPtr> CreatePushBasedShuffleReaderImpl(
    const TClientPtr& client,
    TShuffleHandlePtr handle,
    int partitionIndex,
    std::optional<IShuffleClient::TIndexRange> logicalWriterIndexRange)
{
    THROW_ERROR_EXCEPTION_IF(!handle->Schema, "Push-based shuffle handle is missing a schema");

    auto connection = client->GetNativeConnection();
    auto rpcTimeout = connection->GetConfig()->DefaultShuffleServiceTimeout;
    auto channel = BuildShuffleServiceChannel(connection, handle->CoordinatorAddress);

    return FetchShuffleChunks(
        std::move(channel),
        handle,
        partitionIndex,
        logicalWriterIndexRange,
        rpcTimeout)
        .Apply(BIND_NO_PROPAGATE([
            client,
            handle
        ] (const TShuffleServiceProxy::TRspFetchChunksPtr& rsp) -> IRowBatchReaderPtr {
            auto chunkSpecs = FromProto<std::vector<TChunkSpec>>(rsp->chunk_specs());

            auto validIds = THashSet<i32>(rsp->valid_writer_ids().begin(), rsp->valid_writer_ids().end());
            TRecordHeaderFilter filter = [validIds = std::move(validIds)] (const TRecordHeader& header) {
                return validIds.contains(header.WriterId);
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
            // TODO(apollo1321): Wait for all writers to finish instead of taking a snapshot
            // that can omit subsequently written records (YT-29240).
            partitionReader->FinishAtCurrentCommittedRecordCount();

            // The shuffle schema is the single source of the column name-to-id
            // mapping, so the output name table is derived directly from it and
            // record value ids need no remapping.
            return New<TPushBasedShuffleReader>(
                std::move(partitionReader),
                TNameTable::FromSchema(*handle->Schema));
        }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IRowBatchReaderPtr> CreatePullBasedShuffleReaderImpl(
    const TClientPtr& client,
    TShuffleHandlePtr handle,
    int partitionIndex,
    std::optional<IShuffleClient::TIndexRange> logicalWriterIndexRange,
    const TShuffleReaderOptions& options)
{
    auto connection = client->GetNativeConnection();
    auto channel = connection->CreateChannelByAddress(handle->CoordinatorAddress);
    return FetchShuffleChunks(
        std::move(channel),
        handle,
        partitionIndex,
        logicalWriterIndexRange,
        connection->GetConfig()->DefaultShuffleServiceTimeout)
        .Apply(BIND([
            client,
            options,
            partitionIndex
        ] (const TShuffleServiceProxy::TRspFetchChunksPtr& rsp) {
            auto chunkSpecs = FromProto<std::vector<TChunkSpec>>(rsp->chunk_specs());
            auto dataSourceDirectory = New<TDataSourceDirectory>();
            dataSourceDirectory->DataSources().emplace_back(New<TDataSource>(
                EDataSourceType::UnversionedTable,
                /*path*/ "",
                New<TTableSchema>(),
                /*virtualKeyPrefixLength*/ 0,
                /*columns*/ std::nullopt,
                /*omittedInaccessibleColumns*/ std::vector<std::string>{},
                NullTimestamp,
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
                New<TMultiChunkReaderHost>(New<TChunkReaderHost>(client)),
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

TFuture<IRowBatchReaderPtr> TClient::CreateShuffleReader(
    const TSignedShuffleHandlePtr& signedShuffleHandle,
    int partitionIndex,
    std::optional<TIndexRange> logicalWriterIndexRange,
    const TShuffleReaderOptions& options)
{
    // TODO(pavook): friendly YSON wrapper.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));
    if (shuffleHandle->UsePushBasedShuffle) {
        return CreatePushBasedShuffleReaderImpl(
            MakeStrong(this),
            std::move(shuffleHandle),
            partitionIndex,
            logicalWriterIndexRange);
    }

    return CreatePullBasedShuffleReaderImpl(
        MakeStrong(this),
        std::move(shuffleHandle),
        partitionIndex,
        logicalWriterIndexRange,
        options);
}

TFuture<IRowBatchWriterPtr> TClient::CreateShuffleWriter(
    const TSignedShuffleHandlePtr& signedShuffleHandle,
    const std::string& partitionColumn,
    std::optional<int> logicalWriterIndex,
    const TShuffleWriterOptions& options)
{
    // TODO(pavook): friendly YSON wrapper.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonString(signedShuffleHandle.Underlying()->Payload()));
    if (shuffleHandle->UsePushBasedShuffle) {
        return CreatePushBasedShuffleWriterImpl(
            MakeStrong(this),
            std::move(shuffleHandle),
            partitionColumn,
            logicalWriterIndex,
            options);
    }

    return CreatePullBasedShuffleWriterImpl(
        MakeStrong(this),
        std::move(shuffleHandle),
        partitionColumn,
        logicalWriterIndex,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
