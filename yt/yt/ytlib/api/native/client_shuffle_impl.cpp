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

#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/row_batch_writer.h>

#include <yt/yt/client/signature/generator.h>
#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NShuffleClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::NProto::TChunkSpec;
using NTableClient::TTableReaderOptions;
using NTableClient::TTableWriterOptions;

////////////////////////////////////////////////////////////////////////////////

class TShuffleWriter
    : public IRowBatchWriter
{
public:
    TShuffleWriter(
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

class TShuffleReader
    : public IRowBatchReader
{
public:
    explicit TShuffleReader(ISchemalessMultiChunkReaderPtr reader)
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
    std::optional<std::pair<int, int>> writerIndexRange,
    const TShuffleReaderOptions& options)
{
    // TODO(pavook): friendly YSON wrapper.
    auto shuffleHandle = ConvertTo<TShuffleHandlePtr>(TYsonStringBuf(signedShuffleHandle.Underlying()->Payload()));
    return FetchShuffleChunks(
        shuffleHandle,
        partitionIndex,
        writerIndexRange,
        TFetchShuffleChunksOptions{})
        .ApplyUnique(BIND([=, this, this_ = MakeStrong(this)] (std::vector<TChunkSpec>&& chunkSpecs) mutable {
            auto dataSourceDirectory = New<TDataSourceDirectory>();
            dataSourceDirectory->DataSources().emplace_back(TDataSource(
                EDataSourceType::UnversionedTable,
                /*path*/ "",
                New<TTableSchema>(),
                /*virtualKeyPrefixLength*/ 0,
                /*columns*/ std::nullopt,
                /*omittedInaccessibleColumns*/ {},
                /*timestamp*/ NullTimestamp,
                /*retentionTimestamp*/ NullTimestamp,
                /*columnRenameDescriptors*/ {}));

            std::vector<TDataSliceDescriptor> dataSlices;
            dataSlices.reserve(chunkSpecs.size());
            for (auto& chunk : chunkSpecs) {
                dataSlices.emplace_back(std::move(chunk));
            }

            auto reader = CreateSchemalessSequentialMultiReader(
                options.Config,
                New<TTableReaderOptions>(),
                CreateSingleSourceMultiChunkReaderHost(TChunkReaderHost::FromClient(this)),
                dataSourceDirectory,
                dataSlices,
                /*hintKeyPrefixes*/ std::nullopt,
                New<TNameTable>(),
                TClientChunkReadOptions(),
                TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
                /*columnFilter*/ {},
                partitionIndex);

            return New<TShuffleReader>(std::move(reader));
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

    // The partition column index must be preserved for the partitioner.
    // However, the row is partitioned after the row value ids are mapped to
    // the chunk name table. As a result, the partition column id may differ
    // from the one specified in the partitioner. To prevent this issue, it is
    // necessary to specify the table schema with the partition column, as it
    // guaranteed that the chunk name table always coincides with the column
    // index in the schema (because the chunk name table is initialized from the
    // schema columns).
    auto schema = New<TTableSchema>(
        std::vector{TColumnSchema(partitionColumn, ESimpleLogicalValueType::Int64)},
        /*strict*/ false);
    auto nameTable = TNameTable::FromSchema(*schema);

    auto partitioner = CreateColumnBasedPartitioner(
        shuffleHandle->PartitionCount,
        nameTable->GetId(partitionColumn));

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

    return MakeFuture(New<TShuffleWriter>(
        std::move(writer),
        this,
        std::move(shuffleHandle),
        writerIndex,
        options.OverwriteExistingWriterData))
        .As<IRowBatchWriterPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
