#include "table_reader.h"

#include "client.h"

#include <yt/yt/ytlib/table_client/table_read_spec.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/config.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/ytlib/transaction_client/transaction_listener.h>

#include <yt/yt/ytlib/object_client/helpers.h>

#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NApi::NNative {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TTableReader
    : public ITableReader
    , public TTransactionListener
{
public:
    TTableReader(
        TTableReaderConfigPtr config,
        TTableReaderOptions options,
        IClientPtr client,
        NApi::ITransactionPtr transaction,
        TRichYPath richPath,
        TNameTablePtr nameTable,
        TColumnFilter columnFilter,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IMemoryUsageTrackerPtr memoryUsageTracker)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Client_(std::move(client))
        , Transaction_(std::move(transaction))
        , RichPath_(std::move(richPath))
        , NameTable_(std::move(nameTable))
        , ColumnFilter_(std::move(columnFilter))
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , TransactionId_(Transaction_ ? Transaction_->GetId() : NullTransactionId)
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Client_);

        ReadyEvent_ = BIND(&TTableReader::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (NProfiling::GetCpuInstant() > ReadDeadline_) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::ReaderDeadlineExpired, "Reader deadline expired");
        }

        if (IsAborted() || !ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        YT_VERIFY(Reader_);
        return Reader_->Read(options);
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return ReadyEvent_;
        }

        if (IsAborted()) {
            return MakeFuture(GetAbortError());
        }

        YT_VERIFY(Reader_);
        return Reader_->GetReadyEvent();
    }

    i64 GetStartRowIndex() const override
    {
        YT_VERIFY(Reader_);
        return StartRowIndex_;
    }

    i64 GetTotalRowCount() const override
    {
        YT_VERIFY(Reader_);
        return Reader_->GetTotalRowCount();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YT_VERIFY(Reader_);
        return Reader_->GetDataStatistics();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        YT_VERIFY(Reader_);
        return Reader_->GetNameTable();
    }

    const TTableSchemaPtr& GetTableSchema() const override
    {
        YT_VERIFY(Reader_);
        return TableSchema_;
    }

    const std::vector<std::string>& GetOmittedInaccessibleColumns() const override
    {
        YT_VERIFY(Reader_);
        return OmittedInaccessibleColumns_;
    }

private:
    const TTableReaderConfigPtr Config_;
    TTableReaderOptions Options_;
    const IClientPtr Client_;
    const NApi::ITransactionPtr Transaction_;
    const TRichYPath RichPath_;
    const TNameTablePtr NameTable_;
    const TColumnFilter ColumnFilter_;
    const IThroughputThrottlerPtr BandwidthThrottler_;
    const IThroughputThrottlerPtr RpsThrottler_;
    const TTransactionId TransactionId_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TFuture<void> ReadyEvent_;
    ISchemalessMultiChunkReaderPtr Reader_;
    TTableSchemaPtr TableSchema_;
    std::vector<std::string> OmittedInaccessibleColumns_;
    i64 StartRowIndex_;
    NProfiling::TCpuInstant ReadDeadline_ = Max<NProfiling::TCpuInstant>();

    void DoOpen()
    {
        // Transform NApi::TTableReaderOptions into NTableClient::TTableReader{Options,Config}.
        auto tableReaderConfig = Options_.Config ? Options_.Config : New<TTableReaderConfig>();
        auto tableReaderOptions = ToInternalTableReaderOptions(Options_);

        auto readSessionId = TReadSessionId::Create();
        auto fetchTableReadSpecOptions = TFetchSingleTableReadSpecOptions{
            .RichPath = RichPath_,
            .Client = Client_,
            .TransactionId = Options_.TransactionId,
            .ReadSessionId = readSessionId,
            .GetUserObjectBasicAttributesOptions = TGetUserObjectBasicAttributesOptions{
                // It's fine to ignore SuppressModificationTracking, since read requests can't modify table.
                .SuppressAccessTracking = tableReaderConfig->SuppressAccessTracking || Options_.SuppressAccessTracking,
                .SuppressExpirationTimeoutRenewal = tableReaderConfig->SuppressExpirationTimeoutRenewal || Options_.SuppressExpirationTimeoutRenewal,
                .OmitInaccessibleColumns = Options_.OmitInaccessibleColumns,
            },
            .FetchChunkSpecConfig = Config_,
            .FetchParityReplicas = tableReaderConfig->EnableAutoRepair,
            .UnavailableChunkStrategy = tableReaderConfig->UnavailableChunkStrategy,
            .ChunkAvailabilityPolicy = tableReaderConfig->ChunkAvailabilityPolicy,
        };

        TClientChunkReadOptions chunkReadOptions;
        chunkReadOptions.MemoryUsageTracker = MemoryUsageTracker_;
        chunkReadOptions.WorkloadDescriptor = tableReaderConfig->WorkloadDescriptor;
        chunkReadOptions.WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", RichPath_.GetPath()));
        chunkReadOptions.ReadSessionId = readSessionId;

        auto tableReadSpec = FetchSingleTableReadSpec(fetchTableReadSpecOptions);
        YT_VERIFY(tableReadSpec.DataSourceDirectory->DataSources().size() == 1);
        const auto& dataSource = tableReadSpec.DataSourceDirectory->DataSources().front();
        TableSchema_ = dataSource.Schema();
        OmittedInaccessibleColumns_ = dataSource.OmittedInaccessibleColumns();
        Reader_ = CreateAppropriateSchemalessMultiChunkReader(
            tableReaderOptions,
            tableReaderConfig,
            TChunkReaderHost::FromClient(Client_, BandwidthThrottler_, RpsThrottler_),
            tableReadSpec,
            chunkReadOptions,
            Options_.Unordered,
            NameTable_,
            ColumnFilter_);

        WaitFor(Reader_->GetReadyEvent())
            .ThrowOnError();

        StartRowIndex_ = Reader_->GetTableRowIndex();

        if (Transaction_) {
            StartListenTransaction(Transaction_);
        }

        if (Config_->MaxReadDuration) {
            ReadDeadline_ = NProfiling::GetCpuInstant() + NProfiling::DurationToCpuDuration(*Config_->MaxReadDuration);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<ITableReaderPtr> CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IMemoryUsageTrackerPtr memoryUsageTracker)
{
    NApi::ITransactionPtr transaction;
    if (options.TransactionId) {
        TTransactionAttachOptions transactionOptions;
        transactionOptions.Ping = options.Ping;
        transactionOptions.PingAncestors = options.PingAncestors;
        transaction = client->AttachTransaction(options.TransactionId, transactionOptions);
    }

    auto reader = New<TTableReader>(
        options.Config ? options.Config : New<TTableReaderConfig>(),
        options,
        client,
        transaction,
        path,
        nameTable,
        columnFilter,
        bandwidthThrottler,
        rpsThrottler,
        std::move(memoryUsageTracker));

    return reader->GetReadyEvent().Apply(BIND([=] () -> ITableReaderPtr {
        return reader;
    }));
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessMultiChunkReaderAdapter
    : public IRowBatchReader
{
public:
    TSchemalessMultiChunkReaderAdapter(ISchemalessMultiChunkReaderPtr reader)
        : Reader_(std::move(reader))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        return Reader_->Read(options);
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Reader_->GetNameTable();
    }

    TFuture<void> GetReadyEvent() const override
    {
        return Reader_->GetReadyEvent();
    }

private:
    const ISchemalessMultiChunkReaderPtr Reader_;
};

IRowBatchReaderPtr ToApiRowBatchReader(ISchemalessMultiChunkReaderPtr reader)
{
    return New<TSchemalessMultiChunkReaderAdapter>(std::move(reader));
}

NTableClient::TTableReaderOptionsPtr ToInternalTableReaderOptions(const TTableReaderOptions& options)
{
    auto result = New<NTableClient::TTableReaderOptions>();
    result->EnableTableIndex = options.EnableTableIndex;
    result->EnableRangeIndex = options.EnableRangeIndex;
    result->EnableRowIndex = options.EnableRowIndex;
    result->EnableTabletIndex = options.EnableTabletIndex;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
