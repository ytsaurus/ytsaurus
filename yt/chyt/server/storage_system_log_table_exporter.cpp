#include "storage_system_log_table_exporter.h"

#include "config.h"
#include "conversion.h"

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/error.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>

#include <deque>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SystemLogTableExporterLogger, "SystemLogTableExporter");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SystemLogTableExporterProfiler, "/system_log_table_exporter");

////////////////////////////////////////////////////////////////////////////////

class TCircularChunkBuffer
    : public TRefCounted
{
public:
    TCircularChunkBuffer(i64 maxBytesToKeep, i64 maxRowsToKeep)
        : MaxBytesToKeep_(maxBytesToKeep)
        , MaxRowsToKeep_(maxRowsToKeep)
    { }

    DB::Chunks GetChunks() const
    {
        auto lockGuard = ReaderGuard(SpinLock_);

        DB::Chunks result;
        result.reserve(Chunks_.size());

        for (const auto& chunk : Chunks_) {
            result.push_back(chunk.clone());
        }
        return result;
    }

    void AddChunks(DB::Chunks newChunks)
    {
        auto lockGuard = WriterGuard(SpinLock_);

        for (auto& chunk : newChunks) {
            TotalBytes_ += chunk.bytes();
            TotalRows_ += chunk.getNumRows();
            Chunks_.push_back(std::move(chunk));
        }

        while (!Chunks_.empty() && (TotalBytes_ > MaxBytesToKeep_ || TotalRows_ > MaxRowsToKeep_)) {
            TotalBytes_ -= Chunks_.front().bytes();
            TotalRows_ -= Chunks_.front().getNumRows();
            Chunks_.pop_front();
        }
    }

private:
    const i64 MaxBytesToKeep_;
    const i64 MaxRowsToKeep_;

    i64 TotalBytes_ = 0;
    i64 TotalRows_ = 0;

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    std::deque<DB::Chunk> Chunks_;
};

DECLARE_REFCOUNTED_CLASS(TCircularChunkBuffer)
DEFINE_REFCOUNTED_TYPE(TCircularChunkBuffer)

////////////////////////////////////////////////////////////////////////////////

class TCompletedRowlet
    : public IArchiveRowlet
{
public:
    explicit TCompletedRowlet(TUnversionedOwningRow row)
        : Row_(std::move(row))
    { }

    size_t EstimateSize() const override
    {
        return Row_.GetSpaceUsed();
    }

    TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        return Row_;
    }

private:
    TUnversionedOwningRow Row_;
};

////////////////////////////////////////////////////////////////////////////////

class TSystemLogTableExporterSink
    : public DB::SinkToStorage
{
public:
    TSystemLogTableExporterSink(
        const DB::Block& header,
        TCircularChunkBufferPtr storageBuffer,
        std::shared_ptr<const std::vector<int>> columnIndexToId,
        TCompositeSettingsPtr compositeSettings,
        IArchiveReporterPtr archiveReporter,
        TLogger logger)
        : DB::SinkToStorage(header)
        , StorageBuffer_(std::move(storageBuffer))
        , ColumnIndexToId_(std::move(columnIndexToId))
        , CompositeSettings_(std::move(compositeSettings))
        , ArchiveReporter_(std::move(archiveReporter))
        , Logger(std::move(logger))
    { }

    String getName() const override
    {
        return "SystemLogTableExporterSink";
    }

    void consume(DB::Chunk chunk) override
    {
        NewChunks_.push_back(std::move(chunk));
    }

    void onFinish() override
    {
        if (ArchiveReporter_) {
            for (const auto& chunk : NewChunks_) {
                try {
                    auto block = getHeader().cloneWithColumns(chunk.getColumns());
                    auto rowRange = ToRowRange(block, block.getDataTypes(), *ColumnIndexToId_, CompositeSettings_);
                    for (const auto& row : rowRange) {
                        ArchiveReporter_->Enqueue(std::make_unique<TCompletedRowlet>(TUnversionedOwningRow(row)));
                    }
                } catch (const std::exception& ex) {
                    YT_LOG_ERROR(ex, "Failed to convert chunk to unverionsed rows; chunk skipped (RowCount: %v)", chunk.getNumRows());
                }
            }
        }

        StorageBuffer_->AddChunks(std::move(NewChunks_));
    }

private:
    TCircularChunkBufferPtr StorageBuffer_;
    const std::shared_ptr<const std::vector<int>> ColumnIndexToId_;

    const TCompositeSettingsPtr CompositeSettings_;
    IArchiveReporterPtr ArchiveReporter_;

    const TLogger Logger;

    DB::Chunks NewChunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TStorageSystemLogTableExporter
    : public DB::IStorage
{
public:
    explicit TStorageSystemLogTableExporter(
        TSystemLogTableExporterConfigPtr config,
        TYPath cypressTableDirectory,
        NNative::IClientPtr client,
        IInvokerPtr invoker,
        DB::StorageID storageId,
        DB::ColumnsDescription columnsDescription)
        : DB::IStorage(std::move(storageId))
        , Config_(std::move(config))
        , CypressTableDirectory_(std::move(cypressTableDirectory))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , CompositeSettings_(TCompositeSettings::Create(/*convertUnsupportedTypesToString*/ true))
        , Schema_(ToTableSchema(columnsDescription, /*keyColumns*/ {}, CompositeSettings_))
        , NameTable_(TNameTable::FromSchema(Schema_))
        , ColumnIndexToId_(std::make_shared<const std::vector<int>>(
            GetColumnIndexToId(NameTable_, Schema_.GetColumnNames())))
        , Logger(SystemLogTableExporterLogger().WithTag("TableName: %v", getStorageID().getFullTableName()))
        , Data_(New<TCircularChunkBuffer>(Config_->MaxBytesToKeep, Config_->MaxRowsToKeep))
    {
        DB::StorageInMemoryMetadata storageMetadata;
        storageMetadata.setColumns(columnsDescription);
        setInMemoryMetadata(storageMetadata);
    }

    constexpr static auto Name = "SystemLogTableExporter";

    String getName() const override
    {
        return Name;
    }

    void startup() override
    {
        if (Config_->Enabled) {
            auto [version, schema, mounted] = GetLatestTableInfo();

            if (version == -1 || schema != Schema_) {
                ++version;
                CreateVersionedTable(version);
                MountVerionedTable(version);
            } else if (!mounted) {
                MountVerionedTable(version);
            }

            auto handlerConfig = New<TArchiveHandlerConfig>();
            handlerConfig->MaxInProgressDataSize = Config_->MaxInProgressDataSize;
            handlerConfig->Path = GetVersionedTablePath(version);

            ArchiveReporter_ = CreateArchiveReporter(
                New<TArchiveVersionHolder>(),
                Config_,
                std::move(handlerConfig),
                NameTable_,
                getStorageID().getFullTableName(),
                Client_,
                Invoker_,
                SystemLogTableExporterProfiler().WithTag("table_name", getStorageID().table_name));
        }
    }

    DB::Pipe read(
        const DB::Names& columnNames,
        const DB::StorageSnapshotPtr& storageSnapshot,
        DB::SelectQueryInfo& /*queryInfo*/,
        DB::ContextPtr /*context*/,
        DB::QueryProcessingStage::Enum /*processedStage*/,
        size_t /*maxBlockSize*/,
        size_t /*numStreams*/) override
    {
        storageSnapshot->check(columnNames);

        return DB::Pipe(std::make_shared<DB::SourceFromChunks>(storageSnapshot->metadata->getSampleBlock(), Data_->GetChunks()));
    }

    DB::SinkToStoragePtr write(
        const DB::ASTPtr& /*query*/,
        const DB::StorageMetadataPtr& metadataSnapshot,
        DB::ContextPtr /*context*/,
        bool /*asyncInsert*/) override
    {
        return std::make_shared<TSystemLogTableExporterSink>(
            metadataSnapshot->getSampleBlock(),
            Data_,
            ColumnIndexToId_,
            CompositeSettings_,
            ArchiveReporter_,
            Logger);
    }

private:
    const TSystemLogTableExporterConfigPtr Config_;
    const TYPath CypressTableDirectory_;
    const NNative::IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TCompositeSettingsPtr CompositeSettings_;
    const TTableSchema Schema_;
    const TNameTablePtr NameTable_;
    const std::shared_ptr<const std::vector<int>> ColumnIndexToId_;
    const TLogger Logger;

    TCircularChunkBufferPtr Data_;
    IArchiveReporterPtr ArchiveReporter_;

    TYPath GetLatestTablePath() const
    {
        return Format("%v/latest", CypressTableDirectory_);
    }

    TYPath GetVersionedTablePath(int version) const
    {
        return Format("%v/%v", CypressTableDirectory_, version);
    }

    struct TVersionedTableInfo
    {
        int Version = -1;
        TTableSchema Schema;
        bool Mounted = false;
    };

    TVersionedTableInfo GetLatestTableInfo()
    {
        YT_LOG_DEBUG("Getting latest cypress table info");

        TGetNodeOptions options;
        options.Attributes.Keys = {"key", "schema", "tablet_state"};

        auto resultOrError = WaitFor(Client_->GetNode(GetLatestTablePath() + "/@", options));

        if (resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            YT_LOG_DEBUG(resultOrError, "Cypress table does not exist");
            return {};
        }

        auto result = ConvertToNode(resultOrError.ValueOrThrow())->AsMap();

        int version = FromString<int>(result->GetChildValueOrThrow<TString>("key"));
        auto schema = result->GetChildValueOrThrow<TTableSchema>("schema");
        bool mounted = (result->GetChildValueOrThrow<ETabletState>("tablet_state") == ETabletState::Mounted);

        YT_LOG_DEBUG("Got latest cypress table info (Version: %v, Mounted: %v)", version, mounted);

        return {version, std::move(schema), mounted};
    }

    void CreateVersionedTable(int version)
    {
        // NB: All CreateNode calls are made with either IgnoreExisting or Force options,
        // so these calls are idempotent and can be performed on several instances simultaneously
        // without any synchronization.
        TCreateNodeOptions options;
        options.IgnoreExisting = true;

        YT_LOG_DEBUG("Creating cypress table directory");

        WaitFor(Client_->CreateNode(CypressTableDirectory_, EObjectType::MapNode, options))
            .ThrowOnError();

        auto attributes = ConvertToAttributes(Config_->CreateTableAttributes);
        attributes->Set("atomicity", NTransactionClient::EAtomicity::None);
        attributes->Set("dynamic", true);
        attributes->Set("schema", Schema_);

        options = {};
        options.Attributes = attributes;
        options.IgnoreExisting = true;

        YT_LOG_DEBUG("Creating versioned cypress table (Version: %v)", version);

        WaitFor(Client_->CreateNode(GetVersionedTablePath(version), EObjectType::Table, options))
            .ThrowOnError();

        attributes = CreateEphemeralAttributes();
        attributes->Set("target_path", GetVersionedTablePath(version));

        options = {};
        options.Attributes = attributes;
        options.Force = true;

        YT_LOG_DEBUG("Updating latest link node (Version: %v)", version);

        WaitFor(Client_->CreateNode(GetLatestTablePath(), EObjectType::Link, options))
            .ThrowOnError();

        YT_LOG_DEBUG("Cypress table created and set up (Version: %v)", version);
    }

    void MountVerionedTable(int version)
    {
        YT_LOG_DEBUG("Mounting table (Version: %v)", version);

        WaitFor(Client_->MountTable(GetVersionedTablePath(version)))
            .ThrowOnError();

        YT_LOG_DEBUG("Table mounted (Version: %v)", version);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterStorageSystemLogTableExporter(
    const TSystemLogTableExportersConfigPtr& config,
    const NNative::IClientPtr& client,
    const IInvokerPtr& invoker)
{
    auto& factory = DB::StorageFactory::instance();

    factory.registerStorage(TStorageSystemLogTableExporter::Name, [config, client, invoker](const DB::StorageFactory::Arguments& args) {
        if (args.table_id.database_name != "system") {
            THROW_ERROR_EXCEPTION("%v table may be created only in system database, got %Qv",
                TStorageSystemLogTableExporter::Name,
                args.table_id.database_name);
        }
        return std::make_shared<TStorageSystemLogTableExporter>(
            GetOrDefault(config->Tables, args.table_id.table_name, config->Default),
            Format("%v/%v", config->CypressRootDirectory, ToYPathLiteral(args.table_id.table_name)),
            client,
            invoker,
            args.table_id,
            args.columns);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
