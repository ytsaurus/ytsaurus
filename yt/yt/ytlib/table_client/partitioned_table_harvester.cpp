#include "partitioned_table_harvester.h"

#include "config.h"
#include "schema.h"
#include "chunk_meta_extensions.h"
#include "table_read_spec.h"
#include "virtual_value_directory.h"
#include "helpers.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_spec_fetcher.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/data_source.h>

#include <yt/ytlib/object_client/object_attribute_cache.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/helpers.h>

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/client/object_client/public.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NTableClient {

using namespace NYPath;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NYTree;
using namespace NFuncTools;
using namespace NCypressClient;
using NChunkClient::NProto::TChunkSpec;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionState
{
    TPartitionConfigPtr Config;
    int PartitionIndex = -1;

    TObjectId ObjectId;
    ui64 ExternalCellTag;
    int ChunkCount;
    TTableSchemaPtr TableSchema;
    TUnversionedRow MinKey;
    TUnversionedRow MaxKey;
    //! MinKey prepended with partition key.
    TUnversionedRow ExtendedMinKey;
    //! MaxKey prepended with partition key.
    TUnversionedRow ExtendedMaxKey;
    bool IsEmpty = false;
    bool IsDynamic = false;

    //! Error for this particular partition.
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

static const std::vector<TString> PartitionedTableRelatedAttributes = {
    "id",
    "external",
    "external_cell_tag",
    "chunk_count",
    "type",
    "partitions",
    "partitioned_by",
    "schema",
    "dynamic",
    "boundary_keys",
};

class TPartitionedTableHarvester::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TPartitionedTableHarvesterOptions options)
        : RichPath_(std::move(options.RichPath))
        , Path_(RichPath_.GetPath())
        , Client_(std::move(options.Client))
        , TransactionId_(std::move(options.TransactionId))
        , Invoker_(std::move(options.Invoker))
        , ObjectAttributeCache_(std::move(options.ObjectAttributeCache))
        , Config_(std::move(options.Config))
        , NameTable_(std::move(options.NameTable))
        , Logger(options.Logger)
    { }

    TFuture<void> Prepare()
    {
        return BIND(&TImpl::DoPrepare, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<TTableReadSpec> Fetch(const TFetchSingleTableReadSpecOptions& options)
    {
        return BIND(&TImpl::DoFetch, MakeStrong(this), options)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    TRichYPath RichPath_;
    TYPath Path_;
    NApi::NNative::IClientPtr Client_;
    TTransactionId TransactionId_;
    IInvokerPtr Invoker_;
    TObjectAttributeCachePtr ObjectAttributeCache_;
    TPartitionedTableHarvesterConfigPtr Config_;
    TColumnFilter ColumnFilter_;
    TNameTablePtr NameTable_;
    TLogger Logger;

    std::vector<TPartitionState> PartitionStates_;
    //! Partitioning key column prefix.
    std::vector<TString> PartitionedBy_;
    //! Key column prefix for partitions.
    std::vector<TString> PhysicalKeyColumns_;
    //! Schema on partitioned table. In particular, PartitionedBy_ + PhysicalKeyColumns_ == Schema_.GetKeyColumns().
    TTableSchemaPtr Schema_;
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    std::vector<TChunkSpec> ChunkSpecs_;
    std::vector<TDataSliceDescriptor> DataSliceDescriptors_;
    TVirtualValueDirectoryPtr VirtualValueDirectory_;

    void DoPrepare()
    {
        YT_LOG_INFO("Fetching partitioned table (Path: %v, TransactionId: %v)", Path_, TransactionId_);

        FetchPartitionedTable();
        FetchPartitions();
        ValidatePartitionSchemas();
        ValidatePartitionKeys();
        ValidateSortOrder();
        ValidateMisconfiguredPartitions();

        YT_LOG_INFO("Partitioned table fetched (Path: %v)", Path_);
    }

    void FetchPartitionedTable()
    {
        YT_LOG_DEBUG("Fetching partitioned table node attributes (Path: %v)");

        // This may throw if master request fails.
        auto attributeVector = WaitFor(FetchObjectAttributes({Path_}))
            .ValueOrThrow();

        YT_VERIFY(attributeVector.size() == 1);

        // This may throw if partitioned table not found.
        auto attributes = attributeVector.front().ValueOrThrow();
        auto maybeType = attributes->Find<EObjectType>("type");
        if (!Config_->AssumePartitionedTable && maybeType != EObjectType::PartitionedTable) {
            THROW_ERROR_EXCEPTION(
                "Object %Qv is expected to be partitioned table, buf %Qlv found",
                Path_,
                maybeType);
        }

        // TODO(max42): support ranges.
        if (RichPath_.HasNontrivialRanges()) {
            THROW_ERROR_EXCEPTION("Partitioned tables do not support range selectors");
        }

        auto partitions = attributes->Get<std::vector<TPartitionConfigPtr>>("partitions");

        for (const auto& [partitionIndex, partitionConfig] : Enumerate(partitions)) {
            auto& state = PartitionStates_.emplace_back();
            state.PartitionIndex = partitionIndex;
            state.Config = partitionConfig;
        }

        PartitionedBy_ = attributes->Get<std::vector<TString>>("partitioned_by");
        Schema_ = attributes->Get<TTableSchemaPtr>("schema");

        // Check that partitioned_by is a prefix of key columns.
        const auto& keyColumns = Schema_->GetKeyColumns();
        auto mismatchIt = std::mismatch(PartitionedBy_.begin(), PartitionedBy_.end(), keyColumns.begin(), keyColumns.end()).first;
        if (mismatchIt != PartitionedBy_.end()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::InvalidPartitionedBy, "partitioned_by should form a prefix of schema key columns")
                    << TErrorAttribute("partitioned_by", PartitionedBy_)
                    << TErrorAttribute("key_columns", keyColumns);
        }

        PhysicalKeyColumns_ = std::vector<TString>(keyColumns.begin() + PartitionedBy_.size(), keyColumns.end());

        const auto& columns = Schema_->Columns();
        for (int columnIndex = 0; columnIndex < PartitionedBy_.size(); ++columnIndex) {
            const auto& column = columns[columnIndex];
            if (column.Expression()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::InvalidSchemaValue, "Partitioned columns cannot be computed")
                        << TErrorAttribute("column", column.Name());
            }
            if (column.Aggregate()) {
                THROW_ERROR_EXCEPTION(
                    NTableClient::EErrorCode::InvalidSchemaValue, "Partitioned columns cannot be aggregated")
                        << TErrorAttribute("column", column.Name());
            }
        }

        // It is possible to validate key uniqueness, but let's wait until somebody wants it.
        if (Schema_->IsUniqueKeys()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::InvalidSchemaValue,
                "Unique keys specification is not supported");
        }

        YT_LOG_DEBUG("Finished fetching partitioned table node attributes");
    }

    void FetchPartitions()
    {
        YT_LOG_DEBUG(
            "Fetching partition attributes (PartitionCount: %v)",
            PartitionStates_.size());

        std::vector<TYPath> partitionPaths;
        for (const auto& state : PartitionStates_) {
            partitionPaths.emplace_back(state.Config->Path);
        }

        auto partitionAttributeVector = WaitFor(FetchObjectAttributes(partitionPaths))
            .ValueOrThrow();

        for (const auto& [attributesOrError, state] : Zip(partitionAttributeVector, PartitionStates_)) {
            if (!attributesOrError.IsOK()) {
                state.Error = attributesOrError;
                continue;
            }

            const auto& attributes = attributesOrError.Value();

            auto maybeType = attributes->Find<EObjectType>("type");
            if (maybeType != EObjectType::Table) {
                state.Error = TError(
                    NObjectClient::EErrorCode::InvalidObjectType,
                    "Object has invalid type; expected table, but %Qlv found", maybeType);
                continue;
            }

            state.ObjectId = attributes->Get<TObjectId>("id");
            state.ExternalCellTag = attributes->Get<bool>("external")
                ? attributes->Get<ui64>("external_cell_tag")
                : CellTagFromId(state.ObjectId);
            state.ChunkCount = attributes->Get<int>("chunk_count");

            state.IsDynamic = attributes->Get<bool>("dynamic");
            if (state.IsDynamic) {
                // Dynamic tables are not supported for now.
                state.Error = TError(
                    NObjectClient::EErrorCode::InvalidObjectType,
                    "Dynamic tables are not supported as partitions");
                continue;
            }
            state.TableSchema = attributes->Get<TTableSchemaPtr>("schema");

            if (state.ChunkCount == 0) {
                state.IsEmpty = true;
            } else {
                const auto& boundaryKeys = attributes->Get<TOwningBoundaryKeys>("boundary_keys");
                state.MinKey = RowBuffer_->Capture(boundaryKeys.MinKey);
                state.MaxKey = RowBuffer_->Capture(boundaryKeys.MaxKey);
            }
        }

        YT_LOG_DEBUG("Finished fetching partition attributes");
    }

    //! We iteratively check lots of conditions for partitions; this helper
    //! eliminates the need of if (!state.Error.IsOK()) { continue; } check.
    auto GetValidPartitionStates()
    {
        return Filter([] (const auto& state) { return state.Error.IsOK(); }, PartitionStates_);
    }

    //! Validate each particular partition for being a static table with
    //! schema compatible to partitioned table schema.
    void ValidatePartitionSchemas()
    {
        // We strip partitioned column prefix from schema and validate all partition schemas to be compatible with it.
        auto columns = Schema_->Columns();
        columns.erase(columns.begin(), columns.begin() + PartitionedBy_.size());

        TTableSchema reducedSchema(columns, Schema_->GetStrict());

        for (auto& partitionState : GetValidPartitionStates()) {
            const auto& [compatibility, error] = CheckTableSchemaCompatibility(*partitionState.TableSchema, reducedSchema, false);
            partitionState.Error = error;
        }
    }

    //! Validate that partition keys correspond to partitioned column schema.
    void ValidatePartitionKeys()
    {
        // Consider only partitioned columns to be key.
        const auto& reducedSchema = Schema_->SetKeyColumnCount(PartitionedBy_.size());

        for (auto& partitionState : GetValidPartitionStates()) {
            try {
                if (partitionState.Config->Key.GetCount() != PartitionedBy_.size()) {
                    THROW_ERROR_EXCEPTION(
                        NTableClient::EErrorCode::SchemaViolation,
                        "Partition key should contain same number of columns as in partitioned_by")
                        << TErrorAttribute("partition_key_length", partitionState.Config->Key.GetCount())
                        << TErrorAttribute("partitioned_by", PartitionedBy_);
                }
                ValidatePivotKey(partitionState.Config->Key, *reducedSchema, "partition" /* keyType */);
            } catch (const std::exception& ex) {
                partitionState.Error = TError(ex);
            }
        }
    }

    //! Check sort order among partitions.
    //! NB: throws an error immediately in case of sort order violation.
    //! We cannot be sure which order was actually meant by user, so we
    //! prefer to make this error fatal.
    void ValidateSortOrder()
    {
        for (auto& partitionState : GetValidPartitionStates()) {
            if (partitionState.IsEmpty) {
                continue;
            }
            auto captureExtendedRow = [&] (TUnversionedRow row) {
                auto resultRow = RowBuffer_->AllocateUnversioned(Schema_->GetKeyColumnCount());
                std::memcpy(
                    resultRow.Begin(),
                    partitionState.Config->Key.Begin(),
                    sizeof(TUnversionedValue) * PartitionedBy_.size());
                std::memcpy(
                    resultRow.Begin() + PartitionedBy_.size(),
                    row.Begin(),
                    sizeof(TUnversionedValue) * (Schema_->GetKeyColumnCount() - PartitionedBy_.size()));
                return resultRow;
            };
            partitionState.ExtendedMinKey = captureExtendedRow(partitionState.MinKey);
            partitionState.ExtendedMaxKey = captureExtendedRow(partitionState.MaxKey);
        }

        const TPartitionState* previousPartitionState = nullptr;
        for (const auto& partitionState : GetValidPartitionStates()) {
            if (partitionState.IsEmpty) {
                continue;
            }
            if (previousPartitionState && previousPartitionState->ExtendedMaxKey > partitionState.ExtendedMinKey) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::SortOrderViolation,
                    "Sort order violation: %v > %v",
                    previousPartitionState->MaxKey,
                    partitionState.MinKey)
                    << TErrorAttribute("lhs_partition_index", previousPartitionState->PartitionIndex)
                    << TErrorAttribute("lhs_partition_path", previousPartitionState->Config->Path)
                    << TErrorAttribute("rhs_partition_index", partitionState.PartitionIndex)
                    << TErrorAttribute("rhs_partition_path", partitionState.Config->Path);
            }
            previousPartitionState = &partitionState;
        }
    }

    //! If there are misconfigured partitions and tactics requires throwing,
    //! throw a composite error consisting of all partition errors.
    void ValidateMisconfiguredPartitions()
    {
        if (Config_->MisconfiguredPartitionTactics == EMisconfiguredPartitionTactics::Skip) {
            return;
        }

        std::vector<TError> errors;

        for (const auto& partitionState : PartitionStates_) {
            if (!partitionState.Error.IsOK()) {
                errors.emplace_back(partitionState.Error
                    << TErrorAttribute("partition_index", partitionState.PartitionIndex)
                    << TErrorAttribute("partition_path", partitionState.Config->Path));
            }
        }

        if (!errors.empty()) {
            auto totalErrorCount = errors.size();
            if (errors.size() > Config_->MaxPartitionErrorCount) {
                errors.resize(Config_->MaxPartitionErrorCount);
            }
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::MisconfiguredPartitions,
                "Some partitions are misconfigured; %v of %v errors are listed as inner errors",
                errors.size(),
                totalErrorCount)
                << errors;
        }
    }

    TFuture<std::vector<TErrorOr<IAttributeDictionaryPtr>>> FetchObjectAttributes(const std::vector<TYPath>& paths)
    {
        // TODO(max42): YT-13496: improve object attribute cache so that
        // attribute names become part of the key.
#ifdef NDEBUG
        if (ObjectAttributeCache_) {
            auto sortedPartitionedTableRelatedAttributes = PartitionedTableRelatedAttributes;
            std::sort(sortedPartitionedTableRelatedAttributes.begin(), sortedPartitionedTableRelatedAttributes.end());
            auto sortedObjectAttributeCacheAttributes = ObjectAttributeCache_->AttributeNames();
            std::sort(sortedObjectAttributeCacheAttributes.begin(), sortedObjectAttributeCacheAttributes.end());
            YT_ASSERT(std::includes(
                sortedPartitionedTableRelatedAttributes.begin(),
                sortedPartitionedTableRelatedAttributes.end(),
                sortedObjectAttributeCacheAttributes.begin(),
                sortedObjectAttributeCacheAttributes.end()));
        }
#endif

        return ObjectAttributeCache_
            ? ObjectAttributeCache_->Get(paths)
            : TObjectAttributeCache::GetFromClient(paths, Client_, PartitionedTableRelatedAttributes);
    }

    //! Build virtual column directory and data slice descriptors with filled virtual row indices.
    void BuildVirtualValueDirectoryAndDataSliceDescriptors()
    {
        // Leave only virtual columns requested by column filter.
        std::vector<int> virtualColumnIndices(PartitionedBy_.size());
        std::iota(virtualColumnIndices.begin(), virtualColumnIndices.end(), 0);
        // TODO(max42): YT-13553. By this moment column filter is useless (at least in read_table)
        // and all meaningful information is hidden in column selectors in rich YPath.
        // Fix that by forming proper column filter before fetching table read spec and
        // passing it to readers. The code below is done similarly to
        //   columnFilter.IsUniversal() ? CreateColumnFilter(dataSource.Columns(), nameTable) : columnFilter
        // idiom from #CreateReaderFactories.
        if (ColumnFilter_.IsUniversal() && RichPath_.GetColumns()) {
            ColumnFilter_ = CreateColumnFilter(RichPath_.GetColumns(), NameTable_);
        }

        if (!ColumnFilter_.IsUniversal()) {
            auto it = std::remove_if(
                virtualColumnIndices.begin(),
                virtualColumnIndices.end(),
                [&] (int index) {
                    const auto& columnName = PartitionedBy_[index];
                    auto id = NameTable_->FindId(columnName);
                    return !id || !ColumnFilter_.ContainsIndex(*id);
                });
            virtualColumnIndices.erase(it, virtualColumnIndices.end());
        }

        TNameTablePtr virtualNameTable = New<TNameTable>();
        for (int index : virtualColumnIndices) {
            virtualNameTable->RegisterName(PartitionedBy_[index]);
        }

        // Now build virtual rows and save mapping partition index -> virtual row index.
        // Virtual row indices are saved to chunk specs and are used in readers to retrieve
        // virtual unversioned values later.
        TUnversionedRowsBuilder builder;
        TUnversionedRowBuilder rowBuilder;
        std::vector<int> partitionIndexToVirtualRowIndex(PartitionStates_.size(), -1);
        {
            int virtualRowIndex = 0;
            for (const auto& partitionState : GetValidPartitionStates()) {
                int virtualColumnIndex = 0;
                for (int index : virtualColumnIndices) {
                    auto value = partitionState.Config->Key[index];
                    value.Id = virtualColumnIndex++;
                    rowBuilder.AddValue(partitionState.Config->Key[index]);
                }
                builder.AddRow(rowBuilder.GetRow());
                rowBuilder.Reset();
                partitionIndexToVirtualRowIndex[partitionState.PartitionIndex] = virtualRowIndex++;
            }
        }

        VirtualValueDirectory_ = New<TVirtualValueDirectory>();
        VirtualValueDirectory_->NameTable = std::move(virtualNameTable);
        VirtualValueDirectory_->Rows = builder.Build();

        for (auto& chunkSpec : ChunkSpecs_) {
            int partitionIndex = chunkSpec.table_index();
            chunkSpec.set_table_index(0);
            auto virtualRowIndex = partitionIndexToVirtualRowIndex[partitionIndex];
            YT_VERIFY(virtualRowIndex != -1);
            DataSliceDescriptors_.emplace_back(
                std::move(chunkSpec),
                virtualRowIndex);
        }
    }

    TTableReadSpec DoFetch(const TFetchSingleTableReadSpecOptions& options)
    {
        auto chunkSpecFetcher = New<TChunkSpecFetcher>(
            Client_,
            nullptr /* nodeDirectory */,
            Invoker_,
            options.FetchChunkSpecConfig->MaxChunksPerFetch,
            options.FetchChunkSpecConfig->MaxChunksPerLocateRequest,
            [=] (const TChunkOwnerYPathProxy::TReqFetchPtr& req, int /*tableIndex*/) {
                req->set_fetch_all_meta_extensions(false);
                req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value);
                req->add_extension_tags(TProtoExtensionTag<NTableClient::NProto::THeavyColumnStatisticsExt>::Value);
                SetTransactionId(req, TransactionId_);
                SetSuppressAccessTracking(req, options.GetUserObjectBasicAttributesOptions.SuppressAccessTracking);
                SetSuppressExpirationTimeoutRenewal(req, options.GetUserObjectBasicAttributesOptions.SuppressAccessTracking);
            },
            Logger);

        for (const auto& partitionState : PartitionStates_) {
            // We temporarily assume table index to be partition index, so that
            // we can later recover which chunk spec comes from which partition.
            chunkSpecFetcher->Add(
                partitionState.ObjectId,
                partitionState.ExternalCellTag,
                partitionState.ChunkCount,
                partitionState.PartitionIndex);
        }

        WaitFor(chunkSpecFetcher->Fetch())
            .ThrowOnError();

        ChunkSpecs_ = std::move(chunkSpecFetcher->ChunkSpecs());

        BuildVirtualValueDirectoryAndDataSliceDescriptors();

        auto dataSourceDirectory = New<TDataSourceDirectory>();
        dataSourceDirectory->DataSources().emplace_back(
            MakePartitionedTableDataSource(
                Path_,
                Schema_,
                PartitionedBy_.size(),
                RichPath_.GetColumns(),
                {},
                VirtualValueDirectory_));

        YT_LOG_INFO(
            "Fetched partitioned table (DataSliceCount: %v, PhysicalKeyColumns: %v, VirtualColumnCount: %v, VirtualRowCount: %v)",
            DataSliceDescriptors_.size(),
            PhysicalKeyColumns_,
            VirtualValueDirectory_->NameTable->GetSize(),
            VirtualValueDirectory_->Rows.Size());

        return TTableReadSpec {
            .DataSourceDirectory = std::move(dataSourceDirectory),
            .DataSliceDescriptors = std::move(DataSliceDescriptors_),
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TPartitionedTableHarvester::TPartitionedTableHarvester(TPartitionedTableHarvesterOptions options)
    : Impl_(New<TImpl>(std::move(options)))
{ }

TPartitionedTableHarvester::~TPartitionedTableHarvester() = default;

TFuture<void> TPartitionedTableHarvester::Prepare()
{
    return Impl_->Prepare();
}

TFuture<TTableReadSpec> TPartitionedTableHarvester::Fetch(const TFetchSingleTableReadSpecOptions& options)
{
    return Impl_->Fetch(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
