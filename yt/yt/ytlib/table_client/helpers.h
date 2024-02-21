#pragma once

#include "public.h"

#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/formats/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

NApi::ITableReaderPtr CreateApiFromSchemalessChunkReaderAdapter(
    ISchemalessChunkReaderPtr underlyingReader);

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    const ISchemalessChunkReaderPtr& reader,
    const IUnversionedRowsetWriterPtr& writer,
    const TPipeReaderToWriterOptions& options);

void PipeReaderToWriterByBatches(
    const ISchemalessChunkReaderPtr& reader,
    const NFormats::ISchemalessFormatWriterPtr& writer,
    const TRowBatchReadOptions& options,
    TDuration pipeDelay);

////////////////////////////////////////////////////////////////////////////////

//! Checks whether chunk with `chunkSortColumns' sort columns
//! can belong to sorted table with `tableSortColumns' sort columns and
//! given key uniqueness.
//! Table is sorted without key uniqueness requirement iff all of its
//! chunks are sorted, boundary keys of neighbouring chunks are properly ordered when
//! compared by first `tableSortColumns' values (replacing missing values with nulls if necessary)
//! and for all chunks `chunkSortColumns' is prefix of `tableSortColumns' or
//! `tableSortColumns' is prefix of `chunkSortColumns'.
//! Table is sorted with key uniqueness requirement iff all of its chunks are sorted
//! and have unique keys, boundary keys of neighbouring chunks are properly ordered
//! and different and `chunkSortColumns' is a prefix of `tableSortColumns'.
void ValidateSortColumns(
    const TSortColumns& tableSortColumns,
    const TSortColumns& chunkSortColumns,
    bool requireUniqueKeys);

//! Same as `ValidateSortColumns' but does not check column names.
void ValidateKeyColumnCount(
    int tableKeyColumnCount,
    int chunkKeyColumnCount,
    bool requireUniqueKeys);

TColumnFilter CreateColumnFilter(
    const std::optional<std::vector<TString>>& columns,
    const TNameTablePtr& nameTable);

////////////////////////////////////////////////////////////////////////////////

NControllerAgent::NProto::TOutputResult GetWrittenChunksBoundaryKeys(
    const ISchemalessMultiChunkWriterPtr& writer,
    bool withChunkSpecs = false);

std::pair<TLegacyOwningKey, TLegacyOwningKey> GetChunkBoundaryKeys(
    const NTableClient::NProto::TBoundaryKeysExt& boundaryKeysExt,
    int keyColumnCount);
std::pair<TLegacyOwningKey, TLegacyOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableTimestamp(
    const NYPath::TRichYPath& path,
    bool dynamic,
    const TTableSchema& schema,
    const NYTree::IAttributeDictionary& attributes,
    bool forceDisableDynamicStoreRead = false);

////////////////////////////////////////////////////////////////////////////////

std::tuple<std::vector<NChunkClient::TInputChunkPtr>, TTableSchemaPtr, bool> CollectTableInputChunks(
    const NYPath::TRichYPath& path,
    const NApi::NNative::IClientPtr& client,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NChunkClient::TFetchChunkSpecConfigPtr& config,
    NObjectClient::TTransactionId transactionId,
    std::vector<i32> extensionTags,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

void CheckUnavailableChunks(
    EUnavailableChunkStrategy strategy,
    NChunkClient::EChunkAvailabilityPolicy policy,
    std::vector<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

ui32 GetHeavyColumnStatisticsHash(ui32 salt, const TColumnStableName& stableName);

TColumnarStatistics GetColumnarStatistics(
    const NProto::THeavyColumnStatisticsExt& statistics,
    const std::vector<TColumnStableName>& columnNames,
    i64 chunkRowCount);

////////////////////////////////////////////////////////////////////////////////

//! Helper class for storing virtual values in schemaless reader.
//! It allows extracting them as vector of TUnversionedValue as well as
//! filling them as a sequence of RLE-encoded IUnversionedColumnarRowBatch::TColumn.
class TReaderVirtualValues
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TUnversionedValue>, Values);

public:
    TReaderVirtualValues() = default;

    void AddValue(TUnversionedValue value, TLogicalTypePtr logicalType);

    //! Return number of columns that are required for representing virtual
    //! column #virtualColumnIndex (including inner columns for RLE encoding).
    int GetBatchColumnCount(int virtualColumnIndex) const;

    //! Return total number of columns that are required for representing all virtual columns.
    int GetTotalColumnCount() const;

    void FillColumns(
        TMutableRange<IUnversionedColumnarRowBatch::TColumn> columnRange,
        int virtualColumnIndex,
        ui64 startIndex,
        ui64 valueCount) const;

private:
    static const ui64 Zero_;

    std::vector<TLogicalTypePtr> LogicalTypes_;

    void FillRleColumn(IUnversionedColumnarRowBatch::TColumn* rleColumn, int virtualColumnIndex) const;

    void FillMainColumn(
        IUnversionedColumnarRowBatch::TColumn* mainColumn,
        const IUnversionedColumnarRowBatch::TColumn* rleColumn,
        int virtualColumnIndex,
        ui64 startIndex,
        ui64 valueCount) const;
};

////////////////////////////////////////////////////////////////////////////////

NProto::THeavyColumnStatisticsExt GetHeavyColumnStatisticsExt(
    const TColumnarStatistics& columnarStatistics,
    const std::function<TColumnStableName(int index)>& geTColumnStableNameByIndex,
    int columnCount,
    int maxHeavyColumns);

////////////////////////////////////////////////////////////////////////////////

struct TExtraChunkTags
{
    std::optional<NCompression::ECodec> CompressionCodec;
    std::optional<NErasure::ECodec> ErasureCodec;
};

TExtraChunkTags MakeExtraChunkTags(const NChunkClient::NProto::TMiscExt& miscExt);

void AddTagsFromDataSource(const NYTree::IAttributeDictionaryPtr& baggage, const NChunkClient::TDataSource& dataSource);
void AddTagsFromDataSink(const NYTree::IAttributeDictionaryPtr& baggage, const NChunkClient::TDataSink& dataSink);

void AddExtraChunkTags(const NYTree::IAttributeDictionaryPtr& baggage, const TExtraChunkTags& extraTags);

void PackBaggageFromDataSource(const NTracing::TTraceContextPtr& context, const NChunkClient::TDataSource& dataSource);
void PackBaggageFromExtraChunkTags(const NTracing::TTraceContextPtr& context, const TExtraChunkTags& extraTags);

void PackBaggageForChunkReader(
    const NTracing::TTraceContextPtr& context,
    const NChunkClient::TDataSource& dataSource,
    const TExtraChunkTags& extraTags);

void PackBaggageForChunkWriter(
    const NTracing::TTraceContextPtr& context,
    const NChunkClient::TDataSink& dataSink,
    const TExtraChunkTags& extraTags);

////////////////////////////////////////////////////////////////////////////////

NYTree::IAttributeDictionaryPtr ResolveExternalTable(
    const NApi::NNative::IClientPtr& client,
    const NYPath::TYPath& path,
    TTableId* tableId,
    NObjectClient::TCellTag* externalCellTag,
    const std::vector<TString>& extraAttributeKeys = {});

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TColumnarStatisticsExt* protoStatisticsExt,
    const TColumnarStatistics& statistics);

//! `chunkRowCount` is used to initialize `TColumnarStatistics::ChunkRowCount` from old proto messages
//! that do not contain it explicitly. For new ones equality of these values is validated.
void FromProto(
    TColumnarStatistics* statistics,
    const NProto::TColumnarStatisticsExt& protoStatisticsExt,
    i64 chunkRowCount);

////////////////////////////////////////////////////////////////////////////////

void EnsureAnyValueEncoded(
    TUnversionedValue* value,
    const TTableSchema& schema,
    TChunkedMemoryPool* memoryPool,
    bool ignoreRequired);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
