#pragma once

#include "public.h"

#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/adapters.h>
#include <yt/client/table_client/table_output.h>

#include <yt/client/chunk_client/public.h>

#include <yt/client/formats/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/chunk_owner_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/scheduler/public.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/public.h>


namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

NApi::ITableReaderPtr CreateApiFromSchemalessChunkReaderAdapter(
    ISchemalessChunkReaderPtr underlyingReader);

////////////////////////////////////////////////////////////////////////////////

void PipeReaderToWriter(
    const ISchemalessChunkReaderPtr& reader,
    IUnversionedRowsetWriterPtr writer,
    const TPipeReaderToWriterOptions& options);

////////////////////////////////////////////////////////////////////////////////

// NB: not using TYsonString here to avoid copying.
TUnversionedValue MakeUnversionedValue(
    TStringBuf ysonString,
    int id,
    NYson::TStatelessLexer& lexer);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(
    const TKeyColumns& keyColumns,
    const TKeyColumns& chunkKeyColumns,
    bool requireUniqueKeys,
    bool validateColumnNames);
TColumnFilter CreateColumnFilter(const std::optional<std::vector<TString>>& columns, TNameTablePtr nameTable);
int GetSystemColumnCount(TChunkReaderOptionsPtr options);

////////////////////////////////////////////////////////////////////////////////

NScheduler::NProto::TOutputResult GetWrittenChunksBoundaryKeys(
    ISchemalessMultiChunkWriterPtr writer);

std::pair<TOwningKey, TOwningKey> GetChunkBoundaryKeys(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableTimestamp(
    const NYPath::TRichYPath& path,
    bool dynamic,
    const TTableSchema& schema,
    const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

std::vector<NChunkClient::TInputChunkPtr> CollectTableInputChunks(
    const NYPath::TRichYPath& path,
    const NApi::NNative::IClientPtr& client,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NChunkClient::TFetchChunkSpecConfigPtr& config,
    NObjectClient::TTransactionId transactionId,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Helpers for updating columnar statistics with versioned and unversioned rows.
void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, TUnversionedRow row);
void UpdateColumnarStatistics(NProto::TColumnarStatisticsExt& columnarStatisticsExt, TVersionedRow row);

////////////////////////////////////////////////////////////////////////////////

void CheckUnavailableChunks(EUnavailableChunkStrategy strategy, std::vector<NChunkClient::NProto::TChunkSpec>* chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
