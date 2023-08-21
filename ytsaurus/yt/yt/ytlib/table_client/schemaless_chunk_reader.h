#pragma once

#include "public.h"

#include "timing_reader.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessChunkReader
    : public ISchemalessUnversionedReader
    // TODO(max42): maybe move this base up to NChunkClient::IReaderBase?
    , public virtual ITimingReader
{
    //! Return the current row index (measured from the start of the table).
    //! Only makes sense if the read range is nonempty.
    virtual i64 GetTableRowIndex() const = 0;

    //! Returns #unreadRows to reader and builds data slice descriptors for read and unread data.
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor(
        TRange<NTableClient::TUnversionedRow> unreadRows) const = 0;

    // TODO(lukyan): Move this method to ISchemalessMultiChunkReader.
    virtual const NChunkClient::TDataSliceDescriptor& GetCurrentReaderDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessChunkReader)

////////////////////////////////////////////////////////////////////////////////

ISchemalessChunkReaderPtr CreateSchemalessRangeChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TSortColumns sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const NChunkClient::TReadRange& readRange,
    std::optional<int> partitionTag = std::nullopt,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr,
    std::optional<i64> virtualRowIndex = std::nullopt,
    int interruptDescriptorKeyLength = 0);

ISchemalessChunkReaderPtr CreateSchemalessLookupChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keys,
    std::optional<int> partitionTag = std::nullopt,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

ISchemalessChunkReaderPtr CreateSchemalessKeyRangesChunkReader(
    const TChunkStatePtr& chunkState,
    const TColumnarChunkMetaPtr& chunkMeta,
    TChunkReaderConfigPtr config,
    TChunkReaderOptionsPtr options,
    NChunkClient::IChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TSortColumns& sortColumns,
    const std::vector<TString>& omittedInaccessibleColumns,
    const TColumnFilter& columnFilter,
    const TSharedRange<TLegacyKey>& keyPrefixes,
    std::optional<int> partitionTag = std::nullopt,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
