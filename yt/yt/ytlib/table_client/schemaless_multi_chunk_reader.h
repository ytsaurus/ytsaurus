#pragma once

#include "public.h"
#include "schemaless_chunk_reader.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessMultiChunkReader
    : public virtual NChunkClient::IReaderBase
    , public ISchemalessChunkReader
{
    //! Return the index of the next, unread row.
    virtual i64 GetSessionRowIndex() const = 0;

    //! Returns the row count readable with this reader.
    //! May change over time and finally converges to actually read row count.
    virtual i64 GetTotalRowCount() const = 0;

    //! Interrupts the reader, notifies the consumer via end of stream in Read() method.
    virtual void Interrupt() = 0;

    //! Should be called only after successful call to #Read()
    //! if caller wants to skip remaining rows from the current data slice.
    //! To wait for readiness after this call, use #GetReadyEvent().
    virtual void SkipCurrentReader() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessMultiChunkReader)

////////////////////////////////////////////////////////////////////////////////

struct ReaderInterruptionOptions
{
    bool IsInterruptible;
    int InterruptDescriptorKeyLength;

    static ReaderInterruptionOptions InterruptibleWithEmptyKey();
    static ReaderInterruptionOptions InterruptibleWithKeyLength(int descriptorKeyLength);
    static ReaderInterruptionOptions NonInterruptible();
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSequentialMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TSharedRange<TUnversionedRow> hintKeyPrefixes,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    ReaderInterruptionOptions interruptionOptions,
    const TColumnFilter& columnFilter = {},
    std::optional<int> partitionTag = std::nullopt,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessParallelMultiReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    TSharedRange<TUnversionedRow> hintKeysUnused,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    ReaderInterruptionOptions interruptionOptions,
    const TColumnFilter& columnFilter = {},
    std::optional<int> partitionTag = std::nullopt,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessMergingMultiChunkReader(
    TTableReaderConfigPtr config,
    TTableReaderOptionsPtr options,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const NChunkClient::TDataSliceDescriptor& dataSliceDescriptor,
    TNameTablePtr nameTable,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    NChunkClient::IMultiReaderMemoryManagerPtr multiReaderMemoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateAppropriateSchemalessMultiChunkReader(
    const TTableReaderOptionsPtr& options,
    const TTableReaderConfigPtr& config,
    NChunkClient::TChunkReaderHostPtr chunkReaderHost,
    TTableReadSpec& tableReadSpec,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    bool unordered,
    const TNameTablePtr& nameTable,
    const TColumnFilter& columnFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
