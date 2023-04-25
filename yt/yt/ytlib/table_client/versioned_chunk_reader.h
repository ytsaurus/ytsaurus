#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/versioned_reader.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/linear_probe.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t RowBufferCapacity = 1000;

////////////////////////////////////////////////////////////////////////////////

//! Creates a versioned chunk reader for a given range of rows.
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange = {},
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr,
    IInvokerPtr sessionInvoker = nullptr);

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    TLegacyOwningKey lowerLimit,
    TLegacyOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr,
    IInvokerPtr sessionInvoker = nullptr);

//! Creates a versioned chunk reader for a given set of keys.
/*!
 *  Number of rows readable via this reader is equal to the number of passed keys.
 *  If some key is missing, a null row is returned for it.
*/
IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    NChunkClient::IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TLegacyKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const NChunkClient::TChunkReaderMemoryManagerPtr& memoryManager = nullptr);

////////////////////////////////////////////////////////////////////////////////

//! Asynchronously reads all requested rows via #underlyingReader
//! and returns wire encoded rowset compressed with #codecId.
class TVersionedRowsetReader
    : public TRefCounted
{
public:
    TVersionedRowsetReader(
        int rowCount,
        IVersionedReaderPtr underlyingReader,
        NCompression::ECodec codecId,
        IInvokerPtr invoker);

    TFuture<TSharedRef> ReadRowset();

private:
    const int TotalRowCount_;
    const IVersionedReaderPtr UnderlyingReader_;
    NCompression::ICodec* const Codec_;
    const IInvokerPtr Invoker_;
    const std::unique_ptr<IWireProtocolWriter> WireWriter_ = CreateWireProtocolWriter();

    const TPromise<TSharedRef> RowsetPromise_ = NewPromise<TSharedRef>();

    int ReadRowCount_ = 0;


    void DoReadRowset(const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TVersionedRowsetReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
