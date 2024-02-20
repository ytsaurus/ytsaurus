#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/dictionary_compression_session.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TColumnDictionaryCompressor
{
    NCompression::IDictionaryCompressorPtr Compressor = nullptr;
    i64 CompressedSamplesSize = 0;
};

struct TRowDictionaryCompressor
{
    NChunkClient::TChunkId DictionaryId = NChunkClient::NullChunkId;
    THashMap<int, TColumnDictionaryCompressor> ColumnCompressors;
};

using TRowDictionaryCompressors = TEnumIndexedArray<
    NTableClient::EDictionaryCompressionPolicy,
    TRowDictionaryCompressor>;

using TRowDictionaryDecompressor = THashMap<int, NCompression::IDictionaryDecompressorPtr>;

////////////////////////////////////////////////////////////////////////////////

struct ICompressionDictionaryManager
    : public TRefCounted
{
    virtual NTableClient::IDictionaryCompressionFactoryPtr CreateTabletDictionaryCompressionFactory() const = 0;

    virtual TFuture<THashMap<NChunkClient::TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        const NChunkClient::TClientChunkReadOptions& /*chunkReadOptions*/,
        const THashSet<NChunkClient::TChunkId>& /*dictionaryIds*/) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompressionDictionaryManager)

////////////////////////////////////////////////////////////////////////////////

ICompressionDictionaryManagerPtr CreateCompressionDictionaryManager();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
