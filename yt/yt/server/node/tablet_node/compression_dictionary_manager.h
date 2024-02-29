#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/dictionary_compression_session.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TColumnDictionaryCompressor
{
    NCompression::IDictionaryCompressorPtr Compressor;
    i64 CompressedSamplesSize = 0;
};

struct TRowDictionaryCompressor
{
    NChunkClient::TChunkId DictionaryId;
    THashMap<int, TColumnDictionaryCompressor> ColumnCompressors;
};

using TRowDictionaryCompressors = TEnumIndexedArray<
    NTableClient::EDictionaryCompressionPolicy,
    TRowDictionaryCompressor>;

using TRowDictionaryDecompressor = THashMap<int, NCompression::IDictionaryDecompressorPtr>;

////////////////////////////////////////////////////////////////////////////////

struct ICompressionDictionaryManager
    : public virtual TRefCounted
{
    virtual NTableClient::IDictionaryCompressionFactoryPtr CreateTabletDictionaryCompressionFactory(
        const TTabletSnapshotPtr& tabletSnapshot) = 0;

    virtual TFuture<TRowDictionaryCompressors> MaybeGetCompressors(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions) = 0;

    virtual TFuture<THashMap<NChunkClient::TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const THashSet<NChunkClient::TChunkId>& dictionaryIds) = 0;

    virtual void OnDynamicConfigChanged(const TSlruCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICompressionDictionaryManager)

////////////////////////////////////////////////////////////////////////////////

ICompressionDictionaryManagerPtr CreateCompressionDictionaryManager(
    TSlruCacheConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
