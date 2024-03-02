#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/api/internal_client.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/coding/varint.h>

#include <library/cpp/yt/memory/ref.h>

#include <variant>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkRef
{
    NChunkClient::TChunkId ChunkId;
    NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
    i64 HunkCount = 0;
    i64 TotalHunkLength = 0;
    NChunkClient::TChunkId CompressionDictionaryId;
};

void ToProto(NProto::THunkChunkRef* protoRef, const THunkChunkRef& ref);
void FromProto(THunkChunkRef* ref, const NProto::THunkChunkRef& protoRef);

void Serialize(const THunkChunkRef& ref, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf spec);
TString ToString(const THunkChunkRef& ref);

////////////////////////////////////////////////////////////////////////////////

struct THunkChunksInfo
{
    NElection::TCellId CellId;
    NTabletClient::TTabletId HunkTabletId;
    NHydra::TRevision MountRevision;

    THashMap<NChunkClient::TChunkId, THunkChunkRef> HunkChunkRefs;
};

void ToProto(NTabletClient::NProto::THunkChunksInfo* protoRef, const THunkChunksInfo& ref);
void FromProto(THunkChunksInfo* ref, const NTabletClient::NProto::THunkChunksInfo& protoRef);

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkMeta
{
    NChunkClient::TChunkId ChunkId;
    std::vector<i64> BlockSizes;
};

void ToProto(NProto::THunkChunkMeta* protoMeta, const THunkChunkMeta& meta);
void FromProto(THunkChunkMeta* meta, const NProto::THunkChunkMeta& protoMeta);

////////////////////////////////////////////////////////////////////////////////

//! Every hunk written to a hunk chunk is prepended with this header.
//! Its size is not accounted in hunk ref length.
struct THunkPayloadHeader
{
    TChecksum Checksum;
};

////////////////////////////////////////////////////////////////////////////////

TSharedRef GetAndValidateHunkPayload(
    TSharedRef fragment,
    const NChunkClient::IChunkFragmentReader::TChunkFragmentRequest& request);

////////////////////////////////////////////////////////////////////////////////

/*
 * These are per-column hunk chunk-related statistics that are profiled
 * when hunk columnar profiling of a table is enabled.
 *
 * Inline* represents inline hunk values.
 * Ref* represents local and global ref hunk values.
 *
 * *Count represents number of accesses to the column.
 * *Weight represents total weight of accessed blobs.
*/
struct TColumnarHunkChunkStatistics
{
    i64 InlineValueCount = 0;
    i64 RefValueCount = 0;

    i64 InlineValueWeight = 0;
    i64 RefValueWeight = 0;
};

struct IHunkChunkStatisticsBase
    : public virtual TRefCounted
{
    virtual bool HasColumnarStatistics() const = 0;
    virtual TColumnarHunkChunkStatistics GetColumnarStatistics(int columnId) const = 0;
    virtual void UpdateColumnarStatistics(
        int columnId,
        const TColumnarHunkChunkStatistics& statistics) = 0;
};

struct IHunkChunkReaderStatistics
    : public virtual IHunkChunkStatisticsBase
{
    virtual const NChunkClient::TChunkReaderStatisticsPtr& GetChunkReaderStatistics() const = 0;

    virtual std::atomic<i64>& DataWeight() = 0;
    virtual std::atomic<i64>& DroppedDataWeight() = 0;

    virtual std::atomic<int>& InlineValueCount() = 0;
    virtual std::atomic<int>& RefValueCount() = 0;

    virtual std::atomic<int>& BackendReadRequestCount() = 0;
    virtual std::atomic<int>& BackendHedgingReadRequestCount() = 0;
    virtual std::atomic<int>& BackendProbingRequestCount() = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkReaderStatistics)

IHunkChunkReaderStatisticsPtr CreateHunkChunkReaderStatistics(
    bool enableHunkColumnarProfiling,
    const TTableSchemaPtr& schema);

struct IHunkChunkWriterStatistics
    : public virtual IHunkChunkStatisticsBase
{ };

DEFINE_REFCOUNTED_TYPE(IHunkChunkWriterStatistics)

IHunkChunkWriterStatisticsPtr CreateHunkChunkWriterStatistics(
    bool enableHunkColumnarProfiling,
    const TTableSchemaPtr& schema);

////////////////////////////////////////////////////////////////////////////////

struct TColumnarHunkChunkStatisticsCounters
{
    NProfiling::TCounter InlineValueCount;
    NProfiling::TCounter RefValueCount;

    NProfiling::TCounter InlineValueWeight;
    NProfiling::TCounter RefValueWeight;
};

class THunkChunkStatisticsCountersBase
{
public:
    THunkChunkStatisticsCountersBase() = default;

    THunkChunkStatisticsCountersBase(
        const NProfiling::TProfiler& profiler,
        const TTableSchemaPtr& schema);

    template <class IStatisticsPtr>
    void IncrementColumnar(const IStatisticsPtr& statistics);

private:
    THashMap<int, TColumnarHunkChunkStatisticsCounters> ColumnIdToCounters_;
};

class THunkChunkReaderCounters
    : public THunkChunkStatisticsCountersBase
{
public:
    THunkChunkReaderCounters() = default;

    explicit THunkChunkReaderCounters(
        const NProfiling::TProfiler& profiler,
        const TTableSchemaPtr& schema);

    void Increment(
        const IHunkChunkReaderStatisticsPtr& statistics,
        bool failed);

private:
    NProfiling::TCounter DataWeight_;
    NProfiling::TCounter DroppedDataWeight_;

    NProfiling::TCounter InlineValueCount_;
    NProfiling::TCounter RefValueCount_;

    NProfiling::TCounter BackendReadRequestCount_;
    NProfiling::TCounter BackendHedgingReadRequestCount_;
    NProfiling::TCounter BackendProbingRequestCount_;

    NChunkClient::TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters_;
};

class THunkChunkWriterCounters
    : public THunkChunkStatisticsCountersBase
{
public:
    THunkChunkWriterCounters() = default;

    THunkChunkWriterCounters(
        const NProfiling::TProfiler& profiler,
        const TTableSchemaPtr& schema);

    void Increment(
        const IHunkChunkWriterStatisticsPtr& statistics,
        const NChunkClient::NProto::TDataStatistics& dataStatistics,
        const NChunkClient::TCodecStatistics& codecStatistics,
        int replicationFactor);

private:
    bool HasHunkColumns_ = false;
    NChunkClient::TChunkWriterCounters ChunkWriterCounters_;
};

////////////////////////////////////////////////////////////////////////////////

/*
 *  Hunk value format
 *  =================
 *
 *  Empty values are encoded as-is.
 *
 *  Non-empty values have the following layout:
 *  * tag: ui8
 *
 *  1) tag == EHunkValueTag::Inline
 *  Value payload is being stored inline.
 *  * payload: char[N]
 *
 *  2) tag == EHunkValueTag::LocalRef
 *  Value payload is moved to a hunk chunk and is referenced by index in THunkChunkRefsExt.
 *  * chunkIndex: varuint32
 *  * blockIndex: varuint32
 *  * blockOffset: varuint64
 *  * length: varuint64
 *
 *  3) tag == EHunkValueTag::GlobalRef
 *  Value payload is moved to a hunk chunk and is referenced by chunk id.
 *  * chunkId: TChunkId
 *  * erasureCodec: NErasure::ECodec(varint32) if chunkId is erasure
 *  * blockIndex: varuint32
 *  * blockOffset: varuint64
 *  * blockSize: varuint64 if chunkId is erasure
 *  * length: varuint64
 *
 *  4) tag == EHunkValueTag::CompressedInline
 *  Compressed value payload is being stored inline.
 *  * compressionDictionaryId: TChunkId
 *  * payload: char[N]
 */

struct TInlineHunkValue
{
    TRef Payload;
};

struct TCompressedInlineRefHunkValue
{
    NChunkClient::TChunkId CompressionDictionaryId;
    TRef Payload;
};

struct TLocalRefHunkValue
{
    int ChunkIndex;
    int BlockIndex;
    i64 BlockOffset;
    i64 Length;
};

struct TGlobalRefHunkValue
{
    NChunkClient::TChunkId ChunkId;
    NErasure::ECodec ErasureCodec;
    int BlockIndex;
    i64 BlockOffset;
    std::optional<i64> BlockSize;
    i64 Length;
    NChunkClient::TChunkId CompressionDictionaryId;
};

using THunkValue = std::variant<
    TInlineHunkValue,
    TLocalRefHunkValue,
    TGlobalRefHunkValue,
    TCompressedInlineRefHunkValue
>;

////////////////////////////////////////////////////////////////////////////////

constexpr auto MaxLocalHunkRefSize =
    sizeof(ui8) +      // tag
    MaxVarUint32Size + // chunkIndex
    MaxVarUint64Size + // length
    MaxVarUint32Size + // blockIndex
    MaxVarUint64Size;  // blockOffset

constexpr auto MaxGlobalHunkRefSize =
    sizeof(ui8) +                    // tag
    sizeof(NChunkClient::TChunkId) + // chunkId
    sizeof(NErasure::ECodec)       + // erasureCodec
    MaxVarUint64Size +               // length
    MaxVarUint32Size +               // blockIndex
    MaxVarUint64Size +               // blockOffset
    MaxVarUint64Size +               // blockSize
    sizeof(NChunkClient::TChunkId);  // compressionDictionaryId

////////////////////////////////////////////////////////////////////////////////

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TCompressedInlineRefHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TGlobalRefHunkValue& value);

THunkValue ReadHunkValue(TRef input);

bool TryDecodeInlineHunkValue(TUnversionedValue* value);

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TMutableVersionedRow row);

void GlobalizeHunkValueAndSetHunkFlag(
    TChunkedMemoryPool* pool,
    const std::vector<THunkChunkRef>& hunkChunkRefs,
    const std::vector<THunkChunkMeta>& hunkChunkMetas,
    TUnversionedValue* value);
void GlobalizeHunkValuesAndSetHunkFlag(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    bool* columnHunkFlags,
    TMutableVersionedRow row);

//! Reads hunks in schemaful #rows and decodes them (updating #rows in-place).
TFuture<TSharedRange<TMutableUnversionedRow>> DecodeHunksInSchemafulUnversionedRows(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows);

//! A versioned counterpart of #ReadAndDecodeHunksInSchemafulRows.
TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunksInVersionedRows(
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows);

//! Constructs a writer performing hunk encoding.
//! Encoded rows are written to #underlying, hunks go to #hunkChunkPayloadWriter.
//! If #schema does not contain hunk columns then #underlying is returned as is.
IVersionedChunkWriterPtr CreateHunkEncodingVersionedWriter(
    IVersionedChunkWriterPtr underlying,
    TTableSchemaPtr schema,
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter,
    IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions);

//! Constructs a schemaful reader replacing hunk refs with their content
//! (obtained by reading it via #chunkFragmentReader).
//! If #schema does not contain hunk columns then #underlying is returned as is.
ISchemafulUnversionedReaderPtr CreateHunkDecodingSchemafulReader(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    TBatchHunkReaderConfigPtr config,
    ISchemafulUnversionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    NChunkClient::TClientChunkReadOptions options);

//! Schemaless counterparts of #CreateHunkDecodingSchemafulReader.
ISchemalessChunkReaderPtr CreateHunkDecodingSchemalessChunkReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessChunkReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options);
ISchemalessMultiChunkReaderPtr CreateHunkDecodingSchemalessMultiChunkReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessMultiChunkReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options);

//! Constructs a reader replacing hunk refs with inline hunks
//! (obtained by fetching payloads via #chunkFragmentReader).
//! This inlining happens for hunks smaller than |MaxInlineHunkSize|
//! and is also forced for all hunks contained in chunks with ids from #hunkChunkIdsToForceInline.
//! If #schema does not contain hunk columns then #underlying is returned as is.
IVersionedReaderPtr CreateHunkInliningVersionedReader(
    TBatchHunkReaderConfigPtr config,
    IVersionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    IDictionaryCompressionFactoryPtr dictionaryCompressionFactory,
    TTableSchemaPtr schema,
    THashSet<NChunkClient::TChunkId> hunkChunkIdsToForceInline,
    NChunkClient::TClientChunkReadOptions options);

////////////////////////////////////////////////////////////////////////////////

struct IHunkChunkPayloadWriter
    : public virtual TRefCounted
{
    //! Opens the writer. Must be the first call to the writer.
    virtual TFuture<void> Open() = 0;

    //! Enqueues a given #payload for writing.
    //! Returns |(blockIndex, blockOffset, ready)| where #ready indicates if the caller must wait on
    //! #GetReadyEvent before proceeding any further.
    virtual std::tuple<int, i64, bool> WriteHunk(TRef payload, i64 dataWeight) = 0;

    //! Returns |true| if some hunks were added via #WriteHunk.
    virtual bool HasHunks() const = 0;

    //! See #WriteHunk.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Flushes and closes the writer (both this and the underlying one).
    //! If no hunks were added via #WriteHunk, underlying writer is cancelled.
    virtual TFuture<void> Close() = 0;

    //! Returns the chunk meta.
    virtual NChunkClient::TDeferredChunkMetaPtr GetMeta() const = 0;

    //! Returns the chunk id. The chunk must be already open, see #GetOpenFuture.
    virtual NChunkClient::TChunkId GetChunkId() const = 0;

    //! Returns the chunk erasure codec id.
    virtual NErasure::ECodec GetErasureCodecId() const = 0;

    //! Returns the chunk data statistics.
    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const = 0;

    //! Called when chunk store writer closes.
    virtual void OnParentReaderFinished(NChunkClient::TChunkId compressionDictionaryId) = 0;

    //! Returns the hunk chunk meta.
    virtual THunkChunkMeta GetHunkChunkMeta() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkPayloadWriter)

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    const TWorkloadDescriptor& workloadDescriptor,
    THunkChunkPayloadWriterConfigPtr config,
    NChunkClient::IChunkWriterPtr underlying);

////////////////////////////////////////////////////////////////////////////////

void DecodeInlineHunkInUnversionedValue(TUnversionedValue* value);

std::vector<TRef> ExtractHunks(
    TUnversionedRow row,
    TTableSchemaPtr schema);

void ReplaceHunks(
    TMutableUnversionedRow row,
    const TTableSchemaPtr& schema,
    const std::vector<NApi::THunkDescriptor>& descriptors,
    TChunkedMemoryPool* pool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
