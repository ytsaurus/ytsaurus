#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/varint.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/actions/future.h>

#include <variant>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkRef
{
    NChunkClient::TChunkId ChunkId;
    i64 HunkCount = 0;
    i64 TotalHunkLength = 0;
};

void ToProto(NProto::THunkChunkRef* protoRef, const THunkChunkRef& ref);
void FromProto(THunkChunkRef* ref, const NProto::THunkChunkRef& protoRef);

void Serialize(const THunkChunkRef& ref, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf spec);
TString ToString(const THunkChunkRef& ref);

////////////////////////////////////////////////////////////////////////////////

//! Every hunk written to a hunk chunk is prepended with this header.
//! Its size is not accounted in hunk ref length.
struct THunkPayloadHeader
{
    TChecksum Checksum;
};

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
    virtual std::atomic<i64>& SkippedDataWeight() = 0;

    virtual std::atomic<int>& InlineValueCount() = 0;
    virtual std::atomic<int>& RefValueCount() = 0;

    virtual std::atomic<int>& BackendRequestCount() = 0;
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

    void Increment(const IHunkChunkReaderStatisticsPtr& statistics);

private:
    NProfiling::TCounter DataWeight_;
    NProfiling::TCounter SkippedDataWeight_;

    NProfiling::TCounter InlineValueCount_;
    NProfiling::TCounter RefValueCount_;

    NProfiling::TCounter BackendRequestCount_;

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

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EHunkValueTag, ui8,
    ((Inline)   (0))
    ((LocalRef) (1))
    ((GlobalRef)(2))
);

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
 *  * blockIndex: varuint32
 *  * blockOffset: varuint64
 *  * length: varuint64
 */

struct TInlineHunkValue
{
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
    int BlockIndex;
    i64 BlockOffset;
    i64 Length;
};

using THunkValue = std::variant<
    TInlineHunkValue,
    TLocalRefHunkValue,
    TGlobalRefHunkValue
>;

////////////////////////////////////////////////////////////////////////////////

constexpr auto InlineHunkHeaderSize =
    sizeof(ui8);       // tag

constexpr auto MaxLocalHunkRefSize =
    sizeof(ui8) +      // tag
    MaxVarUint32Size + // chunkIndex
    MaxVarUint64Size + // length
    MaxVarUint32Size + // blockIndex
    MaxVarUint64Size;  // blockOffset

constexpr auto MaxGlobalHunkRefSize =
    sizeof(ui8) +                    // tag
    sizeof(NChunkClient::TChunkId) + // chunkId
    MaxVarUint64Size +               // length
    MaxVarUint32Size +               // blockIndex
    MaxVarUint64Size;                // blockOffset

////////////////////////////////////////////////////////////////////////////////

TRef WriteHunkValue(TChunkedMemoryPool* pool, const TInlineHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TLocalRefHunkValue& value);
TRef WriteHunkValue(TChunkedMemoryPool* pool, const TGlobalRefHunkValue& value);

size_t GetInlineHunkValueSize(const TInlineHunkValue& value);
TRef WriteHunkValue(char* ptr, const TInlineHunkValue& value);

THunkValue ReadHunkValue(TRef input);

void GlobalizeHunkValues(
    TChunkedMemoryPool* pool,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TMutableVersionedRow row);

//! Reads hunks in schemaful #rows and decodes them (updating #rows in-place).
TFuture<TSharedRange<TMutableUnversionedRow>> DecodeHunksInSchemafulUnversionedRows(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableUnversionedRow> rows);

//! A versioned counterpart of #ReadAndDecodeHunksInSchemafulRows.
TFuture<TSharedRange<TMutableVersionedRow>> DecodeHunksInVersionedRows(
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NChunkClient::TClientChunkReadOptions options,
    TSharedRange<TMutableVersionedRow> rows);

//! Constructs a writer performing hunk encoding.
//! Encoded rows are written to #underlying, hunks go to #hunkChunkPayloadWriter.
//! If #schema does not contain hunk columns then #underlying is returned as is.
IVersionedChunkWriterPtr CreateHunkEncodingVersionedWriter(
    IVersionedChunkWriterPtr underlying,
    TTableSchemaPtr schema,
    IHunkChunkPayloadWriterPtr hunkChunkPayloadWriter,
    IHunkChunkWriterStatisticsPtr hunkChunkWriterStatistics);

//! Constructs a schemaful reader replacing hunk refs with their content
//! (obtained by reading it via #chunkFragmentReader).
//! If #schema does not contain hunk columns then #underlying is returned as is.
ISchemafulUnversionedReaderPtr CreateHunkDecodingSchemafulReader(
    const TTableSchemaPtr& schema,
    const TColumnFilter& columnFilter,
    TBatchHunkReaderConfigPtr config,
    ISchemafulUnversionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    NChunkClient::TClientChunkReadOptions options);

//! Schemaless counterparts of #CreateHunkDecodingSchemafulReader.
ISchemalessUnversionedReaderPtr CreateHunkDecodingSchemalessReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessUnversionedReaderPtr underlying,
    NChunkClient::IChunkFragmentReaderPtr chunkFragmentReader,
    TTableSchemaPtr schema,
    NChunkClient::TClientChunkReadOptions options);
ISchemalessChunkReaderPtr CreateHunkDecodingSchemalessChunkReader(
    TBatchHunkReaderConfigPtr config,
    ISchemalessChunkReaderPtr underlying,
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
    TTableSchemaPtr schema,
    THashSet<NChunkClient::TChunkId> hunkChunkIdsToForceInline,
    NChunkClient::TClientChunkReadOptions options);

////////////////////////////////////////////////////////////////////////////////

struct IHunkChunkPayloadWriter
    : public virtual TRefCounted
{
    //! Enqueues a given #payload for writing.
    //! Returns |(blockIndex, blockOffset, ready)| where #ready indicates if the caller must wait on
    //! #GetReadyEvent before proceeding any further.
    virtual std::tuple<int, i64, bool> WriteHunk(TRef payload) = 0;

    //! Returns |true| if some hunks were added via #WriteHunk.
    virtual bool HasHunks() const = 0;

    //! See #WriteHunk.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Returns the future that is set when the chunk becomes open.
    //! At least one hunk must have been added via #WriteHunk prior to this call.
    virtual TFuture<void> GetOpenFuture() = 0;

    //! Flushes and closes the writer (both this and the underlying one).
    //! If no hunks were added via #WriteHunk, returns #VoidFuture.
    virtual TFuture<void> Close() = 0;

    //! Returns the chunk meta. The chunk must be already closed, see #Close.
    virtual NChunkClient::TDeferredChunkMetaPtr GetMeta() const = 0;

    //! Returns the chunk id. The chunk must be already open, see #GetOpenFuture.
    virtual NChunkClient::TChunkId GetChunkId() const = 0;

    //! Returns the chunk data statistics.
    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkChunkPayloadWriter)

IHunkChunkPayloadWriterPtr CreateHunkChunkPayloadWriter(
    THunkChunkPayloadWriterConfigPtr config,
    NChunkClient::IChunkWriterPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
