#pragma once

#include "public.h"
#include "unversioned_row.h"
#include "versioned_row.h"

#include <yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultRowBufferPoolTag { };

//! Holds data for a bunch of rows.
/*!
 *  Acts as a ref-counted wrapped around TChunkedMemoryPool plus a bunch
 *  of helpers.
 */
class TRowBuffer
    : public TIntrinsicRefCounted
{
public:
    TRowBuffer() = default;

    TRowBuffer(
        TRefCountedTypeCookie tagCookie,
        IMemoryChunkProviderPtr chunkProvider,
        size_t startChunkSize = TChunkedMemoryPool::DefaultStartChunkSize)
        : Pool_(
            tagCookie,
            std::move(chunkProvider),
            startChunkSize)
    { }

    template <class TTag>
    explicit TRowBuffer(
        TTag,
        size_t startChunkSize = TChunkedMemoryPool::DefaultStartChunkSize)
        : Pool_(
            GetRefCountedTypeCookie<TTag>(),
            startChunkSize)
    { }

    template <class TTag>
    TRowBuffer(
        TTag,
        IMemoryChunkProviderPtr chunkProvider)
        : Pool_(
            GetRefCountedTypeCookie<TTag>(),
            std::move(chunkProvider))
    { }

    TChunkedMemoryPool* GetPool();

    TMutableUnversionedRow AllocateUnversioned(int valueCount);
    TMutableVersionedRow AllocateVersioned(
        int keyCount,
        int valueCount,
        int writeTimestampCount,
        int deleteTimestampCount);

    void Capture(TUnversionedValue* value);
    TVersionedValue Capture(const TVersionedValue& value);
    TUnversionedValue Capture(const TUnversionedValue& value);

    TMutableUnversionedRow Capture(TUnversionedRow row, bool deep = true);
    TMutableUnversionedRow Capture(const TUnversionedValue* begin, int count, bool deep = true);
    std::vector<TMutableUnversionedRow> Capture(TRange<TUnversionedRow> rows, bool deep = true);

    //! Captures the row applying #idMapping to value ids and placing values to the proper positions.
    //! The returned row is schemaful.
    //! Skips values that map to negative ids with via #idMapping.
    TMutableUnversionedRow CaptureAndPermuteRow(
        TUnversionedRow row,
        const TTableSchema& tableSchema,
        const TNameTableToSchemaIdMapping& idMapping,
        std::vector<bool>* columnPresenceBuffer);

    //! Captures the row applying #idMapping to value ids.
    //! Skips values that map to negative ids with via #idMapping.
    TMutableVersionedRow CaptureAndPermuteRow(
        TVersionedRow row,
        const TTableSchema& tableSchema,
        const TNameTableToSchemaIdMapping& idMapping,
        std::vector<bool>* columnPresenceBuffer);

    i64 GetSize() const;
    i64 GetCapacity() const;

    void Clear();
    void Purge();

private:
    TChunkedMemoryPool Pool_;

};

DEFINE_REFCOUNTED_TYPE(TRowBuffer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
