#pragma once

#include "public.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Holds data for a bunch of rows.
/*!
 *  Internally, implemented as a pair of chunked pools: one for aligned
 *  data (row headers and row values) and another for unaligned data (string values).
 */
class TRowBuffer
{
public:
    explicit TRowBuffer(
        i64 alignedPoolChunkSize = TChunkedMemoryPool::DefaultChunkSize,
        i64 unalignedPoolChunkSize = TChunkedMemoryPool::DefaultChunkSize,
        double maxPoolSmallBlockRatio = TChunkedMemoryPool::DefaultMaxSmallBlockSizeRatio);

    TChunkedMemoryPool* GetAlignedPool();
    const TChunkedMemoryPool* GetAlignedPool() const;

    TChunkedMemoryPool* GetUnalignedPool();
    const TChunkedMemoryPool* GetUnalignedPool() const;

    TVersionedValue Capture(const TVersionedValue& value);
    TUnversionedValue Capture(const TUnversionedValue& value);

    TUnversionedRow Capture(TUnversionedRow row);
    std::vector<TUnversionedRow> Capture(const std::vector<TUnversionedRow>& rows);

    i64 GetSize() const;
    i64 GetCapacity() const;

    void Clear();

private:
    TChunkedMemoryPool AlignedPool_;
    TChunkedMemoryPool UnalignedPool_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
