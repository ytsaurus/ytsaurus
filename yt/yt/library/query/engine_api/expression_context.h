#pragma once

#include "public.h"

#include <yt/yt/library/web_assembly/api/memory_pool.h>

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExpressionContext
    : public TNonCopyable
{
public:
    explicit TExpressionContext(TRowBufferPtr rowBuffer);

    ~TExpressionContext() = default;
    TExpressionContext(TExpressionContext&& other);
    TExpressionContext& operator=(TExpressionContext&& other);

    void Clear();
    i64 GetSize() const;
    i64 GetCapacity() const;

    char* AllocateUnaligned(size_t byteCount);
    char* AllocateAligned(size_t byteCount);
    NTableClient::TMutableUnversionedRow AllocateUnversioned(int valueCount);

    TRowBufferPtr GetRowBuffer() const;

    NTableClient::TMutableUnversionedRow CaptureRow(
        NTableClient::TUnversionedRow row,
        bool captureValues = true);
    NTableClient::TMutableUnversionedRow CaptureRow(
        NTableClient::TUnversionedValueRange values,
        bool captureValues = true);

    void CaptureValues(NTableClient::TMutableUnversionedRow row);

private:
    NTableClient::TRowBufferPtr RowBuffer_;
};

template <class TTag = NTableClient::TDefaultRowBufferPoolTag>
TExpressionContext MakeExpressionContext(
    TTag,
    IMemoryUsageTrackerPtr memoryTracker,
    size_t startChunkSize = TChunkedMemoryPool::DefaultStartChunkSize);

template <class TTag>
TExpressionContext MakeExpressionContext(
    TTag,
    IMemoryChunkProviderPtr chunkProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define EXPRESSION_CONTEXT_INL_H_
#include "expression_context-inl.h"
#undef EXPRESSION_CONTEXT_INL_H_
