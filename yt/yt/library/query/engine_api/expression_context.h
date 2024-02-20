#pragma once

#include "public.h"

#include <yt/yt/library/web_assembly/api/memory_pool.h>
#include <yt/yt/library/web_assembly/api/pointer.h>

#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExpressionContext
    : public TNonCopyable
{
public:
    TExpressionContext();
    explicit TExpressionContext(TRowBufferPtr rowBuffer);

    ~TExpressionContext() = default;
    TExpressionContext(TExpressionContext&& other);
    TExpressionContext& operator=(TExpressionContext&& other);

    void Clear();
    void ClearWebAssemblyPool();
    i64 GetSize() const;
    i64 GetCapacity() const;

    char* AllocateUnaligned(size_t byteCount, NWebAssembly::EAddressSpace where);
    char* AllocateAligned(size_t byteCount, NWebAssembly::EAddressSpace where);

    TRowBufferPtr GetRowBuffer() const;

private:
    NTableClient::TRowBufferPtr HostPool_;
    NWebAssembly::TWebAssemblyMemoryPool WebAssemblyPool_;
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
