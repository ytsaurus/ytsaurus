#pragma once

#include "evaluation_helpers.h"

#include <library/cpp/yt/memory/chunked_memory_pool_allocator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
    requires std::is_trivially_copyable_v<T>
class TVectorOverMemoryChunkProvider
{
public:
    TVectorOverMemoryChunkProvider(
        TRefCountedTypeCookie cookie,
        IMemoryChunkProviderPtr memoryChunkProvider);

    void PushBack(T value);
    i64 Size() const;

    T& operator[](i64 index);
    const T& operator[](i64 index) const;

    T* Begin();
    T* End();

    bool Empty() const;

private:
    static inline constexpr i64 MinCapacity = 1024;

    i64 Capacity() const;

    i64 Size_ = 0;
    const IMemoryChunkProviderPtr Provider_;
    TRefCountedTypeCookie Cookie_;
    std::unique_ptr<TAllocationHolder> DataHolder_;
};

////////////////////////////////////////////////////////////////////////////////

class TTopCollectorBase
{
public:
    virtual ~TTopCollectorBase() = default;

    using TComparerFunction = NWebAssembly::TCompartmentFunction<TComparerFunction>;

    TTopCollectorBase(
        i64 limit,
        TComparerFunction comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    // Returns a pointer to the captured row if |row| was added.
    const TPIValue* AddRow(const TPIValue* row);

    std::vector<const TPIValue*> GetRows() const;

protected:
    virtual void OnInsert(const TPIValue* insertedRow) = 0;
    virtual void OnEvict(const TPIValue* evictedRow) = 0;

private:
    struct TRowAndBuffer
    {
        TPIValue* Row = nullptr;
        i64 ContextIndex = -1;
    };

    class TMinComparer
    {
    public:
        explicit TMinComparer(TComparerFunction comparer);

        bool operator()(const TRowAndBuffer& lhs, const TRowAndBuffer& rhs) const;
        bool operator()(const TPIValue* lhs, const TPIValue* rhs) const;

    private:
        const TComparerFunction Comparer_;
    };

    class TMaxComparer
    {
    public:
        explicit TMaxComparer(TComparerFunction comparer);

        bool operator()(const TRowAndBuffer& lhs, const TRowAndBuffer& rhs) const;
        bool operator()(const TPIValue* lhs, const TPIValue* rhs) const;

    private:
        const TComparerFunction Comparer_;
    };

    void AccountGarbage(const TPIValue* row);
    void CollectGarbageAndAllocateNewContextIfNeeded();
    TRowAndBuffer Capture(const TPIValue* row, TPIValue* destination);

    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    const TMinComparer MinComparer_;
    const TMaxComparer MaxComparer_;
    const size_t RowSize_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;
    const i64 Limit_;

    TExpressionContext RowsContext_;

    std::vector<TExpressionContext> StringLikeValueContexts_;
    std::vector<int> StringLikeValueEmptyContextIds_;

    TVectorOverMemoryChunkProvider<TRowAndBuffer> Heap_;
};

////////////////////////////////////////////////////////////////////////////////

class TTopCollector
    : public TTopCollectorBase
{
public:
    TTopCollector(
        i64 limit,
        TComparerFunction comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    void OnInsert(const TPIValue* /*insertedRow*/) override;
    void OnEvict(const TPIValue* /*evictedRow*/) override;
};

////////////////////////////////////////////////////////////////////////////////

class TTopCollectorWithHashMap
    : public TTopCollectorBase
{
public:
    TTopCollectorWithHashMap(
        i64 limit,
        TComparerFunction comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider,
        TLookupRows* const hashMap);

    void OnInsert(const TPIValue* insertedRow) override;
    void OnEvict(const TPIValue* evictedRow) override;

private:
    TLookupRows* const HashMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define TOP_COLLECTOR_INL_H
#include "top_collector-inl.h"
#undef TOP_COLLECTOR_INL_H
