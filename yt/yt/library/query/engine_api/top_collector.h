#pragma once

#include "evaluation_helpers.h"

namespace NYT::NQueryClient {

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

    TExpressionContext DataContext_;
    TMutableRange<TPIValue> Data_;

    std::vector<TExpressionContext> Contexts_;
    std::vector<int> EmptyContextIds_;
    std::vector<TRowAndBuffer> Heap_;
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

    virtual void OnInsert(const TPIValue* /*insertedRow*/) override;
    virtual void OnEvict(const TPIValue* /*evictedRow*/) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
