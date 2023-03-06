#pragma once

#include "public.h"

#include <yt/yt/library/query/base/callbacks.h>

#include <yt/yt/library/query/misc/objects_holder.h>
#include <yt/yt/library/query/misc/function_context.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 RowsetProcessingSize = 1024;
constexpr i64 WriteRowsetSize = 64 * RowsetProcessingSize;
constexpr i64 MaxJoinBatchSize = 1024 * RowsetProcessingSize;

class TInterruptedIncompleteException
{ };

struct TOutputBufferTag
{ };

struct TIntermediateBufferTag
{ };

struct TPermanentBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

constexpr const size_t InitialGroupOpHashtableCapacity = 1024;

using THasherFunction = ui64(const TValue*);
using TComparerFunction = char(const TValue*, const TValue*);
using TTernaryComparerFunction = i64(const TValue*, const TValue*);

namespace NDetail {

class TGroupHasher
{
public:
    // Intentionally implicit.
    TGroupHasher(THasherFunction* ptr)
        : Ptr_(ptr)
    { }

    ui64 operator () (const TValue* row) const
    {
        return Ptr_(row);
    }

private:
    THasherFunction* Ptr_;
};

class TRowComparer
{
public:
    // Intentionally implicit.
    TRowComparer(TComparerFunction* ptr)
        : Ptr_(ptr)
    { }

    bool operator () (const TValue* a, const TValue* b) const
    {
        return a == b || a && b && Ptr_(a, b);
    }

private:
    TComparerFunction* Ptr_;
};

} // namespace NDetail

using TLookupRows = google::dense_hash_set<
    const TValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookup = google::dense_hash_map<
    const TValue*,
    std::pair<int, bool>,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookupRows = std::unordered_multiset<
    const TValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

struct TExecutionContext;

struct TSingleJoinParameters
{
    size_t KeySize;
    bool IsLeft;
    bool IsPartiallySorted;
    std::vector<size_t> ForeignColumns;
    TJoinSubqueryEvaluator ExecuteForeign;
};

struct TMultiJoinParameters
{
    TCompactVector<TSingleJoinParameters, 10> Items;
    size_t PrimaryRowSize;
    size_t BatchSize;
};

struct TMultiJoinClosure
{
    TRowBufferPtr Buffer;

    typedef google::dense_hash_set<
        TValue*,
        NDetail::TGroupHasher,
        NDetail::TRowComparer> THashJoinLookup;  // + slot after row

    std::vector<TValue*> PrimaryRows;

    struct TItem
    {
        TRowBufferPtr Buffer;
        size_t KeySize;
        TComparerFunction* PrefixEqComparer;

        THashJoinLookup Lookup;
        std::vector<TValue*> OrderedKeys;  // + slot after row
        const TValue* LastKey = nullptr;

        TItem(
            IMemoryChunkProviderPtr chunkProvider,
            size_t keySize,
            TComparerFunction* prefixEqComparer,
            THasherFunction* lookupHasher,
            TComparerFunction* lookupEqComparer);
    };

    TCompactVector<TItem, 32> Items;

    size_t PrimaryRowSize;
    size_t BatchSize;
    std::function<void(size_t)> ProcessSegment;
    std::function<bool()> ProcessJoinBatch;
};

struct TGroupByClosure
{
    TRowBufferPtr Buffer;
    TComparerFunction* PrefixEqComparer;
    TLookupRows Lookup;
    const TValue* LastKey = nullptr;
    std::vector<const TValue*> GroupedRows;
    int KeySize;
    int ValuesCount;
    bool CheckNulls;

    size_t GroupedRowCount = 0;

    TGroupByClosure(
        IMemoryChunkProviderPtr chunkProvider,
        TComparerFunction* prefixEqComparer,
        THasherFunction* groupHasher,
        TComparerFunction* groupComparer,
        int keySize,
        int valuesCount,
        bool checkNulls);

    std::function<void()> ProcessSegment;
};

struct TWriteOpClosure
{
    TRowBufferPtr OutputBuffer;

    // Rows stored in OutputBuffer
    std::vector<TRow> OutputRowsBatch;
    size_t RowSize;

    explicit TWriteOpClosure(IMemoryChunkProviderPtr chunkProvider);
};

typedef TRowBuffer TExpressionContext;

#define CHECK_STACK() (void) 0;

struct TExecutionContext
{
    ISchemafulUnversionedReaderPtr Reader;
    IUnversionedRowsetWriterPtr Writer;

    TQueryStatistics* Statistics = nullptr;

    // These limits prevent full scan.
    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();
    i64 GroupRowLimit = std::numeric_limits<i64>::max();
    i64 JoinRowLimit = std::numeric_limits<i64>::max();

    // Offset from OFFSET clause.
    i64 Offset = 0;
    // Limit from LIMIT clause.
    i64 Limit = std::numeric_limits<i64>::max();

    bool Ordered = false;
    bool IsMerge = false;

    IMemoryChunkProviderPtr MemoryChunkProvider;

    TExecutionContext()
    {
        auto context = this;
        Y_UNUSED(context);
        CHECK_STACK();
    }
};

class TTopCollector
{
public:
    TTopCollector(
        i64 limit,
        TComparerFunction* comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    std::vector<const TValue*> GetRows() const;

    void AddRow(const TValue* row);

private:
    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    class TComparer
    {
    public:
        explicit TComparer(TComparerFunction* ptr)
            : Ptr_(ptr)
        { }

        bool operator() (const std::pair<const TValue*, int>& lhs, const std::pair<const TValue*, int>& rhs) const
        {
            return (*this)(lhs.first, rhs.first);
        }

        bool operator () (const TValue* a, const TValue* b) const
        {
            return Ptr_(a, b);
        }

    private:
        TComparerFunction* const Ptr_;
    };

    TComparer Comparer_;
    size_t RowSize_;
    IMemoryChunkProviderPtr MemoryChunkProvider_;

    std::vector<TRowBufferPtr> Buffers_;
    std::vector<int> EmptyBufferIds_;
    std::vector<std::pair<const TValue*, int>> Rows_;

    std::pair<const TValue*, int> Capture(const TValue* row);

    void AccountGarbage(const TValue* row);
};

class TCGVariables
{
public:
    template <class T, class... TArgs>
    int AddOpaque(TArgs&&... args);

    void* const* GetOpaqueData() const;

    void Clear();

    int AddLiteralValue(TOwningValue value);

    TValue* GetLiteralValues() const;

private:
    TObjectsHolder Holder_;
    std::vector<void*> OpaquePointers_;
    std::vector<TOwningValue> OwningLiteralValues_;
    mutable std::unique_ptr<TValue[]> LiteralValues_;
};

typedef void (TCGQuerySignature)(const TValue*, void* const*, TExecutionContext*);
typedef void (TCGExpressionSignature)(const TValue*, void* const*, TValue*, const TValue*, TExpressionContext*);
typedef void (TCGAggregateInitSignature)(TExpressionContext*, TValue*);
typedef void (TCGAggregateUpdateSignature)(TExpressionContext*, TValue*, const TValue*);
typedef void (TCGAggregateMergeSignature)(TExpressionContext*, TValue*, const TValue*);
typedef void (TCGAggregateFinalizeSignature)(TExpressionContext*, TValue*, const TValue*);

using TCGQueryCallback = TCallback<TCGQuerySignature>;
using TCGExpressionCallback = TCallback<TCGExpressionSignature>;
using TCGAggregateInitCallback = TCallback<TCGAggregateInitSignature>;
using TCGAggregateUpdateCallback = TCallback<TCGAggregateUpdateSignature>;
using TCGAggregateMergeCallback = TCallback<TCGAggregateMergeSignature>;
using TCGAggregateFinalizeCallback = TCallback<TCGAggregateFinalizeSignature>;

struct TCGAggregateCallbacks
{
    TCGAggregateInitCallback Init;
    TCGAggregateUpdateCallback Update;
    TCGAggregateMergeCallback Merge;
    TCGAggregateFinalizeCallback Finalize;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataSource> GetForeignQuery(
    TQueryPtr subquery,
    TConstJoinClausePtr joinClause,
    std::vector<TRow> keys,
    TRowBufferPtr permanentBuffer);

////////////////////////////////////////////////////////////////////////////////

struct TExpressionClosure;

struct TJoinComparers
{
    TComparerFunction* PrefixEqComparer;
    THasherFunction* SuffixHasher;
    TComparerFunction* SuffixEqComparer;
    TComparerFunction* SuffixLessComparer;
    TComparerFunction* ForeignPrefixEqComparer;
    TComparerFunction* ForeignSuffixLessComparer;
    TTernaryComparerFunction* FullTernaryComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#define EVALUATION_HELPERS_INL_H_
#include "evaluation_helpers-inl.h"
#undef EVALUATION_HELPERS_INL_H_
