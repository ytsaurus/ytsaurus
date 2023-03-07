#pragma once

#include "public.h"
#include "callbacks.h"
#include "function_context.h"
#include "objects_holder.h"

#include <yt/client/api/rowset.h>

#include <yt/client/table_client/unversioned_row.h>

#include <yt/library/codegen/function.h>

#include <yt/core/misc/chunked_memory_pool.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

namespace NYT::NQueryClient {

constexpr size_t RowsetProcessingSize = 1024;
constexpr size_t WriteRowsetSize = 64 * RowsetProcessingSize;

////////////////////////////////////////////////////////////////////////////////

class TInterruptedIncompleteException
{ };

struct TOutputBufferTag
{ };

struct TIntermediateBufferTag
{ };

struct TPermanentBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialGroupOpHashtableCapacity = 1024;

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

using TLookupRows = google::sparsehash::dense_hash_set<
    const TValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookup = google::sparsehash::dense_hash_map<
    const TValue*,
    std::pair<int, bool>,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookupRows = std::unordered_multiset<
    const TValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

struct TExecutionContext;

struct TJoinParameters
{
    bool IsOrdered;
    bool IsLeft;
    bool IsSortMergeJoin;
    bool IsPartiallySorted;
    std::vector<size_t> SelfColumns;
    std::vector<size_t> ForeignColumns;
    TJoinSubqueryEvaluator ExecuteForeign;
    size_t BatchSize;
    size_t CommonKeyPrefixDebug;
    size_t PrimaryRowSize;
};

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
    SmallVector<TSingleJoinParameters, 10> Items;
    size_t PrimaryRowSize;
    size_t BatchSize;
};

struct TChainedRow
{
    const TValue* Row;
    const TValue* Key;
    int NextRowIndex;
};

struct TJoinClosure
{
    TRowBufferPtr Buffer;
    TJoinLookup Lookup;
    std::vector<TChainedRow> ChainedRows;

    TComparerFunction* PrefixEqComparer;
    int KeySize;

    const TValue* LastKey = nullptr;
    std::vector<std::pair<const TValue*, int>> KeysToRows;
    size_t CommonKeyPrefixDebug;
    size_t PrimaryRowSize;

    size_t BatchSize;
    std::function<void()> ProcessJoinBatch;
    std::function<void()> ProcessSegment;

    TJoinClosure(
        THasherFunction* lookupHasher,
        TComparerFunction* lookupEqComparer,
        TComparerFunction* prefixEqComparer,
        int keySize,
        int primaryRowSize,
        size_t batchSize);
};

struct TMultiJoinClosure
{
    TRowBufferPtr Buffer;

    typedef google::sparsehash::dense_hash_set<
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

    SmallVector<TItem, 32> Items;

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

    TWriteOpClosure();

};

typedef TRowBuffer TExpressionContext;

#define CHECK_STACK() (void) 0;

struct TExecutionContext
{
    ISchemafulReaderPtr Reader;
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

using TCGQueryCallback = NCodegen::TCGFunction<TCGQuerySignature>;
using TCGExpressionCallback = NCodegen::TCGFunction<TCGExpressionSignature>;
using TCGAggregateInitCallback = NCodegen::TCGFunction<TCGAggregateInitSignature>;
using TCGAggregateUpdateCallback = NCodegen::TCGFunction<TCGAggregateUpdateSignature>;
using TCGAggregateMergeCallback = NCodegen::TCGFunction<TCGAggregateMergeSignature>;
using TCGAggregateFinalizeCallback = NCodegen::TCGFunction<TCGAggregateFinalizeSignature>;

struct TCGAggregateCallbacks
{
    TCGAggregateInitCallback Init;
    TCGAggregateUpdateCallback Update;
    TCGAggregateMergeCallback Merge;
    TCGAggregateFinalizeCallback Finalize;
};

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataRanges> GetForeignQuery(
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
