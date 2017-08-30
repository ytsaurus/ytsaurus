#pragma once

#include "public.h"
#include "callbacks.h"
#include "function_context.h"

#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/codegen/function.h>

#include <yt/core/misc/chunked_memory_pool.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

#include <sparsehash/dense_hash_set>
#include <sparsehash/dense_hash_map>

namespace NYT {
namespace NQueryClient {

const size_t RowsetProcessingSize = 1024;
const size_t WriteRowsetSize = 64 * RowsetProcessingSize;

////////////////////////////////////////////////////////////////////////////////

class TInterruptedCompleteException
{ };

class TInterruptedIncompleteException
{ };

struct TOutputBufferTag
{ };

struct TIntermadiateBufferTag
{ };

struct TPermanentBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialGroupOpHashtableCapacity = 1024;

using THasherFunction = ui64(TRow);
using TComparerFunction = char(TRow, TRow);

namespace NDetail {
class TGroupHasher
{
public:
    TGroupHasher(THasherFunction* ptr)
        : Ptr_(ptr)
    { }

    ui64 operator () (TRow row) const
    {
        return Ptr_(row);
    }

private:
    THasherFunction* Ptr_;
};

class TRowComparer
{
public:
    TRowComparer(TComparerFunction* ptr)
        : Ptr_(ptr)
    { }

    bool operator () (TRow a, TRow b) const
    {
        return a.GetHeader() == b.GetHeader() || a.GetHeader() && b.GetHeader() && Ptr_(a, b);
    }

private:
    TComparerFunction* Ptr_;
};
} // namespace NDetail

using TLookupRows = google::sparsehash::dense_hash_set<
    TRow,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookup = google::sparsehash::dense_hash_map<
    TRow,
    std::pair<int, bool>,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookupRows = std::unordered_multiset<
    TRow,
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
};

struct TChainedRow
{
    TRow Row;
    TRow Key;
    int NextRowIndex;
};

struct TJoinClosure
{
    TRowBufferPtr Buffer;
    TJoinLookup Lookup;
    std::vector<TChainedRow> ChainedRows;

    TComparerFunction* PrefixEqComparer;
    int KeySize;

    TRow LastKey;
    std::vector<std::pair<TRow, int>> KeysToRows;
    size_t CommonKeyPrefixDebug;

    size_t BatchSize;
    std::function<void()> ProcessJoinBatch;
    std::function<void()> ProcessSegment;

    TJoinClosure(
        THasherFunction* lookupHasher,
        TComparerFunction* lookupEqComparer,
        TComparerFunction* prefixEqComparer,
        int keySize,
        size_t batchSize);
};

struct TGroupByClosure
{
    TRowBufferPtr Buffer ;
    TLookupRows Lookup;
    std::vector<TRow> GroupedRows;
    int KeySize;
    bool CheckNulls;

    TGroupByClosure(
        THasherFunction* groupHasher,
        TComparerFunction* groupComparer,
        int keySize,
        bool checkNulls);
};

struct TWriteOpClosure
{
    TRowBufferPtr OutputBuffer;

    // Rows stored in OutputBuffer
    std::vector<TRow> OutputRowsBatch;

    TWriteOpClosure();

};

typedef TRowBuffer TExpressionContext;

#ifndef NDEBUG
#define CHECK_STACK() \
    { \
        int dummy; \
        size_t currentStackSize = reinterpret_cast<intptr_t>(context) - reinterpret_cast<intptr_t>(&dummy); \
        YCHECK(currentStackSize < 100000); \
    }
#else
#define CHECK_STACK() (void) 0;
#endif

struct TExecutionContext
{
    ISchemafulReaderPtr Reader;
    ISchemafulWriterPtr Writer;

    TQueryStatistics* Statistics;

    // These limits prevent full scan.
    i64 InputRowLimit;
    i64 OutputRowLimit;
    i64 GroupRowLimit;
    i64 JoinRowLimit;

    // Limit from LIMIT clause.
    i64 Limit;

    TExecutionContext()
    {
        auto context = this;
        Y_UNUSED(context);
        CHECK_STACK();
    }
    bool IsOrdered = false;

};

class TTopCollector
{
    class TComparer
    {
    public:
        explicit TComparer(TComparerFunction* ptr)
            : Ptr_(ptr)
        { }

        bool operator() (const std::pair<TRow, int>& lhs, const std::pair<TRow, int>& rhs) const
        {
            return (*this)(lhs.first, rhs.first);
        }

        bool operator () (TRow a, TRow b) const
        {
            return Ptr_(a, b);
        }

    private:
        TComparerFunction* const Ptr_;
    };

public:
    TTopCollector(i64 limit, TComparerFunction* comparer);

    std::vector<TMutableRow> GetRows(int rowSize) const;

    void AddRow(TRow row);

private:
    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    TComparer Comparer_;

    std::vector<TRowBufferPtr> Buffers_;
    std::vector<int> EmptyBufferIds_;
    std::vector<std::pair<TMutableRow, int>> Rows_;

    std::pair<TMutableRow, int> Capture(TRow row);

    void AccountGarbage(TRow row);

};

class TCGVariables
{
public:
    template <class T, class... Args>
    size_t AddOpaque(Args&&... args)
    {
        auto pointer = new T(std::forward<Args>(args)...);
        auto deleter = [] (void* ptr) {
            static_assert(sizeof(T) > 0, "Cannot delete incomplete type.");
            delete static_cast<T*>(ptr);
        };

        std::unique_ptr<void, void(*)(void*)> holder(pointer, deleter);
        YCHECK(holder);

        OpaqueValues_.push_back(std::move(holder));
        OpaquePointers_.push_back(pointer);

        return OpaquePointers_.size() - 1;
    }

    void* const* GetOpaqueData() const
    {
        return OpaquePointers_.data();
    }

    void Clear()
    {
        OpaquePointers_.clear();
        OpaqueValues_.clear();
    }

private:
    std::vector<std::unique_ptr<void, void(*)(void*)>> OpaqueValues_;
    std::vector<void*> OpaquePointers_;

};

typedef void (TCGQuerySignature)(void* const*, TExecutionContext*);
typedef void (TCGExpressionSignature)(void* const*, TValue*, TRow, TExpressionContext*);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT