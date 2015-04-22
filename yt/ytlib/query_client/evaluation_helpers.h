#pragma once

#include "public.h"
#include "callbacks.h"
#include "plan_fragment.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/api/rowset.h>

#include <core/codegen/function.h>

#include <core/misc/chunked_memory_pool.h>

#include <unordered_set>
#include <unordered_map>

#include <sparsehash/dense_hash_set>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialGroupOpHashtableCapacity = 1024;

typedef ui64 (*THasherFunction)(TRow);
typedef char (*TComparerFunction)(TRow, TRow);

struct TExecutionContext;

typedef std::function<void(
    TExecutionContext* executionContext,
    THasherFunction,
    TComparerFunction,
    const std::vector<TRow>& keys,
    const std::vector<TRow>& allRows,
    std::vector<TRow>* joinedRows)> TJoinEvaluator;

struct TExecutionContext
{
#ifndef NDEBUG
    size_t StackSizeGuardHelper;
#endif
    const TTableSchema* Schema;
    ISchemafulReader* Reader;
    ISchemafulWriter* Writer;

    std::vector<std::vector<TRow>>* LiteralRows;
    
    TRowBuffer* PermanentBuffer;
    TRowBuffer* OutputBuffer;
    TRowBuffer* IntermediateBuffer;

    std::vector<TRow>* OutputBatchRows;

    TQueryStatistics* Statistics;

    // These limits prevent full scan.
    i64 InputRowLimit;
    i64 OutputRowLimit;
    i64 GroupRowLimit;
    i64 JoinRowLimit;

    // Limit from LIMIT clause.
    i64 Limit;

    // "char" type is to due LLVM interop.
    char StopFlag = false;

    TJoinEvaluator JoinEvaluator;
};

namespace NDetail {

typedef ui64 (*THasherFunc)(TRow);
struct TGroupHasher
{
    THasherFunc Ptr_;
    TGroupHasher(THasherFunc ptr)
        : Ptr_(ptr)
    { }

    ui64 operator () (TRow row) const
    {
        return Ptr_(row);
    }
};

typedef char (*TComparerFunc)(TRow, TRow);
struct TRowComparer
{
public:
    TRowComparer(TComparerFunc ptr)
        : Ptr_(ptr)
    { }

    bool operator () (TRow a, TRow b) const
    {
        return a.GetHeader() == b.GetHeader() || a.GetHeader() && b.GetHeader() && Ptr_(a, b);
    }

private:
    NDetail::TComparerFunc Ptr_;
};

} // namespace NDetail

typedef
    google::sparsehash::dense_hash_set
    <TRow, NDetail::TGroupHasher, NDetail::TRowComparer>
    TLookupRows;

typedef std::unordered_multiset<
    TRow,
    NDetail::TGroupHasher,
    NDetail::TRowComparer> TJoinLookupRows;

class TTopCollector
{
    class TComparer
    {
    public:
        TComparer(NDetail::TComparerFunc ptr)
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
        NDetail::TComparerFunc Ptr_;
    };

public:
    TTopCollector(i64 limit, NDetail::TComparerFunc comparer);

    std::vector<TRow> GetRows() const
    {
        std::vector<TRow> result;
        std::transform(Rows_.begin(), Rows_.end(), std::back_inserter(result), [] (const std::pair<TRow, int>& value) {
            return value.first;
        });

        std::sort(result.begin(), result.end(), Comparer_);

        return result;
    }

    void AddRow(TRow row);

private:
    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    int TotalMemorySize_ = 0;
    int AllocatedMemorySize_ = 0;
    int GarbageMemorySize_ = 0;

    TComparer Comparer_;

    std::vector<std::unique_ptr<TRowBuffer>> Buffers_;
    std::vector<int> EmptyBufferIds_;
    std::vector<std::pair<TRow, int>> Rows_;
    
    std::pair<TRow, int> Capture(TRow row);

    void AccountGarbage(TRow row);

};

struct TCGVariables
{
    TRowBuilder ConstantsRowBuilder;
    std::vector<std::vector<TRow>> LiteralRows;
};

typedef void (TCGQuerySignature)(TRow, TExecutionContext*);
typedef void (TCGExpressionSignature)(TValue*, TRow, TRow, TExecutionContext*);
using TCGQueryCallback = NCodegen::TCGFunction<TCGQuerySignature>;
using TCGExpressionCallback = NCodegen::TCGFunction<TCGExpressionSignature>;

////////////////////////////////////////////////////////////////////////////////

bool CountRow(i64* limit);

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    const TConstExpressionPtr& predicate,
    const TTableSchema& selfTableSchema,
    TExecuteQuery executeCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

