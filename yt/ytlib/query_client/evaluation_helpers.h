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
    TTableSchema Schema;
    ISchemafulReader* Reader;
    ISchemafulWriter* Writer;

    std::vector<std::vector<TOwningRow>>* LiteralRows;
    
    TRowBuffer* PermanentBuffer;
    TRowBuffer* OutputBuffer;
    TRowBuffer* IntermediateBuffer;

    std::vector<TRow>* Batch;

    TQueryStatistics* Statistics;

    // TODO(lukyan): Rename to ReadRowLimit and WriteRowLimit
    i64 InputRowLimit;
    i64 OutputRowLimit;
    i64 GroupRowLimit;
    i64 JoinRowLimit;

    i64 Limit;

    // "char" type is to due LLVM interop.
    char StopFlag = false;

    TJoinEvaluator EvaluateJoin;
};

namespace NDetail {

typedef ui64 (*TGroupHasherFunc)(TRow);
typedef char (*TGroupComparerFunc)(TRow, TRow);

struct TGroupHasher
{
    TGroupHasherFunc Ptr_;
    TGroupHasher(TGroupHasherFunc ptr)
        : Ptr_(ptr)
    { }

    ui64 operator () (TRow row) const
    {
        return Ptr_(row);
    }
};

struct TGroupComparer
{
    TGroupComparerFunc Ptr_;
    TGroupComparer(TGroupComparerFunc ptr)
        : Ptr_(ptr)
    { }

    char operator () (TRow a, TRow b) const
    {
        return a.GetHeader() == b.GetHeader() || a.GetHeader() && b.GetHeader() && Ptr_(a, b);
    }
};

} // namespace NDetail

typedef
    google::sparsehash::dense_hash_set
    <TRow, NDetail::TGroupHasher, NDetail::TGroupComparer>
    TLookupRows;

typedef std::unordered_multiset<
    TRow,
    NDetail::TGroupHasher,
    NDetail::TGroupComparer> TJoinLookupRows;

struct TCGVariables
{
    TRowBuilder ConstantsRowBuilder;
    std::vector<std::vector<TOwningRow>> LiteralRows;
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

