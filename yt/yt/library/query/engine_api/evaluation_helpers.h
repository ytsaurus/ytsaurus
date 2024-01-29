#pragma once

#include "expression_context.h"
#include "position_independent_value.h"

#include "public.h"

#include <yt/yt/library/web_assembly/api/function.h>

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

#include <contrib/libs/re2/re2/re2.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 RowsetProcessingBatchSize = 1024;
constexpr i64 WriteRowsetSize = 64 * RowsetProcessingBatchSize;
constexpr i64 MaxJoinBatchSize = 1024 * RowsetProcessingBatchSize;

class TInterruptedIncompleteException
{ };

struct TOutputBufferTag
{ };

struct TIntermediateBufferTag
{ };

struct TPermanentBufferTag
{ };

struct TForeignExecutorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

constexpr const size_t InitialGroupOpHashtableCapacity = 1024;

using THasherFunction = ui64(const TPIValue*);
using TComparerFunction = char(const TPIValue*, const TPIValue*);
using TTernaryComparerFunction = i64(const TPIValue*, const TPIValue*);

namespace NDetail {

class TGroupHasher
{
public:
    // Intentionally implicit.
    TGroupHasher(NWebAssembly::TCompartmentFunction<THasherFunction> hashser)
        : Hasher_(hashser)
    { }

    ui64 operator () (const TPIValue* row) const
    {
        return Hasher_(row);
    }

private:
    NWebAssembly::TCompartmentFunction<THasherFunction> Hasher_;
};

class TRowComparer
{
public:
    // Intentionally implicit.
    TRowComparer(NWebAssembly::TCompartmentFunction<TComparerFunction> comparer)
        : Comparer_(comparer)
    { }

    bool operator () (const TPIValue* lhs, const TPIValue* rhs) const
    {
        return (lhs == rhs) ||
            (lhs && rhs && Comparer_(lhs, rhs));
    }

private:
    NWebAssembly::TCompartmentFunction<TComparerFunction> Comparer_;
};

} // namespace NDetail

using TLookupRows = google::dense_hash_set<
    const TPIValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookup = google::dense_hash_map<
    const TPIValue*,
    std::pair<int, bool>,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

using TJoinLookupRows = std::unordered_multiset<
    const TPIValue*,
    NDetail::TGroupHasher,
    NDetail::TRowComparer>;

struct TLikeExpressionContext
{
    const std::unique_ptr<re2::RE2> PrecompiledRegex;

    TLikeExpressionContext(std::unique_ptr<re2::RE2> precompiledRegex)
        : PrecompiledRegex(std::move(precompiledRegex))
    { }
};

TString ConvertLikePatternToRegex(
    TStringBuf pattern,
    EStringMatchOp matchOp,
    TStringBuf escapeCharacter,
    bool escapeCharacterUsed);

struct TExecutionContext;

struct TArrayJoinParameters
{
    bool IsLeft;
    std::vector<EValueType> FlattenedTypes;
    std::vector<int> SelfJoinedColumns;
    std::vector<int> ArrayJoinedColumns;
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
    TCompactVector<TSingleJoinParameters, 10> Items;
    size_t PrimaryRowSize;
    size_t BatchSize;
};

struct TMultiJoinClosure
{
    using THashJoinLookup = google::dense_hash_set<
        TPIValue*,
        NDetail::TGroupHasher,
        NDetail::TRowComparer>;  // + slot after row

    struct TItem
    {
        TExpressionContext Context;
        size_t KeySize;
        NWebAssembly::TCompartmentFunction<TComparerFunction> PrefixEqComparer;

        THashJoinLookup Lookup;
        std::vector<TPIValue*> OrderedKeys;  // + slot after row
        const TPIValue* LastKey = nullptr;

        TItem(
            IMemoryChunkProviderPtr chunkProvider,
            size_t keySize,
            NWebAssembly::TCompartmentFunction<TComparerFunction> prefixEqComparer,
            NWebAssembly::TCompartmentFunction<THasherFunction> lookupHasher,
            NWebAssembly::TCompartmentFunction<TComparerFunction> lookupEqComparer);
    };

    TExpressionContext Context;

    std::vector<TPIValue*> PrimaryRows;

    TCompactVector<TItem, 32> Items;

    size_t PrimaryRowSize;
    size_t BatchSize;
    std::function<void(size_t)> ProcessSegment;
    std::function<bool()> ProcessJoinBatch;
};

class TGroupByClosure;

struct TWriteOpClosure
{
    TExpressionContext OutputContext;

    // Rows stored in OutputContext
    std::vector<TRow> OutputRowsBatch;
    size_t RowSize;

    explicit TWriteOpClosure(IMemoryChunkProviderPtr chunkProvider);
};

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
        NWebAssembly::TCompartmentFunction<TComparerFunction> comparer,
        size_t rowSize,
        IMemoryChunkProviderPtr memoryChunkProvider);

    std::vector<const TPIValue*> GetRows() const;

    void AddRow(const TPIValue* row);

private:
    // GarbageMemorySize <= AllocatedMemorySize <= TotalMemorySize
    size_t TotalMemorySize_ = 0;
    size_t AllocatedMemorySize_ = 0;
    size_t GarbageMemorySize_ = 0;

    class TComparer
    {
    public:
        explicit TComparer(NWebAssembly::TCompartmentFunction<TComparerFunction> comparer)
            : Comparer_(comparer)
        { }

        bool operator() (const std::pair<const TPIValue*, int>& lhs, const std::pair<const TPIValue*, int>& rhs) const
        {
            return (*this)(lhs.first, rhs.first);
        }

        bool operator () (const TPIValue* a, const TPIValue* b) const
        {
            return Comparer_(a, b);
        }

    private:
        NWebAssembly::TCompartmentFunction<TComparerFunction> const Comparer_;
    };

    TComparer Comparer_;
    size_t RowSize_;
    IMemoryChunkProviderPtr MemoryChunkProvider_;

    std::vector<TExpressionContext> Contexts_;
    std::vector<int> EmptyContextIds_;
    std::vector<std::pair<const TPIValue*, int>> Rows_;

    std::pair<const TPIValue*, int> Capture(const TPIValue* row);

    void AccountGarbage(const TPIValue* row);
};

struct TRowSchemaInformation
{
    i64 RowWeightWithNoStrings;
    std::vector<int> StringLikeIndices;
};

class TCGVariables
{
public:
    template <class T, class... TArgs>
    int AddOpaque(TArgs&&... args);

    TRange<void*> GetOpaqueData() const;

    void Clear();

    int AddLiteralValue(TOwningValue value);

    TRange<TPIValue> GetLiteralValues() const;

private:
    TObjectsHolder Holder_;
    std::vector<void*> OpaquePointers_;
    std::vector<TOwningValue> OwningLiteralValues_;
    mutable std::unique_ptr<TPIValue[]> LiteralValues_;

    static void InitLiteralValuesIfNeeded(const TCGVariables* variables);
};

using TCGPIQuerySignature = void(const TPIValue[], void* const[], TExecutionContext*);
using TCGPIExpressionSignature = void(const TPIValue[], void* const[], TPIValue*, const TPIValue[], TExpressionContext*);
using TCGPIAggregateInitSignature = void(TExpressionContext*, TPIValue*);
using TCGPIAggregateUpdateSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);
using TCGPIAggregateMergeSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);
using TCGPIAggregateFinalizeSignature = void(TExpressionContext*, TPIValue*, const TPIValue*);

using TCGQuerySignature = void(TRange<TPIValue>, TRange<void*>, TExecutionContext*);
using TCGExpressionSignature = void(TRange<TPIValue>, TRange<void*>, TValue*, TRange<TValue>, const TRowBufferPtr&);
using TCGAggregateInitSignature = void(const TRowBufferPtr&, TValue*);
using TCGAggregateUpdateSignature = void(const TRowBufferPtr&, TValue*, TRange<TValue>);
using TCGAggregateMergeSignature = void(const TRowBufferPtr&, TValue*, const TValue*);
using TCGAggregateFinalizeSignature = void(const TRowBufferPtr&, TValue*, const TValue*);

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

//! NB: TCGQueryInstance is NOT thread-safe.
class TCGQueryInstance
{
public:
    TCGQueryInstance() = default;
    explicit TCGQueryInstance(TCGQueryCallback callback);

    void Run(
        TRange<TPIValue> literalValues,
        TRange<void*> opaqueData,
        TExecutionContext* context);

private:
    const TCGQueryCallback Callback_;
};

class TCGQueryImage
{
public:
    explicit TCGQueryImage(TCGQueryCallback callback);

    TCGQueryInstance Instantiate() const;

private:
    const TCGQueryCallback Callback_;
};

//! NB: TCGExpressionInstance is NOT thread-safe.
class TCGExpressionInstance
{
public:
    TCGExpressionInstance() = default;
    explicit TCGExpressionInstance(TCGExpressionCallback callback);

    void Run(
        TRange<TPIValue> literalValues,
        TRange<void*> opaqueData,
        TValue* result,
        TRange<TValue> inputRow,
        const TRowBufferPtr& buffer);

    operator bool() const;

private:
    TCGExpressionCallback Callback_;
};

class TCGExpressionImage
{
public:
    TCGExpressionImage() = default;
    explicit TCGExpressionImage(TCGExpressionCallback callback);

    TCGExpressionInstance Instantiate() const;

    operator bool() const;

private:
    TCGExpressionCallback Callback_;
};

//! NB: TCGAggregateInstance is NOT thread-safe.
class TCGAggregateInstance
{
public:
    TCGAggregateInstance() = default;
    explicit TCGAggregateInstance(TCGAggregateCallbacks callbacks);

    void RunInit(const TRowBufferPtr& buffer, TValue* state);
    void RunUpdate(const TRowBufferPtr& buffer, TValue* state, TRange<TValue> arguments);
    void RunMerge(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState);
    void RunFinalize(const TRowBufferPtr& buffer, TValue* firstState, const TValue* secondState);

private:
    TCGAggregateCallbacks Callbacks_;
};

class TCGAggregateImage
{
public:
    TCGAggregateImage() = default;
    explicit TCGAggregateImage(TCGAggregateCallbacks callbacks);

    TCGAggregateInstance Instantiate() const;

private:
    TCGAggregateCallbacks Callbacks_;
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
