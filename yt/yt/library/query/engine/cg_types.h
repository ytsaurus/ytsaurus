#pragma once

#include "llvm_types.h"

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/library/query/misc/function_context.h>

#include <yt/yt/client/table_client/llvm_types.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

using NYT::NQueryClient::TExpressionContext;
using NYT::NQueryClient::TFunctionContext;
using NYT::NQueryClient::TExecutionContext;
using NYT::NQueryClient::TRow;
using NYT::NQueryClient::TRowRange;
using NYT::NQueryClient::TPIRowRange;
using NYT::NQueryClient::TMutableRow;
using NYT::NQueryClient::TRowHeader;
using NYT::NQueryClient::TValue;
using NYT::NQueryClient::TPIValue;
using NYT::NQueryClient::TValueData;
using NYT::NQueryClient::TPIValueData;
using NYT::NQueryClient::TLookupRows;
using NYT::NQueryClient::TLookupRowInRowsetWebAssemblyContext;
using NYT::NQueryClient::TJoinLookup;
using NYT::NQueryClient::TJoinLookupRows;
using NYT::NQueryClient::TTopCollector;
using NYT::NQueryClient::TGroupByClosure;
using NYT::NQueryClient::TWriteOpClosure;
using NYT::NQueryClient::TExpressionClosure;
using NYT::NQueryClient::TJoinComparers;
using NYT::NQueryClient::TComparerFunction;
using NYT::NQueryClient::THasherFunction;
using NYT::NQueryClient::TTernaryComparerFunction;
using NYT::NQueryClient::TArrayJoinParameters;
using NYT::NQueryClient::TMultiJoinParameters;
using NYT::NQueryClient::TMultiJoinClosure;
using NYT::NQueryClient::TLikeExpressionContext;
using NYT::NQueryClient::TRowSchemaInformation;
using NYT::NQueryClient::TCompositeMemberAccessorPath;
using NYT::NTableClient::TRowBuffer;
using NYT::TSharedRange;

template <>
struct TTypeBuilder<bool>
    : public TTypeBuilder<char>
{ };

// Opaque types

#define OPAQUE_TYPE(type) \
    template <> \
    struct TTypeBuilder< type > \
        : public TTypeBuilder<void*> \
    { };

    OPAQUE_TYPE(TWriteOpClosure*)
    OPAQUE_TYPE(TGroupByClosure*)
    OPAQUE_TYPE(TExpressionContext*)
    OPAQUE_TYPE(TFunctionContext*)
    OPAQUE_TYPE(TExecutionContext*)
    OPAQUE_TYPE(std::vector<TRow>*)
    OPAQUE_TYPE(std::vector<TMutableRow>*)
    OPAQUE_TYPE(const std::vector<TRow>*)
    OPAQUE_TYPE(const std::vector<TMutableRow>*)
    OPAQUE_TYPE(TLookupRows*)
    OPAQUE_TYPE(TJoinLookupRows*)
    OPAQUE_TYPE(TJoinLookup*)
    OPAQUE_TYPE(TTopCollector*)
    OPAQUE_TYPE(TSharedRange<TRow>*)
    OPAQUE_TYPE(TSharedRange<TRowRange>*)
    OPAQUE_TYPE(TSharedRange<TPIRowRange>*)
    OPAQUE_TYPE(TArrayJoinParameters*)
    OPAQUE_TYPE(TMultiJoinParameters*)
    OPAQUE_TYPE(TMultiJoinClosure*)
    OPAQUE_TYPE(std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>*)
    OPAQUE_TYPE(TSharedRange<TRange<TPIValue>>*)
    OPAQUE_TYPE(TLikeExpressionContext*)
    OPAQUE_TYPE(TRowSchemaInformation*)
    OPAQUE_TYPE(TCompositeMemberAccessorPath*)

    OPAQUE_TYPE(struct tm*)

#undef OPAQUE_TYPE

// Aggregate types

template <>
struct TTypeBuilder<TRowHeader>
{
public:
    enum Fields
    {
        Count,
        Capacity
    };

    static StructType* Get(LLVMContext& context)
    {
        return StructType::get(context, {
            TTypeBuilder<ui32>::Get(context),
            TTypeBuilder<ui32>::Get(context)});
    }
};

template <>
struct TTypeBuilder<TRow>
{
public:
    using THeader = TTypeBuilder<TRowHeader*>;

    enum Fields
    {
        Header
    };

    static StructType* Get(LLVMContext& context)
    {
        return StructType::get(context, ArrayRef<Type*>{
            THeader::Get(context)});
    }
};

template <>
struct TTypeBuilder<TMutableRow>
{
public:
    using THeader = TTypeBuilder<TRowHeader*>;

    enum Fields
    {
        Header
    };

    static StructType* Get(LLVMContext& context)
    {
        return StructType::get(context, ArrayRef<Type*>{
            THeader::Get(context)});
    }
};

template <>
struct TTypeBuilder<TExpressionClosure>
{
public:
    using TOpaqueValues = TTypeBuilder<void* const*>;
    using TBuffer = TTypeBuilder<TExpressionContext*>;

    enum Fields
    {
        FragmentResults,
        OpaqueValues,
        Buffer
    };

    static StructType* Get(LLVMContext& context, size_t size)
    {
        return StructType::get(context, {
            llvm::ArrayType::get(TTypeBuilder<TValue>::Get(context), size),
            TTypeBuilder<void* const*>::Get(context),
            TTypeBuilder<TExpressionContext*>::Get(context)});
    }
};

template <>
struct TTypeBuilder<TJoinComparers>
{
public:
    enum Fields
    {
        PrefixEqComparer,
        SuffixHasher,
        SuffixEqComparer,
        SuffixLessComparer,
        ForeignPrefixEqComparer,
        ForeignSuffixLessComparer,
        FullTernaryComparer
    };

    static StructType* Get(LLVMContext& context)
    {
        return StructType::get(context, {
            TTypeBuilder<TComparerFunction*>::Get(context),
            TTypeBuilder<THasherFunction*>::Get(context),
            TTypeBuilder<TComparerFunction*>::Get(context),
            TTypeBuilder<TComparerFunction*>::Get(context),
            TTypeBuilder<TComparerFunction*>::Get(context),
            TTypeBuilder<TComparerFunction*>::Get(context),
            TTypeBuilder<TTernaryComparerFunction*>::Get(context)});
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
