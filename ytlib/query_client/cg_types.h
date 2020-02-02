#pragma once

#include "private.h"
#include "evaluation_helpers.h"
#include "function_context.h"

#include <yt/client/table_client/llvm_types.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

using NYT::NQueryClient::TExpressionContext;
using NYT::NQueryClient::TFunctionContext;
using NYT::NQueryClient::TExecutionContext;
using NYT::NQueryClient::TRow;
using NYT::NQueryClient::TRowRange;
using NYT::NQueryClient::TMutableRow;
using NYT::NQueryClient::TRowHeader;
using NYT::NQueryClient::TValue;
using NYT::NQueryClient::TValueData;
using NYT::NQueryClient::TLookupRows;
using NYT::NQueryClient::TJoinLookup;
using NYT::NQueryClient::TJoinLookupRows;
using NYT::NQueryClient::TTopCollector;
using NYT::NQueryClient::TJoinParameters;
using NYT::NQueryClient::TJoinClosure;
using NYT::NQueryClient::TGroupByClosure;
using NYT::NQueryClient::TWriteOpClosure;
using NYT::NQueryClient::TExpressionClosure;
using NYT::NQueryClient::TJoinComparers;
using NYT::NQueryClient::TComparerFunction;
using NYT::NQueryClient::THasherFunction;
using NYT::NQueryClient::TTernaryComparerFunction;
using NYT::NQueryClient::TMultiJoinParameters;
using NYT::NQueryClient::TMultiJoinClosure;
using NYT::NTableClient::TRowBuffer;
using NYT::TSharedRange;

template <>
struct TTypeBuilder<bool>
    : public TTypeBuilder<char>
{ };

// Opaque types
template <>
struct TTypeBuilder<TWriteOpClosure*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TJoinClosure*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TGroupByClosure*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TExpressionContext*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TFunctionContext*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TExecutionContext*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<std::vector<TRow>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<std::vector<TMutableRow>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<const std::vector<TRow>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<const std::vector<TMutableRow>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TLookupRows*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TJoinLookupRows*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TJoinLookup*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<std::vector<std::pair<TRow, i64>>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TTopCollector*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TSharedRange<TRow>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TSharedRange<TRowRange>*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TJoinParameters*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TMultiJoinParameters*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<TMultiJoinClosure*>
    : public TTypeBuilder<void*>
{ };

template <>
struct TTypeBuilder<std::unique_ptr<TLookupRows>*>
    : public TTypeBuilder<void*>
{ };

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
    typedef TTypeBuilder<TRowHeader*> THeader;

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
    typedef TTypeBuilder<TRowHeader*> THeader;

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

