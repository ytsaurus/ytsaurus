#pragma once

#include "private.h"
#include "evaluation_helpers.h"
#include "function_context.h"

#include <yt/ytlib/table_client/llvm_types.h>

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
struct TypeBuilder<bool>
    : public TypeBuilder<char>
{ };

// Opaque types
template <>
struct TypeBuilder<TWriteOpClosure*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TJoinClosure*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TGroupByClosure*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TExpressionContext*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TFunctionContext*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TExecutionContext*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<std::vector<TRow>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<std::vector<TMutableRow>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<const std::vector<TRow>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<const std::vector<TMutableRow>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TLookupRows*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TJoinLookupRows*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TJoinLookup*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<std::vector<std::pair<TRow, i64>>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TTopCollector*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TSharedRange<TRow>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TSharedRange<TRowRange>*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TJoinParameters*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TMultiJoinParameters*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<TMultiJoinClosure*>
    : public TypeBuilder<void*>
{ };

template <>
struct TypeBuilder<std::unique_ptr<TLookupRows>*>
    : public TypeBuilder<void*>
{ };

// Aggregate types

template <>
struct TypeBuilder<TRowHeader>
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
            TypeBuilder<ui32>::Get(context),
            TypeBuilder<ui32>::Get(context)});
    }
};

template <>
struct TypeBuilder<TRow>
{
public:
    typedef TypeBuilder<TRowHeader*> THeader;

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
struct TypeBuilder<TMutableRow>
{
public:
    typedef TypeBuilder<TRowHeader*> THeader;

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
struct TypeBuilder<TExpressionClosure>
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
            llvm::ArrayType::get(TypeBuilder<TValue>::Get(context), size),
            TypeBuilder<void* const*>::Get(context),
            TypeBuilder<TExpressionContext*>::Get(context)});
    }
};

template <>
struct TypeBuilder<TJoinComparers>
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
            TypeBuilder<TComparerFunction*>::Get(context),
            TypeBuilder<THasherFunction*>::Get(context),
            TypeBuilder<TComparerFunction*>::Get(context),
            TypeBuilder<TComparerFunction*>::Get(context),
            TypeBuilder<TComparerFunction*>::Get(context),
            TypeBuilder<TComparerFunction*>::Get(context),
            TypeBuilder<TTernaryComparerFunction*>::Get(context)});
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

