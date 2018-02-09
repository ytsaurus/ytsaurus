#pragma once

#include "private.h"
#include "evaluation_helpers.h"
#include "function_context.h"

#include <yt/ytlib/table_client/llvm_types.h>

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

using NYT::NQueryClient::TExpressionContext;
using NYT::NQueryClient::TFunctionContext;
using NYT::NQueryClient::TExecutionContext;
using NYT::NQueryClient::TRow;
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

template <bool Cross>
class TypeBuilder<bool, Cross>
    : public TypeBuilder<char, Cross>
{ };

// Opaque types
template <bool Cross>
class TypeBuilder<TWriteOpClosure*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TJoinClosure*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TGroupByClosure*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TRowBuffer*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TFunctionContext*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TExecutionContext*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<std::vector<TRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<std::vector<TMutableRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<const std::vector<TRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<const std::vector<TMutableRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TLookupRows*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TJoinLookupRows*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TJoinLookup*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<std::vector<std::pair<TRow, i64>>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };;

template <bool Cross>
class TypeBuilder<TTopCollector*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TSharedRange<TRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TJoinParameters*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TMultiJoinParameters*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TMultiJoinClosure*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<std::unique_ptr<TLookupRows>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

// Aggregate types

template <bool Cross>
class TypeBuilder<TRowHeader, Cross>
{
public:
    enum Fields
    {
        Count,
        Capacity
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<ui32, Cross>::get(context),
            TypeBuilder<ui32, Cross>::get(context),
            nullptr);
    }
};

template <bool Cross>
class TypeBuilder<TRow, Cross>
{
public:
    typedef TypeBuilder<TRowHeader*, Cross> THeader;

    enum Fields
    {
        Header
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            THeader::get(context),
            nullptr);
    }
};

template <bool Cross>
class TypeBuilder<TMutableRow, Cross>
{
public:
    typedef TypeBuilder<TRowHeader*, Cross> THeader;

    enum Fields
    {
        Header
    };

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            THeader::get(context),
            nullptr);
    }
};

template <bool Cross>
class TypeBuilder<TExpressionClosure, Cross>
{
public:
    enum Fields
    {
        FragmentResults,
        OpaqueValues,
        Buffer
    };

    static StructType* get(LLVMContext& context, size_t size)
    {
        return StructType::get(
            llvm::ArrayType::get(TypeBuilder<TValue, false>::get(context), size),
            TypeBuilder<void* const*, Cross>::get(context),
            TypeBuilder<TRowBuffer*, Cross>::get(context),
            nullptr);
    }
};

template <bool Cross>
class TypeBuilder<TJoinComparers, Cross>
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

    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<TComparerFunction*, Cross>::get(context),
            TypeBuilder<THasherFunction*, Cross>::get(context),
            TypeBuilder<TComparerFunction*, Cross>::get(context),
            TypeBuilder<TComparerFunction*, Cross>::get(context),
            TypeBuilder<TComparerFunction*, Cross>::get(context),
            TypeBuilder<TComparerFunction*, Cross>::get(context),
            TypeBuilder<TTernaryComparerFunction*, Cross>::get(context),
            nullptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm
