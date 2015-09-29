#pragma once

#include "private.h"
#include "evaluation_helpers.h"
#include "function_context.h"

#include <ytlib/table_client/llvm_types.h>

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

using NYT::NQueryClient::TExpressionContext;
using NYT::NQueryClient::TFunctionContext;
using NYT::NQueryClient::TExecutionContext;
using NYT::NQueryClient::TRow;
using NYT::NQueryClient::TRowHeader;
using NYT::NQueryClient::TValue;
using NYT::NQueryClient::TValueData;
using NYT::NQueryClient::TLookupRows;
using NYT::NQueryClient::TJoinLookupRows;
using NYT::NQueryClient::TTopCollector;

// Opaque types

template <bool Cross>
class TypeBuilder<TExpressionContext*, Cross>
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
class TypeBuilder<const std::vector<TRow>*, Cross>
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
class TypeBuilder<TTopCollector*, Cross>
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
        Padding
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

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm
