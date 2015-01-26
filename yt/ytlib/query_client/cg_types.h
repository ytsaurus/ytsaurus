#pragma once

#include "private.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/llvm_types.h>

#include <core/misc/chunked_memory_pool.h>

#include <core/actions/callback.h>

#include <core/codegen/function.h>

#include <llvm/IR/TypeBuilder.h>

#include <unordered_set>
#include <unordered_map>

#include <sparsehash/dense_hash_set>

// This file serves two purposes: first, to define types used during interaction
// of native and JIT'ed code; second, to map necessary C++ types to LLVM types.

namespace llvm {

class LLVMContext;
class Function;
class FunctionType;
class Module;
class Twine;
class Value;
class Instruction;

} // namespace llvm

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

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

    i64 Limit;

    char stopFlag = false;
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

struct TCGBinding
{
    std::unordered_map<const TExpression*, int> NodeToConstantIndex;
    std::unordered_map<const TExpression*, int> NodeToRows;
    
};

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

} // namespace NQueryClient
} // namespace NYT

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

using NYT::NQueryClient::TRow;
using NYT::NQueryClient::TRowHeader;
using NYT::NQueryClient::TValue;
using NYT::NQueryClient::TValueData;
using NYT::NQueryClient::TLookupRows;
using NYT::NQueryClient::TExecutionContext;

// Opaque types

template <bool Cross>
class TypeBuilder<std::vector<TRow>*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TLookupRows*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

template <bool Cross>
class TypeBuilder<TExecutionContext*, Cross>
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

