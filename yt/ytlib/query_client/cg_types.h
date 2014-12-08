#pragma once

#include "private.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/chunked_memory_pool.h>

#include <core/actions/callback.h>

#include <llvm/IR/TypeBuilder.h>

#include <unordered_map>
#include <unordered_set>

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
#ifdef DEBUG
    size_t StackSizeGuardHelper;
#endif

    TTableSchema Schema;
    ISchemafulReader* Reader;
    std::vector<TDataSplits>* DataSplitsArray;
    std::vector<std::vector<TOwningRow>>* LiteralRows;
    TRowBuffer* RowBuffer;
    TChunkedMemoryPool* ScratchSpace;
    ISchemafulWriter* Writer;
    std::vector<TRow>* Batch;

    TQueryStatistics* Statistics;
    i64 InputRowLimit;
    i64 OutputRowLimit;
};

namespace NDetail {

typedef ui64 (*TGroupHasher)(TRow);
typedef char (*TGroupComparer)(TRow, TRow);

} // namespace NDetail

typedef
    std::unordered_set<TRow, NDetail::TGroupHasher, NDetail::TGroupComparer>
    TLookupRows;

struct TCGBinding
{
    std::unordered_map<const TExpression*, int> NodeToConstantIndex;
    std::unordered_map<const TExpression*, int> NodeToRows;
    
};

struct TCGVariables
{
    TRowBuilder ConstantsRowBuilder;
    std::vector<TDataSplits> DataSplitsArray;
    std::vector<std::vector<TOwningRow>> LiteralRows;
};

typedef void (TCGQuerySignature)(TRow, TExecutionContext*);
using TCGQueryCallback = TCallback<TCGQuerySignature>;

const int MaxRowsPerRead = 1024;
const int MaxRowsPerWrite = 1024;

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
class TypeBuilder<TValueData, Cross>
{
public:
    typedef TypeBuilder<char, Cross> TBoolean;
    typedef TypeBuilder<i64, Cross> TInt64;
    typedef TypeBuilder<ui64, Cross> TUint64;
    typedef TypeBuilder<double, Cross> TDouble;
    typedef TypeBuilder<const char*, Cross> TString;

    static Type* get(LLVMContext& context)
    {
        enum
        {
            UnionSize0 = sizeof(i64),
            UnionSize1 = UnionSize0 > sizeof(double) ? UnionSize0 : sizeof(double),
            UnionSize2 = UnionSize1 > sizeof(const char*) ? UnionSize1 : sizeof(const char*)
        };

        static_assert(UnionSize2 == sizeof(i64), "Unexpected union size");

        return TypeBuilder<i64, Cross>::get(context);
    }
};

template <bool Cross>
class TypeBuilder<TValue, Cross>
{
public:
    typedef TypeBuilder<ui16, Cross> TId;
    typedef TypeBuilder<ui16, Cross> TType;
    typedef TypeBuilder<ui32, Cross> TLength;
    typedef TypeBuilder<TValueData, Cross> TData;

    enum Fields
    {
        Id,
        Type,
        Length,
        Data
    };

    static ::llvm::StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TId::get(context),
            TType::get(context),
            TLength::get(context),
            TData::get(context),
            nullptr);
    }
};

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

