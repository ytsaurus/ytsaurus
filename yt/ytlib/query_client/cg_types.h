#pragma once

#include "public.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/chunked_memory_pool.h>

#include <llvm/IR/TypeBuilder.h>

#include <unordered_map>
#include <unordered_set>

// This file serves two purposes: first, to define types used during interaction
// of native and JIT'ed code; second, to map necessary C++ types to LLVM types.

namespace llvm {
    class Context;
    class Function;
    class FunctionType;
    class Module;
    class Twine;
}

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NVersionedTableClient::TRowBuffer;

struct TPassedFragmentParams
{
    // Constants?
    IEvaluateCallbacks* Callbacks;
    TPlanContext* Context;
    std::vector<TDataSplits>* DataSplitsArray;
};

namespace NDetail {
    class TGroupHasher;
    class TGroupComparer;
}

typedef
    std::unordered_set<TRow, NDetail::TGroupHasher, NDetail::TGroupComparer>
    TLookupRows;

// TODO(sandello): Better names for these.
struct TCGImmediates
{
    std::unordered_map<const TExpression*, int> NodeToConstantIndex;
    std::unordered_map<const TOperator*, int> ScanOpToDataSplits;
};

// TODO(sandello): Better names for these.
struct TFragmentParams
    : public TCGImmediates
{
    std::vector<TValue> ConstantArray;
    std::vector<TDataSplits> DataSplitsArray;
};

typedef void (*TCodegenedFunction)(
    TRow constants,
    TPassedFragmentParams* passedFragmentParams,
    std::vector<TRow>* batch, // TODO(lukyan): remove this
    TRowBuffer* rowBuffer, // TODO(lukyan): remove this
    ISchemafulWriter* writer);

static const int MaxRowsPerRead = 512;
static const int MaxRowsPerWrite = 512;

namespace NDetail {

template <class T>
size_t THashCombine(size_t seed, const T& value)
{
    ::hash<T> hasher;
    return seed ^ (hasher(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2));
    // TODO(lukyan): Fix this function
}

class TGroupHasher
{
public:
    explicit TGroupHasher(int keySize)
        : KeySize_(keySize)
    { }
    size_t operator() (TRow key) const
    {
        size_t result = 0;
        for (int i = 0; i < KeySize_; ++i) {
            result = THashCombine(result, GetHash(key[i]));
        }
        return result;
    }
private:
    int KeySize_;
};

class TGroupComparer
{
public:
    explicit TGroupComparer(int keySize)
        : KeySize_(keySize)
    { }
    bool operator() (TRow lhs, TRow rhs) const
    {
        for (int i = 0; i < KeySize_; ++i) {
            if (CompareRowValues(lhs[i], rhs[i])) {
                return false;
            }
        }
        return true;
    }
private:
    int KeySize_;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

namespace llvm {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NQueryClient;
using namespace NVersionedTableClient;

// Opaque types

template <bool cross>
class TypeBuilder<ISchemafulWriter*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<TRowBuffer*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<std::vector<TRow>*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<std::pair<std::vector<TRow>, TChunkedMemoryPool*>*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<TLookupRows*, cross>
    : public TypeBuilder<void*, cross>
{ };

template <bool cross>
class TypeBuilder<TPassedFragmentParams*, cross>
    : public TypeBuilder<void*, cross>
{ };

// Aggregate types

template <bool cross>
class TypeBuilder<TValueData, cross>
{
public:
    static Type* get(LLVMContext& context)
    {
        enum
        {
            UnionSize0 = sizeof(i64),
            UnionSize1 = UnionSize0 > sizeof(double) ? UnionSize0 : sizeof(double),
            UnionSize2 = UnionSize1 > sizeof(const char*) ? UnionSize1 : sizeof(const char*)
        };

        static_assert(UnionSize2 == sizeof(i64), "Unexpected union size");

        return TypeBuilder<i64, cross>::get(context);
    }

    enum Fields
    {
        Integer,
        Double,
        String
    };

    static Type* getAs(Fields dataFields, LLVMContext& context)
    {
        switch (dataFields) {
            case Fields::Integer:
                return TypeBuilder<i64*, cross>::get(context);
            case Fields::Double:
                return TypeBuilder<double*, cross>::get(context);
            case Fields::String:
                return TypeBuilder<const char**, cross>::get(context);
        }
        YUNREACHABLE();
    }
};

template <bool cross>
class TypeBuilder<TValue, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<ui16, cross>::get(context),
            TypeBuilder<ui16, cross>::get(context),
            TypeBuilder<ui32, cross>::get(context),
            TypeBuilder<TValueData, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Id,
        Type,
        Length,
        Data
    };
};

template <bool cross>
class TypeBuilder<TRowHeader, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<ui32, cross>::get(context),
            TypeBuilder<ui32, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Count,
        Padding
    };
};

template <bool cross>
class TypeBuilder<TRow, cross>
{
public:
    static StructType* get(LLVMContext& context)
    {
        return StructType::get(
            TypeBuilder<TRowHeader*, cross>::get(context),
            nullptr);
    }

    enum Fields
    {
        Header
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace llvm

