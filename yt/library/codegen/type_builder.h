#pragma once

#include "public.h"

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/LLVMContext.h>
#include <climits>

namespace NYT::NCodegen {

/////////////////////////////////////////////////////////////////////////////

/// TTypeBuilder - This provides a uniform API for looking up types
/// known at compile time.    To support cross-compilation, we define a
/// series of tag types in the llvm::types namespace, like i<N>,
/// ieee_float, ppc_fp128, etc.    TTypeBuilder<T> allows T to be
/// any of these, a native ctx type (whose size may depend on the host
/// compiler), or a pointer, function, or struct type built out of
/// these.    TTypeBuilder<T> removes native ctx types from this set
/// to guarantee that its result is suitable for cross-compilation.
/// We define the primitive types, pointer types, and functions up to
/// 5 arguments here, but to use this
/// struct with your own types,
/// you'll need to specialize it.    For example, say you want to call a
/// function defined externally as:
///
/// \code{.cpp}
///
///     struct MyType {
///         int32 a;
///         int32 *b;
///         void *array[1];    // Intended as a flexible array.
///     };
///     int8 AFunction(struct MyType *value);
///
/// \endcode
///
/// You'll want to use
///     Function::Create(TTypeBuilder<TTypes::i<8>(MyType*)>::Get(), ...)
/// to declare the function, but when you first try this, your compiler will
/// complain that TTypeBuilder<MyType>::Get() doesn't exist. To fix this,
/// write:
///
/// \code{.cpp}
///
///     struct TTypeBuilder<MyType>
///     {
///         static StructType *Get(llvm::LLVMContext& ctx) {
///             // If you cache this result, be sure to cache it separately
///             // for each llvm::LLVMContext.
///             return StructType::get(
///                 TTypeBuilder<TTypes::i<32>>::Get(ctx),
///                 TTypeBuilder<TTypes::i<32>*>::Get(ctx),
///                 TTypeBuilder<TTypes::i<8>*[]>::Get(ctx),
///                 nullptr);
///         }
///
///         // You may find this a convenient place to put some constants
///         // to help with getelementptr.    They don't have any effect on
///         // the operation of TTypeBuilder.
///         enum Fields {
///             FIELD_A,
///             FIELD_B,
///             FIELD_ARRAY
///         };
///     }
///
/// \endcode
///
/// TTypeBuilder cannot handle recursive types or types you only know at runtime.
/// If you try to give it a recursive type, it will deadlock, infinitely
/// recurse, or do something similarly undesirable.
template <class T>
struct TTypeBuilder { };

// Types for use with cross-compilable TypeBuilders.    These correspond
// exactly with an LLVM-native type.
namespace TTypes {
/// i<N> corresponds to the LLVM llvm::IntegerType with N bits.
template <ui32 num_bits>
struct i { };

// The following
// structes represent the LLVM floating types.

struct ieee_float { };

struct ieee_double { };

struct x86_fp80 { };

struct fp128 { };

struct ppc_fp128 { };
// X86 MMX.

struct x86_mmx { };
} // namespace TTypes

// LLVM doesn't have const or volatile types.
template <class T>
struct TTypeBuilder<const T>
    : public TTypeBuilder<T>
{ };

template <class T>
struct TTypeBuilder<volatile T>
    : public TTypeBuilder<T>
{ };

template <class T>
struct TTypeBuilder<const volatile T>
    : public TTypeBuilder<T>
{ };

// Pointers
template <class T>
struct TTypeBuilder<T*>
{
    static llvm::PointerType *Get(llvm::LLVMContext& ctx)
    {
        return llvm::PointerType::getUnqual(TTypeBuilder<T>::Get(ctx));
    }
};

/// There is no support for references
template <class T>
struct TTypeBuilder<T&>
{ };

// Arrays
template <class T, size_t N>
struct TTypeBuilder<T[N]>
{
    static llvm::ArrayType *Get(llvm::LLVMContext& ctx)
    {
        return llvm::ArrayType::get(TTypeBuilder<T>::Get(ctx), N);
    }
};
/// LLVM uses an array of length 0 to represent an unknown-length array.
template <class T>
struct TTypeBuilder<T[]>
{
    static llvm::ArrayType *Get(llvm::LLVMContext& ctx)
    {
        return llvm::ArrayType::get(TTypeBuilder<T>::Get(ctx), 0);
    }
};

// Define the ctx integral types only for TTypeBuilder<T>.
//
// ctx integral types do not have a defined size. It would be nice to use the
// stdint.h-defined typedefs that do have defined sizes, but we'd run into the
// following problem:
//
// On an ILP32 machine, stdint.h might define:
//
//     typedef int int32_t;
//     typedef long long int64_t;
//     typedef long size_t;
//
// If we defined TTypeBuilder<int32_t> and TTypeBuilder<int64_t>, then any use of
// TTypeBuilder<size_t> would fail.    We couldn't define TTypeBuilder<size_t> in
// addition to the defined-size types because we'd get duplicate definitions on
// platforms where stdint.h instead defines:
//
//     typedef int int32_t;
//     typedef long long int64_t;
//     typedef int size_t;
//
// So we define all the primitive ctx types and nothing else.
#define DEFINE_INTEGRAL_TYPEBUILDER(T) \
template <> \
struct TTypeBuilder<T> \
{ \
public: \
    static llvm::IntegerType *Get(llvm::LLVMContext& ctx) \
    { \
        return llvm::IntegerType::get(ctx, sizeof(T) * CHAR_BIT); \
    } \
};

DEFINE_INTEGRAL_TYPEBUILDER(char);
DEFINE_INTEGRAL_TYPEBUILDER(signed char);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned char);
DEFINE_INTEGRAL_TYPEBUILDER(short);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned short);
DEFINE_INTEGRAL_TYPEBUILDER(int);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned int);
DEFINE_INTEGRAL_TYPEBUILDER(long);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned long);
#ifdef _MSC_VER
DEFINE_INTEGRAL_TYPEBUILDER(__int64);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned __int64);
#else /* _MSC_VER */
DEFINE_INTEGRAL_TYPEBUILDER(long long);
DEFINE_INTEGRAL_TYPEBUILDER(unsigned long long);
#endif /* _MSC_VER */
#undef DEFINE_INTEGRAL_TYPEBUILDER

template <uint32_t num_bits>

struct TTypeBuilder<TTypes::i<num_bits>>
{
    static llvm::IntegerType *Get(llvm::LLVMContext& ctx)
    {
        return llvm::IntegerType::get(ctx, num_bits);
    }
};

template <>
struct TTypeBuilder<float>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getFloatTy(ctx);
    }
};

template <>
struct TTypeBuilder<double>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getDoubleTy(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::ieee_float>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getFloatTy(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::ieee_double>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getDoubleTy(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::x86_fp80>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getX86_FP80Ty(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::fp128>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getFP128Ty(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::ppc_fp128>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getPPC_FP128Ty(ctx);
    }
};

template <>
struct TTypeBuilder<TTypes::x86_mmx>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getX86_MMXTy(ctx);
    }
};

template <>
struct TTypeBuilder<void>
{
    static llvm::Type *Get(llvm::LLVMContext& ctx)
    {
        return llvm::Type::getVoidTy(ctx);
    }
};

/// void* is disallowed in LLVM types, but it occurs often enough in ctx code that
/// we special case it.
template <>
struct TTypeBuilder<void*>
    : public TTypeBuilder<TTypes::i<8>*>
{ };

template <>
struct TTypeBuilder<const void*>
    : public TTypeBuilder<TTypes::i<8>*>
{ };

template <>
struct TTypeBuilder<volatile void*>
    : public TTypeBuilder<TTypes::i<8>*>
{ };

template <>
struct TTypeBuilder<const volatile void*>
    : public TTypeBuilder<TTypes::i<8>*>
{ };

template <typename R, typename... As>
struct TTypeBuilder<R(As...)>
{
    static llvm::FunctionType *Get(llvm::LLVMContext& ctx)
    {
        llvm::Type* params[] = {TTypeBuilder<As>::Get(ctx)...};
        return llvm::FunctionType::get(TTypeBuilder<R>::Get(ctx), params, false);
    }
};

template <typename R, typename... As>
struct TTypeBuilder<R(As..., ...)>
{
    static llvm::FunctionType *Get(llvm::LLVMContext& ctx)
    {
        llvm::Type* params[] = {TTypeBuilder<As>::Get(ctx)...};
        return llvm::FunctionType::get(TTypeBuilder<R>::Get(ctx), params, true);
    }
};

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
