#pragma once

#include <util/generic/typetraits.h>

// See the following references for an inspiration:
//   * http://llvm.org/viewvc/llvm-project/libcxx/trunk/include/type_traits?revision=HEAD&view=markup
//   * http://www.boost.org/doc/libs/1_48_0/libs/type_traits/doc/html/index.html
//   * http://www.boost.org/doc/libs/1_48_0/libs/mpl/doc/index.html

namespace NYT {
namespace NMpl {

////////////////////////////////////////////////////////////////////////////////

//! An empty struct, a neutral typedef.
struct TEmpty
{ };

typedef int TEmpty::* TNothing;

////////////////////////////////////////////////////////////////////////////////

//! Base metaprogramming class which represents integral constant.
template <class T, T ValueOfTheConstant>
struct TIntegralConstant
{
    static /* constexpr */ const T Value = ValueOfTheConstant;

    typedef T TValueType;
    typedef TIntegralConstant<T, ValueOfTheConstant> TType;

    operator T() const
    {
        return Value;
    }
};

template <class T, T ValueOfTheConstant>
/* constexpr */ const T TIntegralConstant<T, ValueOfTheConstant>::Value;

//! Useful integral constants: True and False.
typedef TIntegralConstant<bool, true> TTrueType;
typedef TIntegralConstant<bool, false> TFalseType;

template <class T, class U> struct TIsSame : TFalseType {};
template <class T> struct TIsSame<T, T> : TTrueType {};

template <bool A, bool B> struct TAndC : TIntegralConstant<bool, A && B> {};
template <class A, class B> struct TAnd : TAndC<A::Value, B::Value> {};

template <bool A, bool B> struct TOrC : TIntegralConstant<bool, A || B> {};
template <class A, class B> struct TOr : TOrC<A::Value, B::Value> {};

//! Base metaprogramming class which represents conditionals.
template <bool B, class TIfTrue, class TIfFalse>
struct TConditional
{
    typedef TIfTrue TType;
};

template <class TIfTrue, class TIfFalse>
struct TConditional<false, TIfTrue, TIfFalse>
{
    typedef TIfFalse TType;
};

////////////////////////////////////////////////////////////////////////////////
// Const-volatile properties and transformations.

template <class T> struct TIsConst : TFalseType {};
template <class T> struct TIsConst<T const> : TTrueType {};

template <class T> struct TIsVolatile : TFalseType {};
template <class T> struct TIsVolatile<T volatile> : TTrueType {};

template <class T> struct TRemoveConst { typedef T TType; };
template <class T> struct TRemoveConst<const T> { typedef T TType; };

template <class T> struct TRemoveVolatile { typedef T TType; };
template <class T> struct TRemoveVolatile<volatile T> { typedef T TType; };

template <class T> struct TRemoveCV
{
    typedef typename TRemoveVolatile<typename TRemoveConst<T>::TType>::TType TType;
};

// TODO(sandello): add_const, add_volatile, add_cv

////////////////////////////////////////////////////////////////////////////////
// Primitive classification traits.

namespace NDetail {

template <class T> struct TIsVoidImpl : TFalseType {};
template <> struct TIsVoidImpl<void> : TTrueType {};

template <class T> struct TIsPointerImpl : TFalseType {};
template <class T> struct TIsPointerImpl<T*> : TTrueType {};

} // namespace NDetail

template <class T> struct TIsVoid
    : NDetail::TIsVoidImpl<typename TRemoveCV<T>::TType> {};

template <class T> struct TIsPointer
    : NDetail::TIsPointerImpl<typename TRemoveCV<T>::TType> {};

template <class T> struct TIsReference : TFalseType {};
template <class T> struct TIsReference<T&> : TTrueType {};
#ifndef _win_ // Somewhat broken for MSVC
template <class T> struct TIsReference<T&&> : TTrueType {};
#endif

template <class T> struct TIsLvalueReference : TFalseType {};
template <class T> struct TIsLvalueReference<T&> : TTrueType {};

template <class T> struct TIsRvalueReference : TFalseType {};
template <class T> struct TIsRvalueReference<T&&> : TTrueType {};

template <class T> struct TIsArray : public TFalseType {};
template <class T> struct TIsArray<T[]> : public TTrueType {};
template <class T, int N> struct TIsArray<T[N]> : public TTrueType {};

////////////////////////////////////////////////////////////////////////////////
// Reference transformations.

template <class T> struct TRemoveReference { typedef T TType; };
template <class T> struct TRemoveReference<T&> { typedef T TType; };
template <class T> struct TRemoveReference<T&&> { typedef T TType; };

template <class T> struct TAddLvalueReference { typedef T& TType; };
template <class T> struct TAddLvalueReference<T&> { typedef T& TType; };
template <> struct TAddLvalueReference<void> { typedef void TType; };
template <> struct TAddLvalueReference<const void> { typedef const void TType; };
template <> struct TAddLvalueReference<volatile void> { typedef volatile void TType; };
template <> struct TAddLvalueReference<const volatile void> { typedef const volatile void TType; };

template <class T> struct TAddRvalueReference { typedef T&& TType; };
template <> struct TAddRvalueReference<void> { typedef void TType; };
template <> struct TAddRvalueReference<const void> { typedef const void TType; };
template <> struct TAddRvalueReference<volatile void> { typedef volatile void TType; };
template <> struct TAddRvalueReference<const volatile void> { typedef const volatile void TType; };

template <class T> struct TRemoveExtent { typedef T TType; };
template <class T> struct TRemoveExtent<T[]> { typedef T TType; };
template <class T, int N> struct TRemoveExtent<T[N]> { typedef T TType; };

// 20.9.7.6, [meta.trans.other]
// Note on std::decay:
//   This behavior is similar to the lvalue-to-rvalue (4.1), array-to-pointer (4.2),
//   and function-to-pointer (4.3) conversions applied when an lvalue expression
//   is used as an rvalue, but also strips cv-qualifiers from class types
//   in order to more closely model by-value argument passing.
// Note on current implementation:
//   Due to lack of is_function I the following code does not work properly with
//   function pointers. Since we barely intend to decay function objects,
//   this is not crucial.

template <class T>
struct TDecay
{
private:
    typedef typename TRemoveReference<T>::TType U;
public:
    typedef typename TConditional<
        TIsArray<U>::Value,
        /* if-true  */ typename TRemoveExtent<U>::TType*,
        /* if-false */ typename TRemoveCV<U>::TType
    >::TType TType;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

typedef char (&TYesType)[1];
typedef char (&TNoType) [2];

template <class TFromType, class TToType>
struct TIsConvertibleImpl
{
    static TYesType Consumer(TToType);
    static TNoType  Consumer(...);

    static TFromType& Producer();

    enum
    {
        Value = (sizeof(Consumer(Producer())) == sizeof(TYesType))
    };
};

template <class T>
struct TIsClassImpl
{
    template <class U>
    static TYesType Test(void (U::*)());
    template <class U>
    static TNoType  Test(...);

    enum
    {
        Value = (sizeof(Test<T>(0)) == sizeof(TYesType))
    };
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! TIsConvertible<U, T>::Value is True iff #U is convertable to #T.
template <class TFromType, class TToType>
struct TIsConvertible
    : TIntegralConstant<
        bool, NDetail::TIsConvertibleImpl<TFromType, TToType>::Value
    >
{ };

//! TIsClass<T>::Value is True iff #T is a class.
template <class T>
struct TIsClass
    : TIntegralConstant<
#if defined(__GNUC__)
        bool, __is_class(T)
#else
        bool, NDetail::TIsClassImpl<T>::Value
#endif
    >
{ };

// TODO(sandello): Implement is_base_of.

////////////////////////////////////////////////////////////////////////////////

template <bool B, class TResult = void>
struct TEnableIfC
{
    typedef TResult TType;
};

template <class TResult>
struct TEnableIfC<false, TResult>
{ };

template <class TCondition, class TResult = void>
struct TEnableIf
    : public TEnableIfC<TCondition::Value, TResult>
{ };

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIsPod
    : TIntegralConstant<bool, ::TTypeTraits<T>::IsPod>
{ };

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, bool isPrimitive>
struct TCallTraitsHelper
{  };

template <class T>
struct TCallTraitsHelper<T, true>
{ 
    typedef T TType;
};

template <class T>
struct TCallTraitsHelper<T, false>
{ 
    typedef const T& TType;
};

} // namespace NDetail

//! A trait for choosing appropriate argument and return types for functions.
/*!
 *  All types except for primitive ones should be passed to functions
 *  and returned from const getters by const ref.
 */
template <class T>
struct TCallTraits
    : public NDetail::TCallTraitsHelper<T, TTypeTraits<T>::IsPrimitive>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NMpl
} // namespace NYT
