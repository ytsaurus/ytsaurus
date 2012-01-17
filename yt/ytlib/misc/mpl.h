#pragma once

namespace NYT {
namespace NMPL {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty
{ };

template<class T, T CompileTimeValue>
struct TIntegralConstant
{
    static const T Value = CompileTimeValue;

    typedef T TValueType;
    typedef TIntegralConstant<T, CompileTimeValue> TType;
};

template<class T, T CompileTimeValue>
const T TIntegralConstant<T, CompileTimeValue>::Value;

typedef TIntegralConstant<bool, true> TTrueType;
typedef TIntegralConstant<bool, false> TFalseType;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

typedef char (&TYesType)[1];
typedef char (&TNoType) [2];

//! TIsConvertible<U, T>::Value is True iff #U is convertable to #T.
template<class TFromType, class TToType>
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template<class TFromType, class TToType>
struct TIsConvertible
    : TIntegralConstant<
        bool, NDetail::TIsConvertibleImpl<TFromType, TToType>::Value
    >
{ };

template <bool B, class TResult = void>
struct TEnableIfC
{
    typedef TResult TType;
};

template <class TResult>
struct TEnableIfC<false, TResult>
{ };

template<class TCondition, class TResult = void>
struct TEnableIf
    : public TEnableIfC<TCondition::Value, TResult>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NMPL
} // namespace NYT
