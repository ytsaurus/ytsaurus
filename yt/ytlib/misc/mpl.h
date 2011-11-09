#pragma once

namespace NYT {
namespace NDetail {

//! TIsConvertible<U, T>::Value is True iff #S is convertable to #T.
template<class U, class T>
struct TIsConvertible
{
    typedef char (&TYes)[1];
    typedef char (&TNo) [2];

    static TYes f(T*);
    static TNo  f(...);

    enum
    {
        Value = sizeof( (f)(static_cast<U*>(0)) ) == sizeof(TYes)
    };
};

struct TEmpty
{ };

template<bool>
struct TEnableIfConvertibleImpl;

template<>
struct TEnableIfConvertibleImpl<true>
{
    typedef TEmpty TType;
};

template<>
struct TEnableIfConvertibleImpl<false>
{ };

template<class U, class T>
struct TEnableIfConvertible
    : public TEnableIfConvertibleImpl< TIsConvertible<U, T>::Value >
{ };

} // namespace NDetail
} //namespace NYT
