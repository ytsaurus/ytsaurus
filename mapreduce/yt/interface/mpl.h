#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <
    class TBase,
    class TDerived,
    bool = TTypeTraits<TBase>::IsClassType && TTypeTraits<TDerived>::IsClassType
>
struct TIsBaseOf
{
    using TYes = char;
    typedef char (&TNo)[2];

    template <class T>
    static TYes Test(TDerived*, T);
    static TNo Test(TBase*, int);

    struct TConv
    {
        operator TBase*() const;
        operator TDerived*();
    };

    enum {
        Value = sizeof(Test(TConv(), 0)) == sizeof(TYes)
    };
};

template <class TBase, class TDerived>
struct TIsBaseOf<TBase, TDerived, false>
{
    enum {
        Value = false
    };
};

////////////////////////////////////////////////////////////////////////////////

}
