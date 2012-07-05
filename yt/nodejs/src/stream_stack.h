#pragma once

#include <ytlib/misc/mpl.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t N>
class TGrowingStreamStack
{
public:
    static_assert(N >= 1, "You have to provide a base stream to grow on.");

    TGrowingStreamStack(T* base)
        : Head(Stack + N - 1)
    {
        * Head = base;
    }

    ~TGrowingStreamStack()
    {
        T** end = Stack + N - 1;
        for (T** current = Head; current != end; ++current) {
            delete *current;
        }
    }

    template <class U>
    U* Add()
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        U* layer = new U(Top());
        *--Head = layer;
        return layer;
    }

    template <class U, class A1>
    U* Add(A1&& a1)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        U* layer = new U(Top(), ForwardRV<A1>(a1));
        *--Head = layer;
        return layer;
    }

    template <class U, class A1, class A2>
    U* Add(A1&& a1, A2&& a2)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        U* layer = new U(Top(), ForwardRV<A1>(a1), ForwardRV<A2>(a2));
        *--Head = layer;
        return layer;
    }

    template <class U, class A1, class A2, class A3>
    U* Add(A1&& a1, A2&& a2, A3&& a3)
    {
        static_assert(
            NMpl::TIsConvertible<U*, T*>::Value,
            "U* have to be convertible to T*");
        YASSERT(Head > Stack);
        U* layer = new U(Top(), ForwardRV<A1>(a1), ForwardRV<A2>(a2), ForwardRV<A3>(a3));
        *--Head = layer;
        return layer;
    }

    T* Top() const
    {
        return *(Head);
    }

    T* Bottom() const
    {
        return *(Stack + N - 1);
    }

    T* const* begin() const
    {
        return Head;
    }

    T* const* end() const
    {
        return Stack + N - 1;
    }

private:
    typedef T* TPtr;

    TPtr Stack[N];
    TPtr* Head;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
