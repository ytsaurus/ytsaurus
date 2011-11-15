#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Represents a sliding window over an infinite sequence of slots of type T.
/*! 
 * Thread-safe cyclic buffer. 
 * T must have a default ctor and assignment operator.
 * Every new slot in a window contains newly created T.
 */
template <class T>
class TCyclicBuffer
{
public:
    TCyclicBuffer(int size)
        : Window(size)
        , CyclicStart(0)
        , WindowStart(0)
    { }

    T& operator[](int index)
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(WindowStart <= index);
        YASSERT(index < WindowStart + static_cast<int>(Window.size()));

        return Window[(CyclicStart + (index - WindowStart)) % static_cast<int>(Window.size())];
    }

    const T& operator[](int index) const
    {
        TGuard<TSpinLock> guard(SpinLock);
        YASSERT(WindowStart <= index);
        YASSERT(index < WindowStart + Window.ysize());

        return Window[(CyclicStart + (index - WindowStart)) % static_cast<int>(Window.size())];
    }

    T& Front()
    {
        return Window[CyclicStart];
    }

    const T& Front() const
    {
        return (*this)[WindowStart + Window.size() - 1];
    }

    T& Back()
    {
        return (*this)[WindowStart + Window.size() - 1];
    }

    const T& Back() const
    {
        return Window[CyclicStart];
    }

    //! Shifts sliding window for one slot.
    void Shift()
    {
        TGuard<TSpinLock> guard(SpinLock);
        Front() = T();
        ++CyclicStart;
        CyclicStart = (CyclicStart) % Window.size();
        ++WindowStart;
    }

private:
    autoarray<T> Window;

    //! Current index of the first slot in #Window
    int CyclicStart;

    //! Smallest index of the infinite sequence slot, which is currently in window.
    int WindowStart;

    TSpinLock SpinLock;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
