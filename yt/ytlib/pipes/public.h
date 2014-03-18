#pragma once

#include <yt/core/actions/future.h>

namespace ev {

////////////////////////////////////////////////////////////////////////////////

struct dynamic_loop;

////////////////////////////////////////////////////////////////////////////////

} // namespace ev

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): public.h must no contain definitions. Please move.

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;
    virtual void Stop() = 0;
};

typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

// TODO(babenko): use DECLARE_REFCOUNTED_XXX/DEFINE_REFCOUNTED_TYPE
class TAsyncReader;
typedef TIntrusivePtr<TAsyncReader> TAsyncReaderPtr;

class TAsyncWriter;
typedef TIntrusivePtr<TAsyncWriter> TAsyncWriterPtr;

// TODO(babenko): why do we need this custom-made smartpointer?

template <typename T>
class THolder
    : private TNonCopyable
{
public:
    THolder(int fd)
        : Object(New<T>(fd))
    {
        Object->Register();
    }

    ~THolder()
    {
        if (Object) {
            Object->Unregister();
        }
    }

    THolder(THolder<T>&& other)
    {
        if (Object) {
            Object->Unregister();
        }
        Object = std::move(other.Object);
    }

    T& operator*() const // noexcept
    {
        return *Object;
    }

    T* operator->() const // noexcept
    {
        return  Object.operator->();
    }

    TIntrusivePtr<T> GetStrongPointer()
    {
        return Object;
    }
private:
    TIntrusivePtr<T> Object;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
