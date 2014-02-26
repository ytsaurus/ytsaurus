#pragma once

#include <yt/core/actions/future.h>

namespace ev {
    struct dynamic_loop;
}


namespace NYT {
namespace NPipes {

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;
    virtual void Stop() = 0;
};

typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

class TAsyncReader;
typedef TIntrusivePtr<TAsyncReader> TAsyncReaderPtr;

class TAsyncWriter;
typedef TIntrusivePtr<TAsyncWriter> TAsyncWriterPtr;

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

}
}
