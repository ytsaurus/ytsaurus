#pragma once

#include <util/system/yassert.h>
#include <util/system/defaults.h>

#include <util/generic/ptr.h>

// Implemntation was forked from util/generic/ptr.h

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail
{

////////////////////////////////////////////////////////////////////////////////

//! TIsConvertable<U, T>::Value True iff #S is convertable to #T.
template<class U, class T>
struct TIsConvertable
{
    typedef char (&TYes)[1];
    typedef char (&TNo) [2];

    static TYes f(T*);
    static TNo  f(...);

    enum {
        Value = sizeof( (f)(static_cast<U*>(0)) ) == sizeof(TYes)
    };
};

struct TEmpty
{
};

template<bool>
struct TEnableIfConvertableImpl;

template<>
struct TEnableIfConvertableImpl<true>
{
    typedef TEmpty TType;
};

template<>
struct TEnableIfConvertableImpl<false>
{
};

template<class U, class T>
struct TEnableIfConvertable
    : public TEnableIfConvertableImpl< TIsConvertable<U, T>::Value >
{
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TIntrusivePtr
{
public:
    TIntrusivePtr()
        : T_(0)
    {
    }

    TIntrusivePtr(T* p, bool addReference = true)
        : T_(p)
    {
        if (T_ != 0 && addReference) {
            T_->Ref();
        }
    }

    TIntrusivePtr(const TIntrusivePtr& other)
        : T_(other.T_)
    {
        if (T_ != 0) {
            T_->Ref();
        }
    }

    template<class U>
    TIntrusivePtr(
        const TIntrusivePtr<U>& other,
        typename NDetail::TEnableIfConvertable<U, T>::TType = NDetail::TEmpty())
        : T_(other.Get())
    {
        if (T_ != 0) {
            T_->Ref();
        }
    }

    TIntrusivePtr(TIntrusivePtr&& other)
        : T_(other.T_)
    {
        other.T_ = 0;
    }

    ~TIntrusivePtr()
    {
        if (T_ != 0) {
            T_->UnRef();
        }
    }

    TIntrusivePtr& operator=(const TIntrusivePtr& other)
    {
        TIntrusivePtr(other).Swap(*this);
        return *this;
    }

    TIntrusivePtr& operator=(TIntrusivePtr&& other)
    {
        TIntrusivePtr(static_cast<TIntrusivePtr&&>(other)).Swap(*this);
        return *this;
    }

    void Reset()
    {
        TIntrusivePtr().Swap(*this);
    }

    void Reset(T* p)
    {
        TIntrusivePtr(p).Swap(*this);
    }

    T* Get() const {
        return T_;
    }

    T& operator*() const
    {
        YASSERT(T_ != 0);
        return *T_;
    }

    T* operator->() const
    {
        YASSERT(T_ != 0);
        return  T_;
    }

    void Swap(TIntrusivePtr& r) {
        DoSwap(T_, r.T_);
    }

private:
    T* T_;
};

// Behaves like TIntrusivePtr but returns const T* to prevent user from accidentally modifying the referenced object.
template <class T>
class TIntrusiveConstPtr {
    public:
        typedef TDefaultIntrusivePtrOps<T> Ops;

        inline TIntrusiveConstPtr(T* t = NULL) throw ()  // we need a non-const pointer to Ref(), UnRef() and eventually delete it.
            : T_(t)
        {
            Ops();
            Ref();
        }

        inline ~TIntrusiveConstPtr() throw () {
            UnRef();
        }

        inline TIntrusiveConstPtr(const TIntrusiveConstPtr& p) throw ()
            : T_(p.T_)
        {
            Ref();
        }

        inline TIntrusiveConstPtr(const TIntrusivePtr<T>& p) throw ()
            : T_(p.Get())
        {
            Ref();
        }

        inline TIntrusiveConstPtr& operator= (TIntrusiveConstPtr p) throw () {
            if (&p != this) {
                p.Swap(*this);
            }

            return *this;
        }

        inline const T* Get() const throw () {
            return T_;
        }

        inline void Swap(TIntrusiveConstPtr& r) throw () {
            DoSwap(T_, r.T_);
        }

        inline void Drop() throw () {
            TIntrusiveConstPtr(0).Swap(*this);
        }

        inline const T* operator-> () const throw () {
            return Get();
        }

        template <class C>
        inline bool operator== (const C& p) const throw () {
            return Get() == p;
        }

        template <class C>
        inline bool operator!= (const C& p) const throw () {
            return Get() != p;
        }

        inline bool operator! () const throw () {
            return Get() == NULL;
        }

        inline const T& operator* () const throw () {
            YASSERT(Get() != NULL);
            return *Get();
        }

    private:
        inline void Ref() throw () {
            if (T_ != NULL) {
                Ops::Ref(T_);
            }
        }

        inline void UnRef() throw () {
            if (T_ != NULL) {
                Ops::UnRef(T_);
            }
        }

    private:
        T* T_;
};

////////////////////////////////////////////////////////////////////////////////

template<class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs.Get() == rhs.Get();
}

template<class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs.Get() != rhs.Get();
}

template<class T, class U>
bool operator==(const TIntrusivePtr<T>& lhs, U * rhs)
{
    return lhs.Get() == rhs;
}

template<class T, class U>
bool operator!=(const TIntrusivePtr<T>& lhs, U * rhs)
{
    return lhs.Get() != rhs;
}

template<class T, class U>
bool operator==(T * lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs == rhs.Get();
}

template<class T, class U>
bool operator!=(T * lhs, const TIntrusivePtr<U>& rhs)
{
    return lhs != rhs.Get();
}

template<class T>
T* operator ~ (const TIntrusivePtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
const T* operator ~ (const TIntrusiveConstPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const TAutoPtr<T>& ptr)
{
    return ptr.Get();
}

template<class T>
T* operator ~ (const THolder<T>& ptr)
{
    return ptr.Get();
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
