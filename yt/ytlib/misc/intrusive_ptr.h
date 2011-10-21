#pragma once

#include <util/system/yassert.h>
#include <util/system/defaults.h>

//! Implemntation was forked from util/generic/ptr.h

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TDefaultIntrusivePtrOps {
    public:
        static inline void Ref(T* t) throw () {
            YASSERT(t);

            t->Ref();
        }

        static inline void UnRef(T* t) throw () {
            YASSERT(t);

            t->UnRef();
        }

        static inline void DecRef(T* t) throw () {
            YASSERT(t);

            t->DecRef();
        }
};

template <
            class T,
            class Ops = TDefaultIntrusivePtrOps<T>
         >
class TIntrusivePtr: public TPointerBase<TIntrusivePtr<T, Ops>, T> {
    public:
        inline TIntrusivePtr(T* t = 0) throw ()
            : T_(t)
        {
            Ops();
            Ref();
        }

        inline ~TIntrusivePtr() throw () {
            UnRef();
        }

        inline TIntrusivePtr(const TIntrusivePtr& p) throw ()
            : T_(p.T_)
        {
            Ref();
        }

        inline TIntrusivePtr& operator= (TIntrusivePtr p) throw () {
            if (&p != this) {
                p.Swap(*this);
            }

            return *this;
        }

        inline T* Get() const throw () {
            return T_;
        }

        inline void Swap(TIntrusivePtr& r) throw () {
            DoSwap(T_, r.T_);
        }

        inline void Drop() throw () {
            TIntrusivePtr(0).Swap(*this);
        }

        inline T* Release() const throw () {
            T* res = T_;
            if (T_) {
                Ops::DecRef(T_);
                T_ = 0;
            }
            return res;
        }

    private:
        inline void Ref() throw () {
            if (T_) {
                Ops::Ref(T_);
            }
        }

        inline void UnRef() throw () {
            if (T_) {
                Ops::UnRef(T_);
            }
        }

    private:
        mutable T* T_;
};

// Behaves like TIntrusivePtr but returns const T* to prevent user from accidentally modifying the referenced object.
template <
            class T,
            class Ops = TDefaultIntrusivePtrOps<T>
         >
class TIntrusiveConstPtr {
    public:
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

        inline TIntrusiveConstPtr(const TIntrusivePtr<T, Ops>& p) throw ()
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

} //namespace NYT
