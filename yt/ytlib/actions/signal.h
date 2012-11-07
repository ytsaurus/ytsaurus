#pragma once

#include "callback.h"

#include <ytlib/misc/foreach.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  A client may subscribe to the list (adding a new handler to it),
 *  unsubscribe from it (removing an earlier added handler),
 *  and fire it thus invoking the actions added so far.
 *
 *  Lists are thread-safe.
 */
template <class Signature>
class TCallbackList;

template <class Signature>
class TCallbackListBase
{
public:
    //! Adds a new handler to the list.
    /*!
     * \param callback Handler to be added.
     */
    void Subscribe(const TCallback<Signature>& callback)
    {
        TGuard<TSpinLock> guard(SpinLock);
        Callbacks.push_back(callback);
    }

    //! Removes a handler from the list.
    /*!
     * \param callback Handler to be removed.
     * \return True if #callback was in the list of handlers.
     */
    bool Unsubscribe(const TCallback<Signature>& callback)
    {
        TGuard<TSpinLock> guard(SpinLock);
        for (auto it = Callbacks.begin(); it != Callbacks.end(); ++it) {
            if (it->Equals(callback)) {
                Callbacks.erase(it);
                return true;
            }
        }
        return false;
    }

    //! Clears the list of handlers.
    void Clear()
    {
        TGuard<TSpinLock> guard(SpinLock);
        Callbacks.clear();
    }

protected:
    mutable TSpinLock SpinLock;
    std::vector< TCallback<Signature> > Callbacks;

};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): generate with pump

template <>
class TCallbackList<void()>
    : public TCallbackListBase<void()>
{
public:
    //! Calls Run for all callbacks in the list.
    void Fire() const
    {
        TGuard<TSpinLock> guard(this->SpinLock);
        
        if (this->Callbacks.empty())
            return;

        std::vector< TCallback<void()> > callbacks(this->Callbacks);
        guard.Release();

        FOREACH (const auto& callback, callbacks) {
            callback.Run();
        }
    }
};

template <class A1>
class TCallbackList<void(A1)>
    : public TCallbackListBase<void(A1)>
{
public:
    //! Calls Run for all callbacks in the list.
    void Fire(const A1& a1) const
    {
        TGuard<TSpinLock> guard(this->SpinLock);

        if (this->Callbacks.empty())
            return;

        std::vector< TCallback<void(A1)> > callbacks(this->Callbacks);
        guard.Release();

        FOREACH (const auto& callback, callbacks) {
            callback.Run(a1);
        }
    }
};

template <class A1, class A2>
class TCallbackList<void(A1, A2)>
    : public TCallbackListBase<void(A1, A2)>
{
public:
    //! Calls Run for all callbacks in the list.
    void Fire(const A1& a1, const A2& a2) const
    {
        TGuard<TSpinLock> guard(this->SpinLock);

        if (this->Callbacks.empty())
            return;

        std::vector< TCallback<void(A1, A2)> > callbacks(this->Callbacks);
        guard.Release();

        FOREACH (const auto& callback, callbacks) {
            callback.Run(a1, a2);
        }
    }
};


////////////////////////////////////////////////////////////////////////////////

#define DEFINE_SIGNAL(signature, name) \
protected: \
    ::NYT::TCallbackList<signature> name##_; \
public: \
    void Subscribe##name(const ::NYT::TCallback<signature>& callback) \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    void Unsubscribe##name(const ::NYT::TCallback<signature>& callback) \
    { \
        name##_.Subscribe(callback); \
    }

#define DECLARE_SIGNAL(signature, name) \
    void Subscribe##name(const ::NYT::TCallback<signature>& callback); \
    void Unsubscribe##name(const ::NYT::TCallback<signature>& callback);

#define DECLARE_INTERFACE_SIGNAL(signature, name) \
    virtual void Subscribe##name(const ::NYT::TCallback<signature>& callback) = 0; \
    virtual void Unsubscribe##name(const ::NYT::TCallback<signature>& callback) = 0;

#define DELEGATE_SIGNAL(declaringType, signature, name, delegateTo) \
    void declaringType::Subscribe##name(const ::NYT::TCallback<signature>& callback) \
    { \
        (delegateTo).Subscribe##name(callback); \
    } \
    \
    void declaringType::Unsubscribe##name(const ::NYT::TCallback<signature>& callback) \
    { \
        (delegateTo).Unsubscribe##name(callback); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

