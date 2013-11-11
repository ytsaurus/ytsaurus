#pragma once

#include "callback.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  A client may subscribe to the list (adding a new handler to it),
 *  unsubscribe from it (removing an earlier added handler),
 *  and fire it thus invoking the handlers added so far.
 *
 *  Handlers' return values are ignored (so typically they must return |void|).
 *
 *  Lists are thread-safe.
 */
template <class TSignature>
class TCallbackList
{
public:
    //! Adds a new handler to the list.
    /*!
     * \param callback A handler to be added.
     */
    void Subscribe(const TCallback<TSignature>& callback)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        Callbacks_.push_back(callback);
    }

    //! Removes a handler from the list.
    /*!
     * \param callback A handler to be removed.
     * \return True if #callback was in the list of handlers.
     */
    bool Unsubscribe(const TCallback<TSignature>& callback)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
            if (it->Equals(callback)) {
                Callbacks_.erase(it);
                return true;
            }
        }
        return false;
    }

    //! Clears the list of handlers.
    void Clear()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        Callbacks_.clear();
    }

    //! Invokes all handlers in the list.
    template <class... TArgs>
    void Fire(TArgs&&... args) const
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (Callbacks_.empty())
            return;

        std::vector<TCallback<TSignature>> callbacks(Callbacks_);
        guard.Release();

        for (const auto& callback : callbacks) {
            // NB: Don't forward, pass as is. Makes sense when more than one handler is attached.
            callback.Run(args...);
        }
    }

private:
    mutable TSpinLock SpinLock_;
    std::vector<TCallback<TSignature>> Callbacks_;

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
        name##_.Unsubscribe(callback); \
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

