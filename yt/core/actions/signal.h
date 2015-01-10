#pragma once

#include "callback.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  A client may subscribe to a list (adding a new handler to it),
 *  unsubscribe from it (removing an earlier added handler),
 *  and fire it thus invoking the callbacks added so far.
 *
 *  Lists are thread-safe.
 */
template <class TSignature>
class TCallbackList
{ };

template <class... TArgs>
class TCallbackList<void(TArgs...)>
{
public:
    typedef NYT::TCallback<void(TArgs...)> TCallback;

    //! Adds a new handler to the list.
    /*!
     * \param callback A handler to be added.
     */
    void Subscribe(const TCallback& callback);

    //! Removes a handler from the list.
    /*!
     * \param callback A handler to be removed.
     * \return True if #callback was in the list of handlers.
     */
    bool Unsubscribe(const TCallback& callback);

    //! Clears the list of handlers.
    void Clear();

    //! Runs all callbacks in the list.
    template <class... TCallArgs>
    void Fire(TCallArgs&&... args) const;

    //! Runs all callbacks in the list and clears the list.
    template <class... TCallArgs>
    void FireAndClear(TCallArgs&&... args);

private:
    mutable TSpinLock SpinLock_;
    std::vector<TCallback> Callbacks_;
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_SIGNAL(TSignature, name) \
protected: \
    ::NYT::TCallbackList<TSignature> name##_; \
public: \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Subscribe(callback); \
    } \
    \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        name##_.Unsubscribe(callback); \
    }

#define DECLARE_SIGNAL(TSignature, name) \
    void Subscribe##name(const ::NYT::TCallback<TSignature>& callback); \
    void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback);

#define DECLARE_INTERFACE_SIGNAL(TSignature, name) \
    virtual void Subscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0; \
    virtual void Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) = 0;

#define DELEGATE_SIGNAL(declaringType, TSignature, name, delegateTo) \
    void declaringType::Subscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        (delegateTo).Subscribe##name(callback); \
    } \
    \
    void declaringType::Unsubscribe##name(const ::NYT::TCallback<TSignature>& callback) \
    { \
        (delegateTo).Unsubscribe##name(callback); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SIGNAL_INL_H_
#include "signal-inl.h"
#undef SIGNAL_INL_H_
