#pragma once

#include "common.h"
#include "action.h"

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
template <class T>
class TActionListBase
{
public:
    //! Adds a new handler to the list.
    /*!
     * \param action Handler to be added.
     */
    void Subscribe(typename T::TPtr action)
    {
        YASSERT(action);

        TGuard<TSpinLock> guard(SpinLock);
        Actions.push_back(action);
    }

    //! Removes a handler from the list.
    /*!
     * \param action Handler to be removed.
     * \return True if #action was in the list of handlers.
     */
    bool Unsubscribe(typename T::TPtr action)
    {
        YASSERT(action);

        TGuard<TSpinLock> guard(SpinLock);
        auto it = std::find(Actions.begin(), Actions.end(), action);
        if (it == Actions.end())
            return false;
        Actions.erase(it);
        return true;
    }

    //! Clears the list of handlers.
    void Clear()
    {
        TGuard<TSpinLock> guard(SpinLock);
        Actions.clear();
    }

protected:
    yvector<typename T::TPtr> Actions;
    TSpinLock SpinLock;

};

////////////////////////////////////////////////////////////////////////////////

//! A list whose handlers are of type #IAction. \see #TActionListBase<T>.
class TActionList
    : public TActionListBase<IAction>
{
public:
    //! Calls #IAction::Do for all actions in the list.
    void Fire()
    {
        TGuard<TSpinLock> guard(this->SpinLock);
        if (this->Actions.empty())
            return;
        yvector<IAction::TPtr> actions(this->Actions);
        guard.Release();

        FOREACH (const auto& action, actions) {
            action->Do();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): remove once new callback system is in place
//! A list whose handlers are of type #IParamAction<T>. \see #TActionListBase<T>
template <class TParam>
class TParamActionList
    : public TActionListBase< IParamAction<TParam> >
{
public:
    //! Calls #IParamAction::Do passing the given #arg to all actions in the list.
    /*!
     * \param arg Argument to be passed to the handlers.
     */
    void Fire(const TParam& arg)
    {
        TGuard<TSpinLock> guard(this->SpinLock);
        if (this->Actions.empty())
            return;
        yvector< typename IParamAction<TParam>::TPtr > actions(this->Actions);
        guard.Release();

        FOREACH (const auto& action, actions) {
            action->Do(arg);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): currently type is just ignored
#define DEFINE_SIGNAL(type, name) \
protected: \
    ::NYT::TActionList name##_; \
public: \
    void Subscribe##name(IAction::TPtr action) \
    { \
        name##_.Subscribe(action); \
    } \
    \
    void Unsubscribe##name(IAction::TPtr action) \
    { \
        name##_.Subscribe(action); \
    }

#define DECLARE_SIGNAL(type, name) \
    void Subscribe##name(IAction::TPtr action); \
    void Unsubscribe##name(IAction::TPtr action);

#define DECLARE_SIGNAL_INTERFACE(type, name) \
    virtual void Subscribe##name(IAction::TPtr action) = 0; \
    virtual void Unsubscribe##name(IAction::TPtr action) = 0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

