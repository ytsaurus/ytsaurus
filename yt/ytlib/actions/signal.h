#pragma once

#include "common.h"
#include "action.h"

#include "../misc/foreach.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////


/*!
 * A signal represents a list of actions (either taking parameters or not).
 * A client may subscribe to a signal (adding a new handler to the list),
 * unsubscribe from a signal (removing an earlier added handler),
 * and fire a signal thus invoking the actions added so far.
 *
 * Signals are thread-safe.
 */
template<class T>
class TSignalBase
{
public:
    //! Adds a new handler to the list.
    /*!
     * \param action Handler to be added.
     */
    void Subscribe(typename T::TPtr action)
    {
        YASSERT(~action != NULL);

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
        YASSERT(~action != NULL);

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

//! A signal whose handlers are of type #IAction. \see #TSignalBase<T>.
class TSignal
    : public TSignalBase<IAction>
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

//! A signal whose handlers are of type #IParamAction<T>. \see #TSignalBase<T>
template<class TParam>
class TParamSignal
    : public TSignalBase< IParamAction<TParam> >
{
public:
    //! Calls #IParamAction::Do passing the given #arg for all actions in the list.
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

} // namespace NYT

