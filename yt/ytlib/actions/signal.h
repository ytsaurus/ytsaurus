#pragma once

#include "action.h"

#include "../misc/foreach.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////


//! Common base for batched callbacks.
template<class T>
class TSignalBase
{
public:

    //! Adds #action to be performed on #Fire.
    void Subscribe(typename T::TPtr action)
    {
        TGuard<TSpinLock> guard(SpinLock);
        Actions.push_back(action);
    }

    //! Removes #action from future performance.
    /*!
     *  Returns 'true' if #action was in callback list.
     */
    bool Unsubscribe(typename T::TPtr action)
    {
        TGuard<TSpinLock> guard(SpinLock);
        auto it = Find(Actions.begin(), Actions.end(), action);
        if (it == Actions.end())
            return false;
        Actions.erase(it);
        return true;
    }

protected:
    yvector<typename T::TPtr> Actions;
    TSpinLock SpinLock;

};

////////////////////////////////////////////////////////////////////////////////

//! Class for subscribtion of actions without parametres
class TSignal
    : public TSignalBase<IAction>
{
public:

    //! Calls action->Do() for all actions subscribed before
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

//! Class for subscribtion of actions with one parameter
template<class TParam>
class TParamSignal
    : public TSignalBase< IParamAction<TParam> >
{
public:

    //! Calls action->Do(#arg) for all actions subscribed before
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
