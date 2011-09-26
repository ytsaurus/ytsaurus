#pragma once

#include "action.h"

#include "../misc/foreach.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
class TSignalBase
{
public:
    void Subscribe(typename T::TPtr action)
    {
        TGuard<TSpinLock> guard(SpinLock);
        Actions.push_back(action);
    }

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

class TSignal
    : public TSignalBase<IAction>
{
public:
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

template<class TParam>
class TParamSignal
    : public TSignalBase< IParamAction<TParam> >
{
public:
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
