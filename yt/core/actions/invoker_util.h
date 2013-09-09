#pragma once

#include "common.h"
#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Returns the synchronous invoker, i.e. the invoker whose |Invoke|
//! method invokes the closure immediately.
IInvokerPtr GetSyncInvoker();

//! Returns the current active invoker.
/*!
 *  Current invokers are maintained in a per-fiber variable that
 *  can be modified by calling #SetCurrentInvoker.
 *  
 *  Initially the sync invoker is assumed to be the current one.
 */
IInvokerPtr GetCurrentInvoker();

//! Set a given invoker as the current one.
void SetCurrentInvoker(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

//! Ensures that calls to #SetCurrentInvoker come in pairs.
class TCurrentInvokerGuard
{
public:
    explicit TCurrentInvokerGuard(IInvokerPtr newInvoker);
    ~TCurrentInvokerGuard();

private:
    IInvokerPtr OldInvoker;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
