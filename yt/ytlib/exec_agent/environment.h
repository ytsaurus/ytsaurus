#pragma once

#include "public.h"

#include <ytlib/ytree/ytree.h>
#include <ytlib/misc/error.h>
#include <ytlib/actions/signal.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IProxyController
    : public virtual TRefCounted
{
    /*!
     *  Runs job proxy.
     *  May throw exception. If no exception is thrown,
     *  Exited signal is guaranteed to be raised.
     */
    virtual void Run() = 0;

    /*!
     *  Is safe to be called anytime.
     *  Kills job proxy if it is running.
     *  
     *  Must be called from the same thread as #Run.
     */
    virtual void Kill(const TError& error) throw() = 0;

    DECLARE_INTERFACE_SIGNAL(void(TError), Exited);

    // virtual void SubscribeOnMemoryLimit(IParamAction<i64>* callback) = 0;
    // virtual bool IsRunning() const = 0;
    // virtual TError GetError() const = 0;
    // virtual void SetMemoryLimit() = 0;
    // virtual TJobStatistics GetStatistics() = 0;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Environment builder represents method of proxy execution 
 *  (e.g simple fork or within container) and related mechanisms of
 *  monitoring, isolation etc.
 */
struct IEnvironmentBuilder
    : public virtual TRefCounted
{
    virtual IProxyControllerPtr CreateProxyController(
        NYTree::INodePtr config,
        const TJobId& jobId,
        const Stroka& workingDirectory) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
