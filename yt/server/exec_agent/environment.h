#pragma once

#include "public.h"

#include <ytlib/cgroup/cgroup.h>

#include <core/ytree/node.h>
#include <core/misc/error.h>
#include <core/actions/signal.h>

namespace NYT {
namespace NExecAgent {

////////////////////////////////////////////////////////////////////////////////

struct IProxyController
    : public virtual TRefCounted
{
    /*!
     *  Runs job proxy.
     *  May throw exception.
     */
    virtual TAsyncError Run() = 0;

    /*!
     *  Is safe to be called anytime.
     *  Kills job proxy if it is running.
     *
     *  Must be called from the same thread as #Run.
     */
    virtual void Kill(const NCGroup::TNonOwningCGroup& group, const TError& error) throw() = 0;

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
        int slotId,
        const Stroka& workingDirectory) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
