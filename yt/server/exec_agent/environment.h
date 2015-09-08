#pragma once

#include "public.h"

#include <ytlib/cgroup/public.h>

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
     */
    virtual TFuture<void> Run() = 0;

    /*!
     *  Safe to call anytime.
     *  Kills job proxy and all user processes if running.
     *
     *  Thread affinity: same thread as #Run.
     */
    virtual void Kill(const NCGroup::TNonOwningCGroup& group) = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyController)

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
        const TSlot& slot,
        const Stroka& workingDirectory) = 0;
};

DEFINE_REFCOUNTED_TYPE(IEnvironmentBuilder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
