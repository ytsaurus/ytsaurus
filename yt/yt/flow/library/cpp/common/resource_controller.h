#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TResourceControllerContext
    : public TRefCounted
{
    // Resource-specific identity.
    TResourceId ResourceId;
    TResourceSpecPtr ResourceSpec;

    // Common infrastructure.
    NClient::NCache::IClientsCachePtr ClientsCache;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr Invoker;

    // Observability.
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
    IStatusProfilerPtr StatusProfiler;
};

DEFINE_REFCOUNTED_TYPE(TResourceControllerContext);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicResourceControllerContext
    : public TRefCounted
{
    TDynamicResourceSpecPtr DynamicResourceSpec;
};

DEFINE_REFCOUNTED_TYPE(TDynamicResourceControllerContext);

////////////////////////////////////////////////////////////////////////////////

//! Controller-side part of a resource.
/*!
 *  A resource class associates a controller via its TController typedef. The controller runs on
 *  the pipeline controller: it periodically builds the target revision of its resource, observes
 *  which revisions the worker-side instances actually serve, and reflects its state into the
 *  flow view.
 *
 *  The methods are invoked synchronously on every iteration of the controller main cycle, so
 *  they must be cheap and non-blocking.
 */
struct IResourceController
    : public TRefCounted
    , public virtual TReconfigurable<TDynamicResourceControllerContext>
{
    // Provide TParameter[Ptr] aliases. It is type of spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(NYTree::TYsonStruct);

    // Provide TDynamicParameter[Ptr] aliases. It is type of dynamic spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_DYNAMIC_PARAMETERS in derived types.
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(NYTree::TYsonStruct);

    //! Initializes the controller; |initContext| provides access to state persisted with the
    //! job manager state, for controllers that need to survive controller restarts.
    virtual void Init(IInitContextPtr initContext) = 0;

    //! Builds the spec of the target revision to broadcast to the worker-side instances.
    /*!
     *  Null means "nothing to publish"; the resource is then absent from the broadcast map.
     *  A result that differs from the previously built one becomes a new revision.
     *
     *  Discover external state in the background; only read its cached result here.
     */
    virtual NYTree::INodePtr BuildTargetRevisionSpec() = 0;

    //! Receives the current statuses of this resource: a full snapshot over the alive workers
    //! (keyed by worker address) plus the status of the controller-side instance (null when it
    //! has nothing to report).
    virtual void CollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) = 0;

    //! State reflected into the flow view. Null means "nothing to show".
    virtual NYTree::IMapNodePtr GetView() = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceController);

////////////////////////////////////////////////////////////////////////////////

//! Controller of a resource class that declares none: publishes nothing and observes nothing.
class TNullResourceController
    : public IResourceController
{
public:
    TNullResourceController(TResourceControllerContextPtr context, TDynamicResourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;

    NYTree::INodePtr BuildTargetRevisionSpec() override;

    void CollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus) override;

    NYTree::IMapNodePtr GetView() override;

protected:
    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
