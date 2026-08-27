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
    ITimeProviderPtr TimeProvider;

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
    static constexpr bool SupportsFileSourceDiscovery = false;

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

    //! Builds the target revision to broadcast to the worker-side instances.
    /*!
     *  Null means "nothing to publish"; the resource is then absent from the broadcast map.
     *  A result that differs from the previously built one becomes a new revision.
     *
     *  Discover external state in the background; only read its cached result here.
     */
    virtual TResourceRevisionPtr BuildTargetRevision() = 0;

    //! Receives the current feedback from alive workers (keyed by worker address) plus the status
    //! of the controller-side resource instance (null when it has nothing to report).
    //! |publishedRevisionId| identifies the target currently broadcast to workers and is null
    //! when there is no published target.
    virtual void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus,
        std::optional<i64> publishedRevisionId) = 0;

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

    TResourceRevisionPtr BuildTargetRevision() override;

    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus,
        std::optional<i64> publishedRevisionId) override;

    NYTree::IMapNodePtr GetView() override;

protected:
    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
