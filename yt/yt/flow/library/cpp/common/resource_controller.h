#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TResourceControllerContext
    : public TRefCounted
{
    // Resource-specific identity.
    TResourceId ResourceId;
    TResourceSpecPtr ResourceSpec;

    // Common infrastructure.
    IPipelineAuthenticatorPtr PipelineAuthenticator;
    NClient::NCache::IClientsCachePtr ClientsCache;
    NHttp::IClientPtr HttpClient;
    NYPath::TRichYPath PipelinePath;
    IInvokerPtr Invoker;
    ITimeProviderPtr TimeProvider;
    IVersionProviderPtr VersionProvider;

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
    static constexpr bool SupportsFileProviderDiscovery = false;

    // Provide TParameter[Ptr] aliases. It is type of spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_PARAMETERS in derived types.
    YT_FLOW_REGISTER_PARAMETERS(NYTree::TYsonStruct);

    // Provide TDynamicParameter[Ptr] aliases. It is type of dynamic spec `Parameters` field.
    // This type is used in resource registration for future parsing.
    // It may be shadowed by macros YT_FLOW_EXTEND_DYNAMIC_PARAMETERS in derived types.
    YT_FLOW_REGISTER_DYNAMIC_PARAMETERS(NYTree::TYsonStruct);

    //! Lifetime cancellation context; cancel with a non-OK error before releasing the controller.
    virtual const TCancelableContextPtr& GetCancelableContext() const = 0;

    void Stop(const TError& error = TError(NYT::EErrorCode::Canceled, "Resource controller stopped"));

    //! Returns the first cancellation error, or null while the controller is running.
    std::optional<TError> GetStatus() const;

    //! Initializes the controller; |initContext| provides access to state persisted with the
    //! job manager state, for controllers that need to survive controller restarts.
    virtual void Init(IInitContextPtr initContext) = 0;

    //! Waits until file provider discovery has results for the current configuration.
    virtual TFuture<void> GetPreparingFuture() = 0;

    //! Returns the target revision with a stable ID until the target changes.
    //! Throws if discovery has not produced results for the current configuration.
    //! A null result is interpreted by the resource implementation.
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

    const TCancelableContextPtr& GetCancelableContext() const override;

    void Init(IInitContextPtr initContext) override;

    TFuture<void> GetPreparingFuture() override;
    TResourceRevisionPtr BuildTargetRevision() override;

    void CollectStatuses(
        const THashMap<std::string, TWorkerStatusPtr>& workerStatuses,
        const TWorkerResourceStatusPtr& controllerStatus,
        std::optional<i64> publishedRevisionId) override;

    NYTree::IMapNodePtr GetView() override;

protected:
    void OnReconfigureFail(const TError& error) override;

    TParametersPtr GetParametersBase() const override;
    TDynamicParametersPtr GetDynamicParametersBase() const override;

private:
    const TCancelableContextPtr CancelableContext_ = New<TCancelableContext>();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
