#pragma once

#include "public.h"

#include "companion_client.h"
#include "companion_model.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <atomic>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Concrete type of the `parameters` node of a companion resource spec.
//! The spec carries it as an untyped node; #YT_FLOW_EXTEND_PARAMETERS below
//! binds this type to #TCompanionResource, and the registry parses the node
//! into it.
struct TCompanionResourceParameters
    : public NYTree::TYsonStruct
{
    //! Name of the companion-side resource class instantiated by the companion.
    std::string CompanionResourceClass;

    //! Interval between idempotent keep-alive init commands.
    TDuration KeepAliveInterval;

    //! Backoff for retrying an init command that returned an error during Load.
    TExponentialBackoffOptions InitBackoff;

    REGISTER_YSON_STRUCT(TCompanionResourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionResourceParameters);

////////////////////////////////////////////////////////////////////////////////

//! Worker-side proxy for a resource hosted inside a companion process.
/*!
 *  Drives the companion-side resource lifecycle through the ResourceExecute
 *  command envelope: init on Load, convergent init after prepared dynamic spec
 *  changes, a periodic idempotent init keep-alive and a best-effort unload on
 *  teardown.
 */
class TCompanionResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCompanionResourceParameters);

    TCompanionResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    ~TCompanionResource() override;

    //! Resolves the companion manager dependency, mints a per-resource client and
    //! initializes the resource in the companion.
    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Sends the idempotent init command for this resource through |client|,
    //! initializing it in the companion process pinned to that client's channel.
    //! |companionProcessId| identifies that process for later best-effort unload.
    //! Throws on a non-Ok response. Must be called from a fiber.
    void InitInCompanion(
        const ICompanionClientPtr& client,
        std::optional<i64> companionProcessId = std::nullopt) const;

    //! Resolves the currently managed object for this resource id. A stale
    //! object may remain referenced by a long-lived computation after preload
    //! recreation, so graph traversal must not use it directly.
    TCompanionResourcePtr GetCurrentResource() const;

    //! Returns the exact resource reference, optionally exposed under |alias|.
    TCompanionResourceInstanceReference GetReference(std::optional<TResourceId> alias = std::nullopt) const;

    //! Returns this resource and all companion-hosted dependencies in topological order.
    std::vector<TCompanionResourcePtr> GetCompanionResourceGraph() const;

    //! Returns exact references for the full dependency graph. Only this resource
    //! is exposed under |alias|; dependency-local aliases stay private.
    std::vector<TCompanionResourceInstanceReference> GetCompanionResourceReferences(
        std::optional<TResourceId> alias = std::nullopt) const;

    //! Reports the last revision accepted by the primary companion and the
    //! latest target delivered by the controller.
    TResourceRevisionState GetRevisionState() const override;

    //! Fired after a new prepared generation is accepted by the primary companion.
    DEFINE_SIGNAL(void(), CompanionStateChanged);

protected:
    //! Resolves the companion manager among |dependencies| and mints the
    //! per-resource client. Virtual for tests.
    virtual ICompanionClientPtr CreateCompanionClient(
        const THashMap<TResourceId, IResourcePtr>& dependencies) const;

    //! Materializes a controller-provided target before it is published to companions.
    /*!
     *  The default implementation returns |targetRevision| immediately. Derived
     *  resources may complete the future asynchronously and return a revision
     *  with an updated opaque spec, but must preserve the target revision id.
     */
    virtual TFuture<TResourceRevisionPtr> PrepareResourceRevision(
        const TResourceRevisionPtr& targetRevision);

private:
    struct TCompanionDependency
    {
        TResourceId ResourceId;
        TResourceId Alias;
        //! Fallback only for tests and standalone contexts without a resource manager.
        TWeakPtr<TCompanionResource> InitialResource;
    };

    struct TPublicationState
    {
        TDynamicResourceContextPtr DynamicContext;
        TResourceRevisionPtr ResourceRevision;
        ui64 ConfigurationGeneration = 0;
    };

    void DoLoad(const THashMap<TResourceId, IResourcePtr>& dependencies);
    TCompanionResourcePtr GetCurrentDependencyResource(
        const TCompanionDependency& dependency) const;
    //! Initializes this resource and its dependency graph in topological order.
    //! |publicationState|, when given, applies to this resource only;
    //! dependencies always init with their applied state.
    void InitGraphInCompanion(
        const ICompanionClientPtr& client,
        std::optional<i64> companionProcessId,
        const TPublicationState* publicationState = nullptr) const;
    void InitSelfInCompanion(
        const ICompanionClientPtr& client,
        std::optional<i64> companionProcessId,
        const TPublicationState& state) const;
    void BuildCompanionResourceGraph(
        THashSet<TResourceInstanceId>* visiting,
        THashSet<TResourceInstanceId>* collected,
        std::vector<TCompanionResourcePtr>* result) const;
    TPublicationState GetPreparedPublicationState() const;
    TPublicationState GetAppliedPublicationState() const;
    NYson::TYsonString BuildInitArgument(const TPublicationState& state) const;
    TCompanionResourceExecuteResponsePtr ExecuteCommand(
        const ICompanionClientPtr& client,
        ECompanionResourceCommand command,
        const NYson::TYsonString& argument) const;
    [[noreturn]] void ThrowCommandFailed(
        ECompanionResourceCommand command,
        const TCompanionResourceExecuteResponsePtr& response) const;
    void OnReconfigured(
        ui64 preparationEpoch,
        const TDynamicResourceContextPtr& dynamicContext);
    void ScheduleReconfiguration(const TDynamicResourceContextPtr& dynamicContext);
    void FinishPreparation(ui64 preparationEpoch);
    //! Returns the exact snapshot it wrote, or std::nullopt when the epoch is
    //! stale; the caller must publish that snapshot rather than re-read the
    //! prepared state, which may advance concurrently.
    std::optional<TPublicationState> TrySetPreparedPublicationState(
        const TDynamicResourceContextPtr& dynamicContext,
        const TResourceRevisionPtr& resourceRevision,
        std::optional<ui64> preparationEpoch,
        bool advanceGeneration);
    void PrepareAndPublish(
        ui64 preparationEpoch,
        const TDynamicResourceContextPtr& dynamicContext);
    void MarkApplied(const TPublicationState& state);
    void RegisterCompanionClient(
        const ICompanionClientPtr& client,
        std::optional<i64> companionProcessId) const;
    void KeepAlive();

    ICompanionClientPtr CompanionClient_;
    std::vector<TCompanionDependency> CompanionDependencies_;
    NConcurrency::TPeriodicExecutorPtr KeepAliveExecutor_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PublicationLock_);
    TPublicationState PreparedPublicationState_;
    std::optional<TPublicationState> AppliedPublicationState_;
    ui64 PreparationEpoch_ = 0;
    TDynamicResourceContextPtr PreparingDynamicContext_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CompanionClientsLock_);
    mutable THashMap<i64, ICompanionClientPtr> CompanionClientsByProcessId_;
    //! Accessed only from the resource invoker.
    bool FirstKeepAliveTickSkipped_ = false;
    //! Latched after the companion fences this superseded resource instance;
    //! no further init RPCs are issued for it on any path.
    std::atomic<bool> Retired_ = false;
};

DEFINE_REFCOUNTED_TYPE(TCompanionResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
