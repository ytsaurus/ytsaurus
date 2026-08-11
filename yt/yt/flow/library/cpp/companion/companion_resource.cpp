#include "companion_resource.h"

#include "companion_manager.h"
#include "companion_model.h"

#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCompanionResourceCommandException
    : public TErrorException
{
public:
    explicit TCompanionResourceCommandException(ECompanionResourceExecuteStatus status)
        : Status(status)
    { }

    const ECompanionResourceExecuteStatus Status;
};

//! Statuses that no retry of the same init can fix.
bool IsPermanentCommandStatus(ECompanionResourceExecuteStatus status)
{
    return status == ECompanionResourceExecuteStatus::ResourceNotFound ||
        status == ECompanionResourceExecuteStatus::Unsupported ||
        status == ECompanionResourceExecuteStatus::StaleResourceIncarnation;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TCompanionResourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("companion_resource_class", &TThis::CompanionResourceClass);
    // A non-positive period would make TPeriodicExecutor re-fire immediately,
    // turning the keep-alive into an unpaced init RPC loop.
    registrar.Parameter("keep_alive_interval", &TThis::KeepAliveInterval)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("init_backoff", &TThis::InitBackoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = 5,
            .MinBackoff = TDuration::Seconds(1),
            .MaxBackoff = TDuration::Seconds(30),
        });
}

////////////////////////////////////////////////////////////////////////////////

TCompanionResource::TCompanionResource(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(std::move(context), std::move(dynamicContext))
{ }

TCompanionResource::~TCompanionResource()
{
    if (KeepAliveExecutor_) {
        YT_UNUSED_FUTURE(KeepAliveExecutor_->Stop());
    }
    std::vector<ICompanionClientPtr> clients;
    if (CompanionClient_) {
        clients.push_back(CompanionClient_);
    }
    {
        auto guard = Guard(CompanionClientsLock_);
        for (const auto& entry : CompanionClientsByProcessId_) {
            const auto& client = entry.second;
            if (client != CompanionClient_) {
                clients.push_back(client);
            }
        }
    }
    if (clients.empty()) {
        return;
    }
    TUnloadResourceCommandArg arg;
    arg.IncarnationId = GetContext()->ResourceInstanceId;
    auto argument = NYson::ConvertToYsonString(arg);
    // Detached best-effort unload captures no |this|. The incarnation fence
    // makes a delayed unload harmless after a successor has initialized.
    for (const auto& client : clients) {
        GetContext()->Invoker->Invoke(BIND([
            client,
            resourceId = GetContext()->ResourceId,
            argument,
            logger = Logger
        ] {
            const auto& Logger = logger;
            try {
                auto response = NConcurrency::WaitFor(
                    client->ResourceExecute(resourceId, ECompanionResourceCommand::Unload, argument))
                    .ValueOrThrow();
                if (response->Status != ECompanionResourceExecuteStatus::Ok) {
                    YT_TLOG_WARNING("Best-effort companion resource unload failed")
                        .With("ResourceId", resourceId)
                        .With("Status", response->Status)
                        .With(response->Error);
                }
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Best-effort companion resource unload failed")
                    .With("ResourceId", resourceId)
                    .With(TError(ex));
            }
        }));
    }
}

TFuture<void> TCompanionResource::Load(const THashMap<TResourceId, IResourcePtr>& dependencies)
{
    return BIND(&TCompanionResource::DoLoad, MakeStrong(this), dependencies)
        .AsyncVia(GetContext()->Invoker)
        .Run();
}

ICompanionClientPtr TCompanionResource::CreateCompanionClient(
    const THashMap<TResourceId, IResourcePtr>& dependencies) const
{
    auto it = dependencies.find(TResourceId(CompanionManagerAlias));
    if (it == dependencies.end()) {
        THROW_ERROR_EXCEPTION(
            "Companion resource must declare a dependency on a companion manager under alias %Qv",
            CompanionManagerAlias)
            .With("resource_id", GetContext()->ResourceId);
    }
    return it->second->As<TCompanionManager>()->CreateCompanionClient(
        GetContext()->StatusProfiler->WithPrefix("/companion_resource_client"));
}

TFuture<TResourceRevisionPtr> TCompanionResource::PrepareResourceRevision(
    const TResourceRevisionPtr& targetRevision)
{
    return MakeFuture(targetRevision);
}

void TCompanionResource::DoLoad(const THashMap<TResourceId, IResourcePtr>& dependencies)
{
    CompanionClient_ = CreateCompanionClient(dependencies);

    for (const auto& [resourceId, description] : GetSpec()->Dependencies) {
        auto alias = description->Alias ? *description->Alias : resourceId;
        auto it = dependencies.find(alias);
        if (it == dependencies.end()) {
            THROW_ERROR_EXCEPTION("Resource dependency is missing during companion resource load")
                .With("resource_id", GetContext()->ResourceId)
                .With("dependency_id", resourceId)
                .With("dependency_alias", alias);
        }
        if (auto companionResource = it->second->TryAs<TCompanionResource>()) {
            CompanionDependencies_.push_back({
                .ResourceId = resourceId,
                .Alias = alias,
                .InitialResource = MakeWeak(companionResource.Get()),
            });
        }
    }
    std::sort(
        CompanionDependencies_.begin(),
        CompanionDependencies_.end(),
        [] (const TCompanionDependency& lhs, const TCompanionDependency& rhs) {
            return lhs.ResourceId < rhs.ResourceId;
        });

    auto dynamicContext = GetDynamicContext();
    auto resourceRevision = NConcurrency::WaitFor(
        PrepareResourceRevision(dynamicContext->TargetRevision))
        .ValueOrThrow();
    auto publicationState = TrySetPreparedPublicationState(
        dynamicContext,
        resourceRevision,
        std::nullopt,
        false);
    YT_VERIFY(publicationState);

    auto backoffStrategy = TBackoffStrategy(GetParameters()->InitBackoff);
    // Rethrows the exception in flight when the backoff is exhausted.
    auto backoffOrRethrow = [&] (const std::exception& ex) {
        if (!backoffStrategy.Next()) {
            throw;
        }
        auto backoff = backoffStrategy.GetBackoff();
        YT_TLOG_WARNING("Companion resource init failed, retrying")
            .With("ResourceId", GetContext()->ResourceId)
            .With("Attempt", backoffStrategy.GetInvocationIndex())
            .With("SleepDuration", backoff)
            .With(TError(ex));
        NConcurrency::TDelayedExecutor::WaitForDuration(backoff);
    };
    while (true) {
        try {
            InitGraphInCompanion(CompanionClient_, std::nullopt, &*publicationState);
            MarkApplied(*publicationState);
            break;
        } catch (const TCompanionResourceCommandException& ex) {
            if (IsPermanentCommandStatus(ex.Status)) {
                throw;
            }
            backoffOrRethrow(ex);
        } catch (const std::exception& ex) {
            backoffOrRethrow(ex);
        }
    }

    YT_TLOG_INFO("Companion resource initialized in companion")
        .With("ResourceId", GetContext()->ResourceId)
        .With("CompanionResourceClass", GetParameters()->CompanionResourceClass);

    SubscribeReconfigured(BIND([weakThis = MakeWeak(this)] (
        const TDynamicResourceContextPtr& dynamicContext) {
        if (auto this_ = weakThis.Lock()) {
            this_->ScheduleReconfiguration(dynamicContext);
        }
    }));

    // Close the gap between the initial context snapshot and the subscription.
    ScheduleReconfiguration(GetDynamicContext());

    KeepAliveExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TCompanionResource::KeepAlive, MakeWeak(this)),
        GetParameters()->KeepAliveInterval);
    KeepAliveExecutor_->Start();
}

void TCompanionResource::InitInCompanion(
    const ICompanionClientPtr& client,
    std::optional<i64> companionProcessId) const
{
    GetCurrentResource()->InitGraphInCompanion(client, companionProcessId);
}

TCompanionResourcePtr TCompanionResource::GetCurrentResource() const
{
    auto resourceManager = GetContext()->ResourceManager.Lock();
    if (!resourceManager) {
        return MakeStrong(const_cast<TCompanionResource*>(this));
    }
    NConcurrency::WaitFor(resourceManager->Load(GetContext()->ResourceId))
        .ThrowOnError();
    return resourceManager->Get(GetContext()->ResourceId)->As<TCompanionResource>();
}

TCompanionResourcePtr TCompanionResource::GetCurrentDependencyResource(
    const TCompanionDependency& dependency) const
{
    if (auto resourceManager = GetContext()->ResourceManager.Lock()) {
        NConcurrency::WaitFor(resourceManager->Load(dependency.ResourceId))
            .ThrowOnError();
        return resourceManager->Get(dependency.ResourceId)->As<TCompanionResource>();
    }

    auto initialResource = dependency.InitialResource.Lock();
    THROW_ERROR_EXCEPTION_UNLESS(initialResource,
        "Companion resource dependency object is no longer available")
        .With("resource_id", GetContext()->ResourceId)
        .With("dependency_id", dependency.ResourceId);
    return initialResource;
}

void TCompanionResource::InitGraphInCompanion(
    const ICompanionClientPtr& client,
    std::optional<i64> companionProcessId,
    const TPublicationState* publicationState) const
{
    THashSet<TResourceInstanceId> visiting;
    THashSet<TResourceInstanceId> collected;
    std::vector<TCompanionResourcePtr> graph;
    BuildCompanionResourceGraph(&visiting, &collected, &graph);
    for (const auto& resource : graph) {
        auto state = resource.Get() == this && publicationState
            ? *publicationState
            : resource->GetAppliedPublicationState();
        resource->InitSelfInCompanion(client, companionProcessId, state);
    }
}

void TCompanionResource::InitSelfInCompanion(
    const ICompanionClientPtr& client,
    std::optional<i64> companionProcessId,
    const TPublicationState& state) const
{
    if (Retired_.load()) {
        throw TCompanionResourceCommandException(
            ECompanionResourceExecuteStatus::StaleResourceIncarnation) <<= TError("Companion resource incarnation is retired")
            .With("resource_id", GetContext()->ResourceId)
            .With("resource_instance_id", GetContext()->ResourceInstanceId);
    }
    RegisterCompanionClient(client, companionProcessId);
    auto response = ExecuteCommand(
        client,
        ECompanionResourceCommand::Init,
        BuildInitArgument(state));
    if (response->Status != ECompanionResourceExecuteStatus::Ok) {
        ThrowCommandFailed(ECompanionResourceCommand::Init, response);
    }
}

TCompanionResourceInstanceReference TCompanionResource::GetReference(std::optional<TResourceId> alias) const
{
    auto publicationState = GetAppliedPublicationState();
    TCompanionResourceInstanceReference reference;
    reference.ResourceId = GetContext()->ResourceId;
    reference.IncarnationId = GetContext()->ResourceInstanceId;
    reference.ConfigurationGeneration = publicationState.ConfigurationGeneration;
    reference.Alias = std::move(alias);
    return reference;
}

std::vector<TCompanionResourcePtr> TCompanionResource::GetCompanionResourceGraph() const
{
    auto current = GetCurrentResource();
    if (current.Get() != this) {
        return current->GetCompanionResourceGraph();
    }
    THashSet<TResourceInstanceId> visiting;
    THashSet<TResourceInstanceId> collected;
    std::vector<TCompanionResourcePtr> result;
    BuildCompanionResourceGraph(&visiting, &collected, &result);
    return result;
}

void TCompanionResource::BuildCompanionResourceGraph(
    THashSet<TResourceInstanceId>* visiting,
    THashSet<TResourceInstanceId>* collected,
    std::vector<TCompanionResourcePtr>* result) const
{
    const auto& instanceId = GetContext()->ResourceInstanceId;
    if (collected->contains(instanceId)) {
        return;
    }
    if (!visiting->insert(instanceId).second) {
        THROW_ERROR_EXCEPTION("Cycle in companion resource dependency graph")
            .With("resource_id", GetContext()->ResourceId)
            .With("resource_instance_id", instanceId);
    }
    for (const auto& dependency : CompanionDependencies_) {
        auto resource = GetCurrentDependencyResource(dependency);
        resource->BuildCompanionResourceGraph(visiting, collected, result);
    }
    visiting->erase(instanceId);
    collected->insert(instanceId);
    result->push_back(MakeStrong(const_cast<TCompanionResource*>(this)));
}

std::vector<TCompanionResourceInstanceReference> TCompanionResource::GetCompanionResourceReferences(
    std::optional<TResourceId> alias) const
{
    auto current = GetCurrentResource();
    auto resources = current->GetCompanionResourceGraph();
    std::vector<TCompanionResourceInstanceReference> references;
    references.reserve(resources.size());
    for (const auto& resource : resources) {
        references.push_back(resource->GetReference(
            resource == current ? alias : std::nullopt));
    }
    return references;
}

TCompanionResource::TPublicationState TCompanionResource::GetPreparedPublicationState() const
{
    auto guard = Guard(PublicationLock_);
    return PreparedPublicationState_;
}

TCompanionResource::TPublicationState TCompanionResource::GetAppliedPublicationState() const
{
    auto guard = Guard(PublicationLock_);
    THROW_ERROR_EXCEPTION_UNLESS(
        AppliedPublicationState_,
        "Companion resource has no applied publication state")
        .With("resource_id", GetContext()->ResourceId);
    return *AppliedPublicationState_;
}

NYson::TYsonString TCompanionResource::BuildInitArgument(
    const TPublicationState& state) const
{
    TInitResourceCommandArg arg;
    arg.Spec = GetSpec();
    arg.DynamicSpec = state.DynamicContext->DynamicResourceSpec;
    arg.IncarnationId = GetContext()->ResourceInstanceId;
    arg.IncarnationGeneration = GetContext()->ResourceIncarnationGeneration;
    arg.ConfigurationGeneration = state.ConfigurationGeneration;
    arg.ResourceRevision = state.ResourceRevision;
    arg.Dependencies.reserve(CompanionDependencies_.size());
    for (const auto& dependency : CompanionDependencies_) {
        auto resource = GetCurrentDependencyResource(dependency);
        arg.Dependencies.push_back(resource->GetReference(dependency.Alias));
    }
    return NYson::ConvertToYsonString(arg);
}

TCompanionResourceExecuteResponsePtr TCompanionResource::ExecuteCommand(
    const ICompanionClientPtr& client,
    ECompanionResourceCommand command,
    const NYson::TYsonString& argument) const
{
    return NConcurrency::WaitFor(
        client->ResourceExecute(GetContext()->ResourceId, command, argument))
        .ValueOrThrow();
}

void TCompanionResource::ThrowCommandFailed(
    ECompanionResourceCommand command,
    const TCompanionResourceExecuteResponsePtr& response) const
{
    auto message = response->Status == ECompanionResourceExecuteStatus::ResourceNotFound
        ? "Companion has no factory for the companion resource class"
        : "Companion resource command failed";
    throw TCompanionResourceCommandException(response->Status) <<= TError("%v", message)
        .With("resource_id", GetContext()->ResourceId)
        .With("companion_resource_class", GetParameters()->CompanionResourceClass)
        .With("command", command)
        .With("status", response->Status)
        .With(response->Error);
}

TResourceRevisionState TCompanionResource::GetRevisionState() const
{
    auto dynamicContext = GetDynamicContext();
    auto guard = Guard(PublicationLock_);
    return {
        .AppliedRevisionId = AppliedPublicationState_ && AppliedPublicationState_->ResourceRevision
            ? std::make_optional(AppliedPublicationState_->ResourceRevision->RevisionId)
            : std::nullopt,
        .TargetRevisionId = dynamicContext->TargetRevision
            ? std::make_optional(dynamicContext->TargetRevision->RevisionId)
            : std::nullopt,
    };
}

std::optional<TCompanionResource::TPublicationState> TCompanionResource::TrySetPreparedPublicationState(
    const TDynamicResourceContextPtr& dynamicContext,
    const TResourceRevisionPtr& resourceRevision,
    std::optional<ui64> preparationEpoch,
    bool advanceGeneration)
{
    if (dynamicContext->TargetRevision) {
        THROW_ERROR_EXCEPTION_UNLESS(resourceRevision,
            "Resource revision preparation returned no revision for a non-empty target")
            .With("resource_id", GetContext()->ResourceId)
            .With("target_revision_id", dynamicContext->TargetRevision->RevisionId);
        THROW_ERROR_EXCEPTION_UNLESS(
            resourceRevision->RevisionId == dynamicContext->TargetRevision->RevisionId,
            "Prepared resource revision id differs from the target revision id")
            .With("resource_id", GetContext()->ResourceId)
            .With("target_revision_id", dynamicContext->TargetRevision->RevisionId)
            .With("prepared_revision_id", resourceRevision->RevisionId);
    } else {
        THROW_ERROR_EXCEPTION_UNLESS(!resourceRevision,
            "Resource revision preparation returned a revision for an empty target")
            .With("resource_id", GetContext()->ResourceId)
            .With("prepared_revision_id", resourceRevision->RevisionId);
    }

    auto guard = Guard(PublicationLock_);
    if (preparationEpoch && *preparationEpoch != PreparationEpoch_) {
        return std::nullopt;
    }
    PreparedPublicationState_.DynamicContext = dynamicContext;
    PreparedPublicationState_.ResourceRevision = resourceRevision;
    if (advanceGeneration) {
        ++PreparedPublicationState_.ConfigurationGeneration;
    }
    return PreparedPublicationState_;
}

void TCompanionResource::PrepareAndPublish(
    ui64 preparationEpoch,
    const TDynamicResourceContextPtr& dynamicContext)
{
    auto resourceRevision = NConcurrency::WaitFor(
        PrepareResourceRevision(dynamicContext->TargetRevision))
        .ValueOrThrow();
    auto publicationState = TrySetPreparedPublicationState(
        dynamicContext,
        resourceRevision,
        preparationEpoch,
        true);
    if (!publicationState) {
        YT_TLOG_DEBUG("Ignoring obsolete prepared companion resource revision")
            .With("ResourceId", GetContext()->ResourceId)
            .With("PreparationEpoch", preparationEpoch);
        return;
    }

    InitGraphInCompanion(CompanionClient_, std::nullopt, &*publicationState);
    MarkApplied(*publicationState);
}

void TCompanionResource::MarkApplied(const TPublicationState& state)
{
    bool changed = false;
    {
        auto guard = Guard(PublicationLock_);
        if (!AppliedPublicationState_ ||
            state.ConfigurationGeneration > AppliedPublicationState_->ConfigurationGeneration)
        {
            AppliedPublicationState_ = state;
            changed = true;
        }
    }
    if (changed) {
        CompanionStateChanged_.Fire();
    }
}

void TCompanionResource::OnReconfigured(
    ui64 preparationEpoch,
    const TDynamicResourceContextPtr& dynamicContext)
{
    try {
        PrepareAndPublish(preparationEpoch, dynamicContext);
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to prepare or publish companion resource revision")
            .With("ResourceId", GetContext()->ResourceId)
            .With(TError(ex));
    }
    FinishPreparation(preparationEpoch);
}

void TCompanionResource::ScheduleReconfiguration(
    const TDynamicResourceContextPtr& dynamicContext)
{
    ui64 preparationEpoch;
    {
        auto guard = Guard(PublicationLock_);
        if (PreparedPublicationState_.DynamicContext == dynamicContext ||
            PreparingDynamicContext_ == dynamicContext)
        {
            return;
        }
        PreparingDynamicContext_ = dynamicContext;
        preparationEpoch = ++PreparationEpoch_;
    }
    GetContext()->Invoker->Invoke(BIND(
        &TCompanionResource::OnReconfigured,
        MakeWeak(this),
        preparationEpoch,
        dynamicContext));
}

void TCompanionResource::FinishPreparation(ui64 preparationEpoch)
{
    auto guard = Guard(PublicationLock_);
    if (preparationEpoch == PreparationEpoch_) {
        PreparingDynamicContext_.Reset();
    }
}

void TCompanionResource::RegisterCompanionClient(
    const ICompanionClientPtr& client,
    std::optional<i64> companionProcessId) const
{
    if (!companionProcessId) {
        return;
    }
    auto guard = Guard(CompanionClientsLock_);
    CompanionClientsByProcessId_[*companionProcessId] = client;
}

void TCompanionResource::KeepAlive()
{
    if (Retired_.load()) {
        return;
    }
    // The periodic executor kickstarts immediately after Start; the resource was
    // initialized just before that, so the first tick carries no information.
    if (!std::exchange(FirstKeepAliveTickSkipped_, true)) {
        return;
    }
    try {
        auto dynamicContext = GetDynamicContext();
        bool needsPreparation;
        {
            auto guard = Guard(PublicationLock_);
            needsPreparation = PreparedPublicationState_.DynamicContext != dynamicContext;
        }
        if (needsPreparation) {
            ScheduleReconfiguration(dynamicContext);
            return;
        }
        // Idempotent: no-ops when the specs are already applied, retries a
        // previously failed reconfigure and heals the resource channel process
        // after a restart even when no batches flow.
        auto publicationState = GetPreparedPublicationState();
        InitGraphInCompanion(CompanionClient_, std::nullopt, &publicationState);
        MarkApplied(publicationState);
    } catch (const TCompanionResourceCommandException& ex) {
        if (ex.Status == ECompanionResourceExecuteStatus::StaleResourceIncarnation) {
            Retired_.store(true);
            YT_UNUSED_FUTURE(KeepAliveExecutor_->Stop());
            YT_TLOG_INFO("Stopping keep-alive for stale companion resource incarnation")
                .With("ResourceId", GetContext()->ResourceId)
                .With(TError(ex));
            return;
        }
        YT_TLOG_WARNING("Companion resource keep-alive failed")
            .With("ResourceId", GetContext()->ResourceId)
            .With(TError(ex));
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Companion resource keep-alive failed")
            .With("ResourceId", GetContext()->ResourceId)
            .With(TError(ex));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
