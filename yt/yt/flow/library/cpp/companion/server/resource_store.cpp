#include "resource_store.h"

#include "private.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/bounded_concurrency_invoker.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NCompanion;
using namespace NYTree;

constinit const auto Logger = CompanionServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Key inside the resource spec's parameters naming the companion-side class;
//! mirrors TCompanionResourceParameters on the worker side.
constexpr TStringBuf CompanionResourceClassKey = "companion_resource_class";

template <class TArg>
TArg ParseArgument(const NYson::TYsonString& argument)
{
    THROW_ERROR_EXCEPTION_UNLESS(argument,
        "Resource command argument is required");
    return ConvertTo<TArg>(argument);
}

std::string ExtractCompanionResourceClass(const TResourceSpecPtr& spec)
{
    auto child = spec->Parameters
        ? spec->Parameters->FindChild(TString(CompanionResourceClassKey))
        : nullptr;
    THROW_ERROR_EXCEPTION_UNLESS(child,
        "Resource spec parameters do not name a companion resource class under key %Qv",
        CompanionResourceClassKey);
    return child->AsString()->GetValue();
}

TResourceCommandOutcome ErrorOutcome(TError error)
{
    return TResourceCommandOutcome{
        .Status = ECompanionResourceExecuteStatus::Error,
        .Error = std::move(error),
    };
}

TResourceCommandOutcome StaleIncarnationOutcome(
    const TResourceId& resourceId,
    const TResourceInstanceId& requestedIncarnationId,
    const TResourceInstanceId& currentIncarnationId)
{
    return TResourceCommandOutcome{
        .Status = ECompanionResourceExecuteStatus::StaleResourceIncarnation,
        .Error = TError(
            "Resource %Qv incarnation %v is stale; current incarnation is %v",
            resourceId,
            requestedIncarnationId,
            currentIncarnationId),
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TResourceStore::TResourceStore(
    THashSet<std::string> resourceClassNames,
    IInvokerPtr invoker)
    : ResourceClassNames_(std::move(resourceClassNames))
    , Invoker_(std::move(invoker))
{ }

TFuture<TResourceCommandOutcome> TResourceStore::Execute(
    const TResourceId& resourceId,
    ECompanionResourceCommand command,
    const NYson::TYsonString& argument)
{
    if (command == ECompanionResourceCommand::Init) {
        auto entry = GetOrCreateEntry(resourceId);
        return BIND(&TResourceStore::DoInit, MakeStrong(this), resourceId, entry, argument)
            .AsyncVia(entry->LifecycleInvoker)
            .Run();
    }
    if (command == ECompanionResourceCommand::Unload) {
        auto entry = GetOrCreateEntry(resourceId);
        return BIND(&TResourceStore::DoUnload, MakeStrong(this), resourceId, entry, argument)
            .AsyncVia(entry->LifecycleInvoker)
            .Run();
    }
    return MakeFuture(TResourceCommandOutcome{
        .Status = ECompanionResourceExecuteStatus::Unsupported,
        .Error = TError("Unsupported resource command %Qv", command),
    });
}

IResourcePtr TResourceStore::FindInitializedResource(
    const TCompanionResourceInstanceReference& reference) const
{
    auto entry = FindEntry(reference.ResourceId);
    if (!entry) {
        return nullptr;
    }
    auto guard = Guard(entry->Lock);
    return entry->State == EState::Initialized &&
            entry->HasIncarnation &&
            entry->IncarnationId == reference.IncarnationId &&
            entry->ConfigurationGeneration == reference.ConfigurationGeneration
        ? entry->Resource
        : nullptr;
}

std::vector<TCompanionResourceInstanceReference> TResourceStore::FindUninitialized(
    const std::vector<TCompanionResourceInstanceReference>& references) const
{
    std::vector<TCompanionResourceInstanceReference> result;
    for (const auto& reference : references) {
        if (!FindInitializedResource(reference)) {
            result.push_back(reference);
        }
    }
    return result;
}

TResourceStore::TEntryPtr TResourceStore::FindEntry(const TResourceId& resourceId) const
{
    auto guard = Guard(Lock_);
    return GetOrDefault(Entries_, resourceId);
}

TResourceStore::TEntryPtr TResourceStore::GetOrCreateEntry(const TResourceId& resourceId)
{
    auto guard = Guard(Lock_);
    auto it = Entries_.find(resourceId);
    if (it == Entries_.end()) {
        auto entry = New<TEntry>();
        entry->LifecycleInvoker = NConcurrency::CreateBoundedConcurrencyInvoker(
            Invoker_,
            /*maxConcurrentInvocations*/ 1);
        it = EmplaceOrCrash(Entries_, resourceId, std::move(entry));
    }
    return it->second;
}

TResourceCommandOutcome TResourceStore::DoInit(
    TResourceId resourceId,
    TEntryPtr entry,
    NYson::TYsonString argument)
{
    try {
        auto arg = ParseArgument<TInitResourceCommandArg>(argument);
        TEntry::TAppliedSpecs incomingSpecs{
            .Spec = NYson::ConvertToYsonString(arg.Spec).ToString(),
            .DynamicSpec = NYson::ConvertToYsonString(arg.DynamicSpec).ToString(),
            .ResourceRevision = arg.ResourceRevision
                ? NYson::ConvertToYsonString(arg.ResourceRevision).ToString()
                : TString(),
        };

        // NB: Incarnation generations are monotone only within one companion
        // process lifetime: the store is in-memory and the worker-side
        // generation counter resets on worker restart. This is sound because
        // the companion process is always worker-managed, so a restarted
        // worker always talks to a freshly spawned companion with an empty
        // store.
        if (entry->HasIncarnation) {
            if (arg.IncarnationGeneration < entry->IncarnationGeneration ||
                (arg.IncarnationGeneration == entry->IncarnationGeneration &&
                    arg.IncarnationId != entry->IncarnationId) ||
                (arg.IncarnationId == entry->IncarnationId && entry->Retired))
            {
                return StaleIncarnationOutcome(
                    resourceId,
                    arg.IncarnationId,
                    entry->IncarnationId);
            }
        }

        const bool newIncarnation =
            !entry->HasIncarnation || arg.IncarnationGeneration > entry->IncarnationGeneration;
        if (newIncarnation) {
            IResourcePtr detached;
            {
                auto guard = Guard(entry->Lock);
                detached = std::move(entry->Resource);
                entry->State = EState::Registered;
                entry->IncarnationId = arg.IncarnationId;
                entry->IncarnationGeneration = arg.IncarnationGeneration;
                entry->ConfigurationGeneration = 0;
                entry->HasIncarnation = true;
                entry->Retired = false;
            }
            entry->ResetApplied();
            entry->ResetPending();
            YT_TLOG_INFO("Advancing companion resource incarnation")
                .With("ResourceId", resourceId)
                .With("IncarnationId", arg.IncarnationId)
                .With("IncarnationGeneration", arg.IncarnationGeneration)
                .With("ConfigurationGeneration", arg.ConfigurationGeneration);
            return InitializeCleanInstance(resourceId, entry, arg, incomingSpecs);
        }

        if (arg.ConfigurationGeneration < entry->ConfigurationGeneration) {
            return entry->State == EState::Initialized
                ? TResourceCommandOutcome{}
                : TResourceCommandOutcome{
                    .Status = ECompanionResourceExecuteStatus::ResourceNotInitialized,
                    .Error = TError(
                        "Resource %Qv is not initialized at configuration generation %v",
                        resourceId,
                        entry->ConfigurationGeneration),
                  };
        }

        if (entry->AppliedSpecs && entry->AppliedSpecs->Spec != incomingSpecs.Spec) {
            return ErrorOutcome(TError(
                "Static resource spec changed within resource %Qv incarnation %v",
                resourceId,
                arg.IncarnationId));
        }

        const bool dependencyReferencesChanged =
            arg.Dependencies != entry->DependencyReferences;

        // A reconfigure waiting for the hosted resource to reach its target
        // revision converges on retries of the very same init: re-poll the
        // cheap revision state instead of rebuilding the instance.
        if (entry->State == EState::Reconfiguring &&
            entry->PendingSpecs == incomingSpecs &&
            entry->PendingConfigurationGeneration == arg.ConfigurationGeneration &&
            !dependencyReferencesChanged)
        {
            return TryCommitReconfigure(resourceId, entry);
        }

        if (arg.ConfigurationGeneration == entry->ConfigurationGeneration) {
            // Conflicts are detectable only against successfully applied
            // specs; after a failed init there is nothing to conflict with
            // and the retry must be allowed to rebuild.
            if (entry->AppliedSpecs &&
                entry->AppliedSpecs->DynamicSpec != incomingSpecs.DynamicSpec)
            {
                return ErrorOutcome(TError(
                    "Resource %Qv incarnation %v has conflicting dynamic specs at "
                    "configuration generation %v",
                    resourceId,
                    arg.IncarnationId,
                    arg.ConfigurationGeneration));
            }
            if (entry->AppliedSpecs &&
                entry->AppliedSpecs->ResourceRevision != incomingSpecs.ResourceRevision)
            {
                return ErrorOutcome(TError(
                    "Resource %Qv incarnation %v has conflicting revisions at "
                    "configuration generation %v",
                    resourceId,
                    arg.IncarnationId,
                    arg.ConfigurationGeneration));
            }
            if (entry->State == EState::Initialized &&
                !dependencyReferencesChanged)
            {
                return {};
            }
            return InitializeCleanInstance(resourceId, entry, arg, incomingSpecs);
        }

        if (dependencyReferencesChanged ||
            entry->State != EState::Initialized)
        {
            return InitializeCleanInstance(resourceId, entry, arg, incomingSpecs);
        }

        return ApplyReconfigure(resourceId, entry, arg, incomingSpecs);
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Companion resource init failed")
            .With("ResourceId", resourceId)
            .With(TError(ex));
        return ErrorOutcome(TError(ex));
    }
}

TResourceCommandOutcome TResourceStore::DoUnload(
    TResourceId resourceId,
    TEntryPtr entry,
    NYson::TYsonString argument)
{
    TUnloadResourceCommandArg arg;
    try {
        arg = ParseArgument<TUnloadResourceCommandArg>(argument);
    } catch (const std::exception& ex) {
        return ErrorOutcome(TError(ex));
    }

    if (!entry->HasIncarnation) {
        {
            auto guard = Guard(entry->Lock);
            entry->State = EState::Registered;
            entry->IncarnationId = arg.IncarnationId;
            entry->HasIncarnation = true;
            entry->Retired = true;
        }
        entry->ResetApplied();
        entry->ResetPending();
        return {};
    }

    if (entry->Retired || arg.IncarnationId != entry->IncarnationId) {
        return {};
    }

    IResourcePtr detached;
    {
        auto guard = Guard(entry->Lock);
        entry->State = EState::Registered;
        detached = std::move(entry->Resource);
        entry->Retired = true;
    }
    if (detached) {
        YT_TLOG_INFO("Companion resource unloaded")
            .With("ResourceId", resourceId)
            .With("IncarnationId", arg.IncarnationId);
    }
    entry->ResetApplied();
    entry->ResetPending();
    return {};
}

IResourcePtr TResourceStore::CreateResourceInstance(
    const TResourceId& resourceId,
    const std::string& className,
    const TInitResourceCommandArg& arg) const
{
    // TResourceBase parses its parameters from the registry descriptor keyed
    // by the spec's resource_class_name, which on the wire names the
    // worker-side proxy class; substitute the companion-side class before
    // construction so the parameters parse against the hosted class.
    auto specNode = ConvertTo<IMapNodePtr>(arg.Spec);
    specNode->RemoveChild("resource_class_name");
    specNode->AddChild("resource_class_name", ConvertToNode(className));
    auto spec = ConvertTo<TResourceSpecPtr>(specNode);

    auto context = New<TResourceContext>();
    context->ResourceId = resourceId;
    context->ResourceInstanceId = arg.IncarnationId;
    context->ResourceIncarnationGeneration = arg.IncarnationGeneration;
    context->ResourceSpec = std::move(spec);
    // ResourceManager and PipelineAuthenticator stay null: a companion
    // process hosts no resource manager.
    context->Invoker = Invoker_;
    context->Logger = CompanionServerLogger().WithTag("Resource", resourceId);
    context->StatusProfiler = CreateSyncStatusProfiler(context->Logger);

    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = arg.DynamicSpec;
    dynamicContext->TargetRevision = arg.ResourceRevision;

    return TRegistry::Get()->CreateResource(context, dynamicContext);
}

TResourceCommandOutcome TResourceStore::InitializeCleanInstance(
    const TResourceId& resourceId,
    const TEntryPtr& entry,
    const TInitResourceCommandArg& arg,
    const TEntry::TAppliedSpecs& incomingSpecs)
{
    auto className = ExtractCompanionResourceClass(arg.Spec);
    if (!ResourceClassNames_.contains(className)) {
        return TResourceCommandOutcome{
            .Status = ECompanionResourceExecuteStatus::ResourceNotFound,
            .Error = TError(
                "Companion has no factory for resource class %Qv; declare it via TPipeline::AddResource",
                className),
        };
    }

    IResourcePtr detached;
    {
        auto guard = Guard(entry->Lock);
        entry->State = EState::Registered;
        detached = std::move(entry->Resource);
    }
    entry->ResetPending();

    THashMap<TResourceId, IResourcePtr> dependencies;
    std::vector<TResourceId> missingDependencyIds;
    for (const auto& reference : arg.Dependencies) {
        auto dependency = FindInitializedResource(reference);
        if (!dependency) {
            missingDependencyIds.push_back(reference.ResourceId);
            continue;
        }
        const auto& alias = reference.Alias ? *reference.Alias : reference.ResourceId;
        dependencies[alias] = std::move(dependency);
    }
    if (!missingDependencyIds.empty()) {
        return TResourceCommandOutcome{
            .Status = ECompanionResourceExecuteStatus::ResourceNotInitialized,
            .Error = TError(
                "Companion dependencies %v are not initialized for resource %Qv",
                missingDependencyIds,
                resourceId),
        };
    }

    auto resource = CreateResourceInstance(resourceId, className, arg);

    YT_TLOG_INFO("Loading companion resource")
        .With("ResourceId", resourceId)
        .With("ResourceClass", className)
        .With("IncarnationId", arg.IncarnationId)
        .With("ConfigurationGeneration", arg.ConfigurationGeneration);
    NConcurrency::WaitFor(resource->Load(dependencies))
        .ThrowOnError();

    entry->AppliedSpecs = incomingSpecs;
    entry->DependencyReferences = arg.Dependencies;
    {
        auto guard = Guard(entry->Lock);
        entry->Resource = std::move(resource);
        entry->ConfigurationGeneration = arg.ConfigurationGeneration;
        entry->State = EState::Initialized;
    }
    YT_TLOG_INFO("Companion resource initialized")
        .With("ResourceId", resourceId)
        .With("IncarnationId", arg.IncarnationId)
        .With("ConfigurationGeneration", arg.ConfigurationGeneration);
    return {};
}

TResourceCommandOutcome TResourceStore::ApplyReconfigure(
    const TResourceId& resourceId,
    const TEntryPtr& entry,
    const TInitResourceCommandArg& arg,
    const TEntry::TAppliedSpecs& incomingSpecs)
{
    auto dynamicContext = New<TDynamicResourceContext>();
    dynamicContext->DynamicResourceSpec = arg.DynamicSpec;
    dynamicContext->TargetRevision = arg.ResourceRevision;
    {
        auto guard = Guard(entry->Lock);
        entry->State = EState::Reconfiguring;
    }
    try {
        entry->Resource->Reconfigure(dynamicContext);
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Companion resource reconfigure failed")
            .With("ResourceId", resourceId)
            .With(TError(ex));
        IResourcePtr detached;
        {
            auto guard = Guard(entry->Lock);
            detached = std::move(entry->Resource);
            entry->State = EState::ReconfigureFailed;
        }
        entry->ResetPending();
        return ErrorOutcome(TError(ex));
    }

    entry->PendingSpecs = incomingSpecs;
    entry->PendingConfigurationGeneration = arg.ConfigurationGeneration;
    entry->PendingTargetRevisionId = arg.ResourceRevision
        ? std::make_optional(arg.ResourceRevision->RevisionId)
        : std::nullopt;
    return TryCommitReconfigure(resourceId, entry);
}

TResourceCommandOutcome TResourceStore::TryCommitReconfigure(
    const TResourceId& resourceId,
    const TEntryPtr& entry)
{
    YT_VERIFY(entry->PendingSpecs);

    // By contract #IResource::Reconfigure may only hand the target revision to
    // an asynchronous switch and return while the applied revision still lags.
    // Publishing the generation before the switch completes would let a batch
    // carrying it pass the store fence while user code still sees the previous
    // revision.
    if (entry->PendingTargetRevisionId) {
        auto revisionState = entry->Resource->GetRevisionState();
        // Both ids empty means the instance does not track revisions at all,
        // so its switch is instant by contract.
        const bool tracksRevisions =
            revisionState.AppliedRevisionId || revisionState.TargetRevisionId;
        if (tracksRevisions &&
            revisionState.AppliedRevisionId != entry->PendingTargetRevisionId)
        {
            YT_TLOG_DEBUG("Companion resource is still switching to the target revision")
                .With("ResourceId", resourceId)
                .With("ConfigurationGeneration", entry->PendingConfigurationGeneration)
                .With("TargetRevisionId", entry->PendingTargetRevisionId)
                .With("AppliedRevisionId", revisionState.AppliedRevisionId);
            return TResourceCommandOutcome{
                .Status = ECompanionResourceExecuteStatus::ResourceNotInitialized,
                .Error = TError(
                    "Resource %Qv has not applied target revision %v yet",
                    resourceId,
                    *entry->PendingTargetRevisionId),
            };
        }
    }

    auto configurationGeneration = entry->PendingConfigurationGeneration;
    entry->AppliedSpecs = std::move(*entry->PendingSpecs);
    entry->ResetPending();
    {
        auto guard = Guard(entry->Lock);
        entry->ConfigurationGeneration = configurationGeneration;
        entry->State = EState::Initialized;
    }
    YT_TLOG_INFO("Companion resource reconfigured")
        .With("ResourceId", resourceId)
        .With("ConfigurationGeneration", configurationGeneration);
    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
