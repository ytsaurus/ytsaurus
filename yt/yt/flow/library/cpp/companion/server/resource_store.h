#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/companion/companion_model.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Outcome of one resource command. User-code failures travel in-band as
//! statuses (the ResourceExecute RPC itself succeeds), so the worker can
//! distinguish them from transport failures.
struct TResourceCommandOutcome
{
    NCompanion::ECompanionResourceExecuteStatus Status =
        NCompanion::ECompanionResourceExecuteStatus::Ok;
    TError Error;
};

////////////////////////////////////////////////////////////////////////////////

//! Process-wide store of the resources hosted inside this companion, keyed by
//! resource id. Owned by the companion service for its entire lifetime:
//! resources are process-scoped and shared by every job that requires them.
class TResourceStore
    : public TRefCounted
{
public:
    //! |resourceClassNames| are the classes declared via
    //! #TPipeline::AddResource; only they may be instantiated by the "init"
    //! command. |invoker| must support fibers; per-resource-id lifecycle
    //! commands are serialized on top of it.
    TResourceStore(
        THashSet<std::string> resourceClassNames,
        IInvokerPtr invoker);

    //! Dispatches one ResourceExecute command. The returned future carries
    //! user-code failures in-band and only fails on companion bugs.
    TFuture<TResourceCommandOutcome> Execute(
        const TResourceId& resourceId,
        NCompanion::ECompanionResourceCommand command,
        const NYson::TYsonString& argument);

    //! Returns the initialized instance matching |reference| exactly, or null.
    //! Cheap and non-blocking; safe on the batch hot path.
    IResourcePtr FindInitializedResource(
        const NCompanion::TCompanionResourceInstanceReference& reference) const;

    //! Returns the exact references that are not initialized in this process.
    std::vector<NCompanion::TCompanionResourceInstanceReference> FindUninitialized(
        const std::vector<NCompanion::TCompanionResourceInstanceReference>& references) const;

private:
    enum class EState
    {
        Registered,
        Initialized,
        Reconfiguring,
        ReconfigureFailed,
    };

    struct TEntry
        : public TRefCounted
    {
        //! Canonical YSON of the specs the current instance was successfully
        //! initialized from. Both sides of every comparison are produced by
        //! the same serialization of the parsed argument, and within one
        //! incarnation the worker serializes the same spec objects, so string
        //! equality is exact.
        struct TAppliedSpecs
        {
            TString Spec;
            TString DynamicSpec;
            //! Empty when the init carried no revision.
            TString ResourceRevision;

            bool operator==(const TAppliedSpecs& other) const = default;
        };

        //! Admits one lifecycle command at a time. Bounded-concurrency rather
        //! than serialized: command bodies suspend on WaitFor while awaiting
        //! #IResource::Load, and a serialized invoker would admit the next
        //! callback at the suspension point.
        IInvokerPtr LifecycleInvoker;

        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
        //! Published under |Lock| for cross-thread readers; mutated only from
        //! |LifecycleInvoker|.
        EState State = EState::Registered;
        IResourcePtr Resource;
        TResourceInstanceId IncarnationId;
        ui64 IncarnationGeneration = 0;
        ui64 ConfigurationGeneration = 0;
        bool HasIncarnation = false;
        bool Retired = false;

        //! Touched only from |LifecycleInvoker|; set only after a successful
        //! Load or a committed Reconfigure.
        std::optional<TAppliedSpecs> AppliedSpecs;
        std::vector<NCompanion::TCompanionResourceInstanceReference> DependencyReferences;

        //! Touched only from |LifecycleInvoker|; set while the entry is
        //! #EState::Reconfiguring and the hosted resource has not reached the
        //! requested target revision yet. Holds what a later retry commits.
        std::optional<TAppliedSpecs> PendingSpecs;
        ui64 PendingConfigurationGeneration = 0;
        //! Empty when the pending init carried no revision.
        std::optional<i64> PendingTargetRevisionId;

        void ResetApplied()
        {
            AppliedSpecs.reset();
            DependencyReferences.clear();
        }

        void ResetPending()
        {
            PendingSpecs.reset();
            PendingConfigurationGeneration = 0;
            PendingTargetRevisionId.reset();
        }
    };

    using TEntryPtr = TIntrusivePtr<TEntry>;

    const THashSet<std::string> ResourceClassNames_;
    const IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TResourceId, TEntryPtr> Entries_;

    TEntryPtr FindEntry(const TResourceId& resourceId) const;
    TEntryPtr GetOrCreateEntry(const TResourceId& resourceId);

    TResourceCommandOutcome DoInit(
        TResourceId resourceId,
        TEntryPtr entry,
        NYson::TYsonString argument);
    TResourceCommandOutcome DoUnload(
        TResourceId resourceId,
        TEntryPtr entry,
        NYson::TYsonString argument);

    IResourcePtr CreateResourceInstance(
        const TResourceId& resourceId,
        const std::string& className,
        const NCompanion::TInitResourceCommandArg& arg) const;
    TResourceCommandOutcome InitializeCleanInstance(
        const TResourceId& resourceId,
        const TEntryPtr& entry,
        const NCompanion::TInitResourceCommandArg& arg,
        const TEntry::TAppliedSpecs& incomingSpecs);
    TResourceCommandOutcome ApplyReconfigure(
        const TResourceId& resourceId,
        const TEntryPtr& entry,
        const NCompanion::TInitResourceCommandArg& arg,
        const TEntry::TAppliedSpecs& incomingSpecs);
    //! Publishes the pending generation once the hosted resource reports the
    //! pending target revision as applied, and reports it as not initialized
    //! until then.
    TResourceCommandOutcome TryCommitReconfigure(
        const TResourceId& resourceId,
        const TEntryPtr& entry);
};

DEFINE_REFCOUNTED_TYPE(TResourceStore);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
