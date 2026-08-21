#pragma once

#ifndef FILE_RESOURCE_INL_H_
    #error "Direct inclusion of this file is not allowed, include file_resource.h"
#endif

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TData>
TFileResourceAccessor<TData>::TFileResourceAccessor(TIntrusivePtr<TSnapshot> snapshot)
    : Snapshot_(std::move(snapshot))
{ }

template <class TData>
const TData& TFileResourceAccessor<TData>::operator*() const
{
    return *Snapshot_->Data;
}

template <class TData>
const TData* TFileResourceAccessor<TData>::operator->() const
{
    return Snapshot_->Data.Get();
}

template <class TData>
const TFileSourceRevisionPtr& TFileResourceAccessor<TData>::GetSourceRevision() const
{
    return Snapshot_->SourceRevision;
}

template <class TData>
i64 TFileResourceAccessor<TData>::GetDeliveryRevisionId() const
{
    return Snapshot_->DeliveryRevisionId;
}

////////////////////////////////////////////////////////////////////////////////

template <class TData>
TFileResourceBase<TData>::TFileResourceBase(
    TResourceContextPtr context,
    TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(std::move(context), dynamicContext)
    , Source_([&] {
        auto sourceContext = New<TFileSourceContext>();
        sourceContext->SourceSpec = GetParameters()->FileSource;
        sourceContext->ClientsCache = GetContext()->ClientsCache;
        sourceContext->PipelinePath = GetContext()->PipelinePath;
        sourceContext->Invoker = GetContext()->Invoker;
        sourceContext->Logger = GetContext()->Logger.WithTag("Component", "FileSource");
        return TRegistry::Get()->CreateFileSource(sourceContext);
    }())
    , FileStorage_(GetContext()->FileStorage)
    , UpdateError_(GetContext()->StatusProfiler
            ? GetContext()->StatusProfiler->ErrorState("/file_update")
            : nullptr)
{
    SubscribeReconfigured(BIND(&TFileResourceBase::OnReconfigured, Unretained(this)));
    OnReconfigured(dynamicContext);
}

template <class TData>
TFuture<void> TFileResourceBase<TData>::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    auto guard = Guard(Lock_);
    LoadStarted_ = true;
    ScheduleProcessing(guard);
    return InitialLoadPromise_.ToFuture().ToUncancelable();
}

template <class TData>
TResourceRevisionState TFileResourceBase<TData>::GetRevisionState() const
{
    auto guard = Guard(Lock_);
    return {
        .AppliedRevisionId = AppliedRevisionId_,
        .TargetRevisionId = Target_ ? std::optional(Target_->RevisionId) : std::nullopt,
        .UpdateState = UpdateState_,
    };
}

template <class TData>
typename TFileResourceBase<TData>::TAccessor TFileResourceBase<TData>::Lock() const
{
    auto snapshot = ActiveSnapshot_.Acquire();
    THROW_ERROR_EXCEPTION_UNLESS(snapshot, "File resource has no initialized data");
    return TAccessor(std::move(snapshot));
}

template <class TData>
void TFileResourceBase<TData>::Validate(const TDataPtr& /*data*/)
{ }

template <class TData>
void TFileResourceBase<TData>::OnReconfigured(const TDynamicResourceContextPtr& dynamicContext)
{
    auto guard = Guard(Lock_);
    Target_ = dynamicContext->TargetRevision;
    ScheduleProcessing(guard);
}

template <class TData>
void TFileResourceBase<TData>::ScheduleProcessing(const TGuard<NThreading::TSpinLock>& /*guard*/)
{
    if (!LoadStarted_ || Processing_ || !Target_) {
        return;
    }

    Processing_ = true;
    YT_UNUSED_FUTURE(
        BIND(&TFileResourceBase::ProcessTargets, MakeStrong(this))
            .AsyncVia(GetContext()->Invoker)
            .Run());
}

template <class TData>
void TFileResourceBase<TData>::ProcessTargets()
{
    while (true) {
        TResourceRevisionPtr target;
        {
            auto guard = Guard(Lock_);
            if (!Target_ || AppliedRevisionId_ == Target_->RevisionId) {
                Processing_ = false;
                UpdateState_.reset();
                return;
            }
            target = Target_;
        }

        TFileSourceRevisionPtr sourceRevision;
        TSnapshotPtr candidate;
        try {
            sourceRevision = ParseTarget(target);
            candidate = BuildCandidate(target, sourceRevision);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to prepare file resource revision")
                .With("update_state", UpdateState_)
                .With("revision_id", target->RevisionId)
                .With(ex);
            if (sourceRevision) {
                error.Add("object_id", sourceRevision->ObjectId);
                error.Add("display_version", sourceRevision->DisplayVersion);
            }
            YT_TLOG_WARNING("Failed to prepare file resource revision")
                .With(error);
            if (UpdateError_) {
                UpdateError_->SetError(error);
            }

            auto guard = Guard(Lock_);
            if (Target_ && target->RevisionId == Target_->RevisionId) {
                UpdateState_ = EFileResourceUpdateState::WaitingForRetry;
                Processing_ = false;
                if (!RetryScheduled_) {
                    RetryScheduled_ = true;
                    NConcurrency::TDelayedExecutor::Submit(
                        BIND([weakThis = MakeWeak(this)] {
                            if (auto strongThis = weakThis.Lock()) {
                                auto guard = Guard(strongThis->Lock_);
                                strongThis->RetryScheduled_ = false;
                                strongThis->ScheduleProcessing(guard);
                            }
                        }),
                        GetDynamicParameters()->UpdateRetryPeriod,
                        GetContext()->Invoker);
                }
                return;
            }
            continue;
        }

        bool completeInitialLoad = false;
        TSnapshotPtr previousSnapshot;
        {
            auto guard = Guard(Lock_);
            if (!Target_ || target->RevisionId != Target_->RevisionId) {
                continue;
            }

            previousSnapshot = ActiveSnapshot_.Exchange(std::move(candidate));
            AppliedRevisionId_ = target->RevisionId;
            UpdateState_.reset();
            if (!InitialLoadCompleted_) {
                InitialLoadCompleted_ = true;
                completeInitialLoad = true;
            }
        }
        previousSnapshot.Reset();

        if (UpdateError_) {
            UpdateError_->ClearError();
        }
        if (completeInitialLoad) {
            InitialLoadPromise_.Set();
        }
    }
}

template <class TData>
typename TFileResourceBase<TData>::TSnapshotPtr TFileResourceBase<TData>::BuildCandidate(
    const TResourceRevisionPtr& target,
    const TFileSourceRevisionPtr& sourceRevision)
{
    if (auto active = ActiveSnapshot_.Acquire();
        active && active->SourceRevision->ObjectId == sourceRevision->ObjectId)
    {
        return New<TSnapshot>(
            active->Data,
            active->Directory,
            std::move(sourceRevision),
            target->RevisionId);
    }

    SetUpdateState(EFileResourceUpdateState::Downloading);
    THROW_ERROR_EXCEPTION_UNLESS(
        FileStorage_,
        "File resource cannot download revision because file storage is unavailable in this process");
    auto storageObject = NConcurrency::WaitFor(FileStorage_->GetOrCreate(
        sourceRevision->ObjectId,
        sourceRevision->Size,
        [source = Source_, sourceRevision] (const std::string& directory) {
            return source->Download(sourceRevision, directory);
        }))
        .ValueOrThrow();
    auto materializedDirectory = New<TMaterializedDirectory>(sourceRevision, storageObject);

    SetUpdateState(EFileResourceUpdateState::Initializing);
    auto data = Initialize(materializedDirectory);
    THROW_ERROR_EXCEPTION_UNLESS(data, "File resource initializer returned null data");
    SetUpdateState(EFileResourceUpdateState::Validating);
    Validate(data);
    return New<TSnapshot>(
        std::move(data),
        std::move(materializedDirectory),
        std::move(sourceRevision),
        target->RevisionId);
}

template <class TData>
TFileSourceRevisionPtr TFileResourceBase<TData>::ParseTarget(const TResourceRevisionPtr& target) const
{
    THROW_ERROR_EXCEPTION_UNLESS(target->Spec, "File resource target revision has no spec");
    auto sourceRevision = NYTree::ConvertTo<TFileSourceRevisionPtr>(target->Spec);
    THROW_ERROR_EXCEPTION_UNLESS(
        sourceRevision->FileSourceClassName == GetParameters()->FileSource->FileSourceClassName,
        "File resource target source class %Qv differs from configured source class %Qv",
        sourceRevision->FileSourceClassName,
        GetParameters()->FileSource->FileSourceClassName);
    return sourceRevision;
}

template <class TData>
void TFileResourceBase<TData>::SetUpdateState(EFileResourceUpdateState state)
{
    auto guard = Guard(Lock_);
    UpdateState_ = state;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
