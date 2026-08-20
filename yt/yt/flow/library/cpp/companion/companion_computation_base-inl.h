#pragma once

#ifndef COMPANION_COMPUTATION_BASE_INL_H_
    #error "Direct inclusion of this file is not allowed, include companion_computation_base.h"
    // For the sake of sane code completion.
    #include "companion_computation_base.h"
#endif

#include <algorithm>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
TCompanionComputationBaseAdapter<TBase>::TCompanionComputationBaseAdapter(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TBase(std::move(context), std::move(dynamicContext))
{
    CompanionManager_ = this->GetContext()
        ->GetStaticResource(CompanionManagerAlias)
        ->template As<TCompanionManager>();
    CompanionClient_ = CompanionManager_->CreateCompanionClient(this->GetContext()->StatusProfiler);
    // Strictly the last constructor action, before any registration can reach
    // the companion: the reconcile pass removes companion jobs absent from
    // the live set, and a throw after this line must run the destructor.
    CompanionManager_->RegisterLiveJob(this->GetJobId(), CompanionClient_);
}

template <class TBase>
TCompanionComputationBaseAdapter<TBase>::~TCompanionComputationBaseAdapter()
{
    for (const auto& [resource, callback] : ResourceStateChangedSubscriptions_) {
        resource->UnsubscribeCompanionStateChanged(callback);
    }
    // Sends one prompt removal and leaves the rest to the reconcile pass;
    // never blocks, so teardown does not wait on a stuck companion.
    CompanionManager_->UnregisterLiveJob(this->GetJobId(), CompanionClient_);
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::FetchAndValidateCompanionInfo()
{
    CompanionInfo_ = CompanionClient_->GetCompanionInfo();
    auto computationIt = CompanionInfo_->Computations.find(this->GetComputationId());
    if (computationIt == CompanionInfo_->Computations.end()) {
        THROW_ERROR_EXCEPTION("There is no corresponding computation in companion")
            .With("computation_id", this->GetComputationId());
    }
}

template <class TBase>
std::vector<typename TCompanionComputationBaseAdapter<TBase>::TRequiredCompanionResource>
TCompanionComputationBaseAdapter<TBase>::GetRequiredCompanionResources()
{
    std::vector<TRequiredCompanionResource> result;
    for (const auto& [resourceId, description] : this->GetSpec()->RequiredResourceIds) {
        // Only worker-side required resources are present in the computation context.
        if (!description->Worker) {
            continue;
        }
        const auto& alias = description->Alias ? *description->Alias : resourceId;
        auto resource = this->GetContext()->GetStaticResource(alias)->template TryAs<TCompanionResource>();
        if (resource) {
            resource = resource->GetCurrentResource();
            result.push_back({
                .ResourceId = resourceId,
                .Alias = alias,
                .Resource = std::move(resource),
            });
        }
    }
    std::sort(
        result.begin(),
        result.end(),
        [] (const TRequiredCompanionResource& lhs, const TRequiredCompanionResource& rhs) {
            return lhs.ResourceId < rhs.ResourceId;
        });
    return result;
}

template <class TBase>
std::vector<TCompanionResourcePtr>
TCompanionComputationBaseAdapter<TBase>::CollectCompanionResourceGraph(
    const std::vector<TRequiredCompanionResource>& requiredResources)
{
    std::vector<TCompanionResourcePtr> result;
    THashSet<TResourceInstanceId> resourceInstanceIds;
    for (const auto& requiredResource : requiredResources) {
        for (auto resource : requiredResource.Resource->GetCompanionResourceGraph()) {
            if (resourceInstanceIds.insert(resource->GetContext()->ResourceInstanceId).second) {
                result.push_back(std::move(resource));
            }
        }
    }
    return result;
}

template <class TBase>
std::vector<TCompanionResourceInstanceReference>
TCompanionComputationBaseAdapter<TBase>::GetRequiredCompanionResourceReferences()
{
    auto requiredResources = GetRequiredCompanionResources();
    THashMap<TResourceInstanceId, TResourceId> aliases;
    for (const auto& requiredResource : requiredResources) {
        aliases[requiredResource.Resource->GetContext()->ResourceInstanceId] = requiredResource.Alias;
    }

    auto resources = CollectCompanionResourceGraph(requiredResources);
    std::vector<TCompanionResourceInstanceReference> references;
    references.reserve(resources.size());
    for (const auto& resource : resources) {
        auto aliasIt = aliases.find(resource->GetContext()->ResourceInstanceId);
        references.push_back(resource->GetReference(
            aliasIt != aliases.end()
                ? std::make_optional(aliasIt->second)
                : std::nullopt));
    }
    std::sort(
        references.begin(),
        references.end(),
        [] (const TCompanionResourceInstanceReference& lhs, const TCompanionResourceInstanceReference& rhs) {
            return lhs.ResourceId < rhs.ResourceId;
        });
    return references;
}

template <class TBase>
std::vector<TCompanionResourcePtr>
TCompanionComputationBaseAdapter<TBase>::GetRequiredCompanionResourceGraph()
{
    return CollectCompanionResourceGraph(GetRequiredCompanionResources());
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutRequiredCompanionResourcesToCompanion()
{
    for (const auto& requiredResource : GetRequiredCompanionResources()) {
        // Initialize the resource in the companion process pinned to this
        // computation's own channel.
        requiredResource.Resource->InitInCompanion(CompanionClient_, CompanionInfo_->ProcessId);
    }
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutRequiredCompanionResourcesToCompanionWithReconfigure()
{
    // Initial delivery: the process pinned to this computation's channel is
    // initialized up front.
    PutRequiredCompanionResourcesToCompanion();
    RefreshRequiredCompanionResourceSubscriptions();
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::RefreshRequiredCompanionResourceSubscriptions()
{
    auto resources = GetRequiredCompanionResourceGraph();
    bool unchanged = resources.size() == ResourceStateChangedSubscriptions_.size();
    if (unchanged) {
        for (size_t index = 0; index < resources.size(); ++index) {
            if (resources[index] != ResourceStateChangedSubscriptions_[index].first) {
                unchanged = false;
                break;
            }
        }
    }
    if (unchanged) {
        return;
    }

    for (const auto& [resource, callback] : ResourceStateChangedSubscriptions_) {
        resource->UnsubscribeCompanionStateChanged(callback);
    }
    ResourceStateChangedSubscriptions_.clear();

    // Re-send init only after the primary companion has accepted a prepared
    // generation. Target delivery alone must not expose unprepared state.
    for (const auto& resource : resources) {
        auto callback = BIND(
            [weakThis = MakeWeak(this), weakResource = MakeWeak(resource.Get())] {
                auto this_ = weakThis.Lock();
                auto lockedResource = weakResource.Lock();
                if (!this_ || !lockedResource) {
                    return;
                }
                this_->GetContext()->SerializedInvoker->Invoke(BIND(
                    &TCompanionComputationBaseAdapter::OnRequiredCompanionResourceStateChanged,
                    MakeWeak(this_.Get()),
                    std::move(lockedResource)));
            });
        resource->SubscribeCompanionStateChanged(callback);
        ResourceStateChangedSubscriptions_.emplace_back(resource, std::move(callback));
    }
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::OnRequiredCompanionResourceStateChanged(
    const TCompanionResourcePtr& /*resource*/)
{
    const auto& Logger = this->GetContext()->Logger;
    try {
        PutRequiredCompanionResourcesToCompanion();
        PutJobInfoToCompanion();
    } catch (const std::exception& ex) {
        // Healed in-band later: ProcessBatch returns ResourceNotInitialized and
        // the retry loop re-sends init.
        YT_TLOG_WARNING("Failed to propagate prepared companion resource state to companion")
            .With("ComputationId", this->GetComputationId())
            .With(ex);
    }
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutJobInfoToCompanion()
{
    RefreshRequiredCompanionResourceSubscriptions();
    auto putJobRequest = New<TCompanionPutJobRequest>();
    putJobRequest->JobId = this->GetJobId();
    putJobRequest->ComputationId = this->GetComputationId();
    putJobRequest->ComputationSpec = this->GetSpec();
    putJobRequest->DynamicComputationSpec = this->GetDynamicSpec();
    putJobRequest->JobStreamSpecs = this->GetContext()->StreamSpecStorage->GetStreamSpecs();
    putJobRequest->CompanionResources = GetRequiredCompanionResourceReferences();

    auto putJobResponse = CompanionClient_->PutJob(
        putJobRequest,
        this->GetContext()->ExternalMetricsReporter);
    if (putJobResponse->Status != ECompanionResponseStatus::Ok) {
        THROW_ERROR_EXCEPTION("Failed to put job to companion")
            .With("job_id", putJobRequest->JobId)
            .With("computation_id", this->GetComputationId());
    }
    PublishedCompanionResourceReferences_ = putJobRequest->CompanionResources;
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::PutJobInfoToCompanionWithReconfigure()
{
    // Initial publish.
    PutJobInfoToCompanion();

    // Re-publish on dynamic spec changes.
    this->SubscribeOnReconfigure(BIND([this] {
        PutJobInfoToCompanion();
    }));
}

template <class TBase>
void TCompanionComputationBaseAdapter<TBase>::InitCompanionJob()
{
    FetchAndValidateCompanionInfo();

    // Strictly before the job info: it carries the exact references, and the
    // companion rejects a batch whose references are not initialized yet.
    PutRequiredCompanionResourcesToCompanionWithReconfigure();

    PutJobInfoToCompanionWithReconfigure();
}

template <class TBase>
TCompanionResponsePtr TCompanionComputationBaseAdapter<TBase>::DoProcessWithCompanion(
    const TCompanionProcessRequestPtr& request)
{
    auto response = ProcessWithCompanionHealing(
        CompanionClient_,
        request,
        this->GetContext()->ExternalMetricsReporter,
        [this] {
            PutRequiredCompanionResourcesToCompanion();
            return GetRequiredCompanionResourceReferences();
        });

    if (response->Status != ECompanionResponseStatus::Ok) {
        THROW_ERROR_EXCEPTION("Failed to process with companion")
            .With("job_id", this->GetJobId())
            .With("computation_id", this->GetComputationId())
            .With("status", response->Status);
    }

    PublishedCompanionResourceReferences_ = request->CompanionResources;
    return response;
}

template <class TBase>
template <class TRequestType>
TIntrusivePtr<TRequestType> TCompanionComputationBaseAdapter<TBase>::CreateCompanionRequest()
{
    RefreshRequiredCompanionResourceSubscriptions();
    auto companionResources = GetRequiredCompanionResourceReferences();
    auto request = New<TRequestType>();
    request->JobId = this->GetJobId();
    request->ComputationId = this->GetComputationId();
    request->ComputationSpec = this->GetSpec();
    request->DynamicComputationSpec = this->GetDynamicSpec();
    request->JobStreamSpecs = this->GetContext()->StreamSpecStorage->GetStreamSpecs();
    if (companionResources != PublishedCompanionResourceReferences_)
    {
        // A resource-manager recreation has replaced at least one object.
        // Initialize the current graph on this channel and atomically replace
        // the companion job through the embedded job info before user code.
        PutRequiredCompanionResourcesToCompanion();
        request->SendJobInfo = true;
    }
    request->CompanionResources = std::move(companionResources);
    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
