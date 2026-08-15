#pragma once

#include "companion_client.h"
#include "companion_manager.h"
#include "companion_model.h"
#include "companion_resource.h"
#include "public.h"
#include <yt/yt/flow/library/cpp/computation/computation_base.h>

#include <functional>

namespace NYT::NFlow {

struct TSimpleExternalState;

template <class T>
class TJoinedStateKeyClient;

} // namespace NYT::NFlow

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Runs a companion process request with bounded in-band healing.
/*!
 *  On JobNotFound the request is resent with job info included; on
 *  ResourceNotInitialized |healRequiredCompanionResources| is invoked before
 *  the resend and the request references are replaced with the returned current
 *  references. Bounded at three attempts. Returns the last response; the caller
 *  is responsible for handling a non-Ok final status.
 */
TCompanionResponsePtr ProcessWithCompanionHealing(
    const ICompanionClientPtr& client,
    const TCompanionProcessRequestPtr& request,
    const IExternalPerformanceMetricsReporterPtr& reporter,
    const std::function<std::vector<TCompanionResourceInstanceReference>()>& healRequiredCompanionResources);

////////////////////////////////////////////////////////////////////////////////

//! Packs read-only joined external states into |request|: for every joiner, the loaded states for
//! its extract-derived keys — the keys its preload loaded, derived from the whole batch rather
//! than from a single message key.
void AddJoinedExternalStates(
    const TCompanionProcessRequestPtr& request,
    const THashMap<std::string, TJoinedStateKeyClient<TSimpleExternalState>>& joiners,
    const IInputContextPtr& input);

////////////////////////////////////////////////////////////////////////////////

//! A CRTP base class template that provides common companion computation functionality.
/*!
 *  This class template inherits from TBase (which should be a computation class
 *  derived from TUniversalComputationBase) and adds companion-specific functionality.
 */
template <class TBase>
class TCompanionComputationBaseAdapter
    : public TBase
{
    // Verify that TBase is derived from TUniversalComputationBase.
    static_assert(std::derived_from<TBase, TUniversalComputationBase>);

public:
    TCompanionComputationBaseAdapter(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    //! Drops the required-resource subscriptions and hands the job removal to
    //! the companion manager. Never waits: a stuck companion must not hold up
    //! job teardown.
    ~TCompanionComputationBaseAdapter() override;

protected:
    //! Fetches and validates companion info.
    /*!
     *  This method should be called from DoInit() of the derived class.
     *  It fetches the companion info and validates that the computation exists
     *  and that the companion supports resource commands when the computation
     *  requires companion resources.
     */
    void FetchAndValidateCompanionInfo();

    //! Put job info to companion context.
    /*!
     *  This method should be called from DoInit() of the derived class.
     *  It puts the computation static spec, dynamic spec and streams to the companion context.
     */
    void PutJobInfoToCompanion();

    //! Puts job info to the companion and re-sends it whenever the dynamic spec is reconfigured.
    void PutJobInfoToCompanionWithReconfigure();

    //! Sends the idempotent init command for every required companion resource
    //! on this computation's own channel.
    void PutRequiredCompanionResourcesToCompanion();

    //! Puts required companion resources to the companion and re-sends init
    //! whenever a resource publishes a prepared generation.
    /*!
     *  This method should be called from DoInit() of the derived class, after
     *  FetchAndValidateCompanionInfo() and before PutJobInfoToCompanion().
     */
    void PutRequiredCompanionResourcesToCompanionWithReconfigure();

    //! Runs the init sequence shared by every companion computation: validates
    //! the companion info, proactively delivers the required companion
    //! resource graph and only then publishes the job info holding references
    //! to it.
    /*!
     *  This method should be called from DoInit() of the derived class. The
     *  three steps are kept together here so that no adapter can silently skip
     *  proactive resource delivery and reach its first batch through the slower
     *  ResourceNotInitialized healing path.
     */
    void InitCompanionJob();

    //! Processes a request with the companion.
    /*!
     *  Sends a process request to the companion and heals in-band failures:
     *  a job missing at the companion is re-put with job info, uninitialized
     *  required companion resources are re-initialized on this computation's
     *  channel.
     *
     *  \param request The process request to send to the companion.
     *  \return The response from the companion.
     */
    TCompanionResponsePtr DoProcessWithCompanion(const TCompanionProcessRequestPtr& request);

    //! Creates a new companion request of the specified type.
    /*!
     *  Creates a new request of type TRequestType and initializes it with the current job's
     *  information including job ID, computation ID, computation static and dynamic specs, and stream specs.
     *  This template method is used to create TCompanionProcessRequest.
     *
     *  \tparam TRequestType The type of request to create (e.g., TCompanionProcessRequest).
     *  \return A pointer to the newly created request.
     */
    template <class TRequestType>
    TIntrusivePtr<TRequestType> CreateCompanionRequest();

    ICompanionClientPtr CompanionClient_;
    TCompanionInfoPtr CompanionInfo_;
    //! Outlives every computation; owns the retried job removals.
    TCompanionManagerPtr CompanionManager_;

private:
    struct TRequiredCompanionResource
    {
        TResourceId ResourceId;
        TResourceId Alias;
        TCompanionResourcePtr Resource;
    };

    //! Resolves direct companion resources required by this computation.
    std::vector<TRequiredCompanionResource> GetRequiredCompanionResources();

    //! Merges the transitive graphs of |requiredResources|, deduplicated by
    //! resource instance id.
    static std::vector<TCompanionResourcePtr> CollectCompanionResourceGraph(
        const std::vector<TRequiredCompanionResource>& requiredResources);

    //! Exact direct and transitive references serialized into job info.
    std::vector<TCompanionResourceInstanceReference> GetRequiredCompanionResourceReferences();

    //! Full companion-resource graph required by this computation.
    std::vector<TCompanionResourcePtr> GetRequiredCompanionResourceGraph();

    //! Replaces subscriptions when resource-manager recreation changes any
    //! object in the required transitive graph.
    void RefreshRequiredCompanionResourceSubscriptions();

    void OnRequiredCompanionResourceStateChanged(const TCompanionResourcePtr& resource);

    //! Required companion resources outlive this adapter (they are shared across
    //! jobs), so their applied-state subscriptions must be removed explicitly
    //! in the destructor to avoid accumulating dead callbacks.
    std::vector<std::pair<
        TCompanionResourcePtr,
        TCallback<void()>>>
        ResourceStateChangedSubscriptions_;
    std::vector<TCompanionResourceInstanceReference> PublishedCompanionResourceReferences_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion

#define COMPANION_COMPUTATION_BASE_INL_H_
#include "companion_computation_base-inl.h"
#undef COMPANION_COMPUTATION_BASE_INL_H_
