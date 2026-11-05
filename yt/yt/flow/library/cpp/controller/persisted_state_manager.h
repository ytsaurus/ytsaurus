#pragma once

#include "public.h"

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct IPersistedStateManager
    : public NYT::TRefCounted
{
    virtual void AdvanceInputMessagesWatermark(TSystemTimestamp watermark) = 0;

    virtual void PersistFlowState(const TFlowStatePtr& flowState, const TPipelineImportantVersionsPtr& expectedVersions) = 0;
    virtual TFlowStatePtr RecoverFlowState() = 0;

    // Update spec and dynamic spec (one or both).
    // #expectedVersions is required parameter if spec is being updated.
    // #expectedDynamicSpecVersion is required parameter if dynamic spec is being updated.
    virtual void PersistSpecs(
        const std::optional<TVersionedPipelineSpecPtr>& spec,
        const std::optional<TPipelineImportantVersionsPtr>& expectedVersions,
        const std::optional<TVersionedDynamicPipelineSpecPtr>& dynamicSpec,
        const std::optional<TVersion>& expectedDynamicSpecVersion) = 0;

    virtual TVersionedPipelineSpecPtr RecoverSpec() = 0;
    virtual TVersionedDynamicPipelineSpecPtr RecoverDynamicSpec() = 0;

    virtual void PersistFlowCoreTarget(
        const TVersionedFlowCoreTargetPtr& flowCoreTarget,
        const TPipelineImportantVersionsPtr& expectedVersions) = 0;
    virtual TVersionedFlowCoreTargetPtr RecoverFlowCoreTarget() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPersistedStateManager);

////////////////////////////////////////////////////////////////////////////////

IPersistedStateManagerPtr CreatePersistedStateManager(
    IYTConnectorPtr connector,
    TPersistedStateManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

TPipelineImportantVersionsPtr MakePipelineImportantVersions(const TFlowStatePtr& flowState, const TVersionedPipelineSpecPtr& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
