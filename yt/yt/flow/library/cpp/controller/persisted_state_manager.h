#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

//! Pipeline state as seen by spec and target updates.
struct TObservedPipelineState
{
    //! Important versions that an update must expect.
    TPipelineImportantVersionsPtr ExpectedVersions;
    EPipelineState State = EPipelineState::Unknown;
    //! Whether the applied specs equal the canonical ones. Always true for a state resolved from the
    //! persisted rows: without a FlowView the controller cannot apply specs, so sync is not checked.
    bool Synced = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IPersistedStateManager
    : public NYT::TRefCounted
{
    virtual void AdvanceInputMessagesWatermark(TSystemTimestamp watermark) = 0;

    virtual void PersistFlowState(const TFlowStatePtr& flowState, const TPipelineImportantVersionsPtr& expectedVersions) = 0;
    virtual TFlowStatePtr RecoverFlowState() = 0;

    //! Resolves the pipeline state from one consistent snapshot of the persisted rows, read in a
    //! tablet transaction.
    virtual TObservedPipelineState RecoverPersistedPipelineState() = 0;

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
    //! Return the raw canonical rows, or null when a row is absent.
    virtual NYTree::INodePtr RecoverSpecNode() = 0;
    virtual NYTree::INodePtr RecoverDynamicSpecNode() = 0;

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

//! Reads only the version of a persisted versioned value, so the value itself may be unparsable.
TVersion ReadRawVersion(const NYTree::INodePtr& versionedValue);

//! Looks up rows of the obsolete state table by key; std::nullopt for an absent row. All calls made by
//! one resolution must read the same snapshot.
using TObsoleteStateLookup = std::function<std::vector<std::optional<NYson::TYsonString>>(const std::vector<std::string>& keys)>;

//! Resolves the persisted pipeline state; reads the FlowState row only if the replica is missing or stale.
//! Throws if FlowState and the important versions disagree, which the single writer never persists.
TObservedPipelineState ResolvePipelineState(const TObsoleteStateLookup& lookup);

////////////////////////////////////////////////////////////////////////////////

//! Replaces embedded pipeline specs of |flowStateNode| that fail to parse with the canonical ones
//! under the embedded version, so that the controller re-applies them. Throws if a replacement is
//! needed but not possible; |flowStateNode| is then left unmodified.
void RepairEmbeddedPipelineSpecs(
    const NYTree::INodePtr& flowStateNode,
    const NYTree::INodePtr& canonicalSpec,
    const NYTree::INodePtr& canonicalDynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
