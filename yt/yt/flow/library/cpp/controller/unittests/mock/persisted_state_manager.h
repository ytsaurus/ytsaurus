#pragma once

#include <yt/yt/flow/library/cpp/controller/persisted_state_manager.h>

#include <library/cpp/testing/gtest_extensions/gtest_extensions.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct TMockPersistedStateManager
    : public IPersistedStateManager
{
    // clang-format off

    MOCK_METHOD(void, AdvanceInputMessagesWatermark, (TSystemTimestamp watermark), (override));

    MOCK_METHOD(void, PersistFlowState, (
        const TFlowStatePtr& flowState,
        const TPipelineImportantVersionsPtr& expectedVersions),
        (override));
    MOCK_METHOD(TFlowStatePtr, RecoverFlowState, (), (override));

    MOCK_METHOD(void, PersistSpecs, (
        const std::optional<TVersionedPipelineSpecPtr>& spec,
        const std::optional<TPipelineImportantVersionsPtr>& expectedVersions,
        const std::optional<TVersionedDynamicPipelineSpecPtr>& dynamicSpec,
        const std::optional<TVersion>& expectedDynamicSpecVersion),
        (override));

    MOCK_METHOD(TVersionedPipelineSpecPtr, RecoverSpec, (), (override));
    MOCK_METHOD(TVersionedDynamicPipelineSpecPtr, RecoverDynamicSpec, (), (override));

    MOCK_METHOD(void, PersistFlowCoreTarget, (
        const TVersionedFlowCoreTargetPtr& flowCoreTarget,
        const TPipelineImportantVersionsPtr& expectedVersions),
        (override));
    MOCK_METHOD(TVersionedFlowCoreTargetPtr, RecoverFlowCoreTarget, (), (override));

    // clang-format on
};

using TMockPersistedStateManagerPtr = TIntrusivePtr<TMockPersistedStateManager>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
