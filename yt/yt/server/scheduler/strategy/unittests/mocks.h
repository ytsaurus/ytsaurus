#pragma once

#include <yt/yt/server/scheduler/strategy/policy/structs.h>

#include <yt/yt/server/scheduler/strategy/strategy.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>
#include <yt/yt/server/scheduler/strategy/resource_tree.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/scheduler/common/exec_node.h>

#include <yt/yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/yson/null_consumer.h>

namespace NYT::NScheduler::NStrategy::NPolicy {
namespace NTestMocks {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerStrategyHostMock
    : public TRefCounted
    , public IStrategyHost
    , public TEventLogHostBase
{
public:
    TSchedulerStrategyHostMock(
        std::vector<IInvokerPtr> nodeShardInvokers = {},
        std::vector<TExecNodePtr> execNodes = {})
        : NodeShardInvokers_(std::move(nodeShardInvokers))
        , ExecNodes_(std::move(execNodes))
        , MediumDirectory_(New<NChunkClient::TMediumDirectory>())
    {
        NChunkClient::NProto::TMediumDirectory protoDirectory;
        auto* item = protoDirectory.add_items();
        item->set_name(NChunkClient::DefaultSlotsMediumName);
        item->set_index(NChunkClient::DefaultSlotsMediumIndex);
        item->set_priority(0);
        MediumDirectory_->LoadFrom(protoDirectory);

        for (const auto& node : ExecNodes_) {
            NodeToState_.emplace(node, New<TNodeState>());
        }
    }

    IInvokerPtr GetControlInvoker(EControlQueue /*queue*/) const override
    {
        return GetCurrentInvoker();
    }

    IInvokerPtr GetFairShareLoggingInvoker() const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetFairShareProfilingInvoker() const override
    {
        YT_UNIMPLEMENTED();
    }

    IInvokerPtr GetFairShareUpdateInvoker() const override
    {
        return GetCurrentInvoker();
    }

    IInvokerPtr GetBackgroundInvoker() const override
    {
        return GetCurrentInvoker();
    }

    IInvokerPtr GetOrchidWorkerInvoker() const override
    {
        return GetCurrentInvoker();
    }

    int GetNodeShardId(NNodeTrackerClient::TNodeId /*nodeId*/) const override
    {
        return 0;
    }

    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const override
    {
        return NodeShardInvokers_;
    }

    NEventLog::TFluentLogEvent LogFairShareEventFluently(TInstant /*now*/) override
    {
        YT_UNIMPLEMENTED();
    }

    NEventLog::TFluentLogEvent LogAccumulatedUsageEventFluently(TInstant /*now*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const override
    {
        TJobResources result;
        for (const auto& execNode : ExecNodes_) {
            if (execNode->CanSchedule(filter)) {
                result += execNode->ResourceLimits();
            }
        }
        return result;
    }

    TJobResources GetResourceUsage(const TSchedulingTagFilter& /*filter*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void Disconnect(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TInstant GetConnectionTime() const override
    {
        return TInstant();
    }

    void MarkOperationAsRunningInStrategy(TOperationId /*operationId*/) override
    { }

    void AbortOperation(TOperationId /*operationId*/, const TError& /*error*/) override
    { }

    void FlushOperationNode(TOperationId /*operationId*/) override
    { }

    TMemoryDistribution GetExecNodeMemoryDistribution(const TSchedulingTagFilter& filter) const override
    {
        TMemoryDistribution result;
        for (const auto& execNode : ExecNodes_)
            if (execNode->CanSchedule(filter)) {
                ++result[execNode->ResourceLimits().GetMemory()];
            }
        return result;
    }

    void AbortAllocationsAtNode(NNodeTrackerClient::TNodeId /*nodeId*/, EAbortReason /*reason*/) override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<int> FindMediumIndexByName(const std::string& /*mediumName*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    const std::string& GetMediumNameByIndex(int /*mediumIndex*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void ValidatePoolPermission(
        const std::string& /*treeId*/,
        NObjectClient::TObjectId /*poolObjectId*/,
        const TString& /*poolName*/,
        const std::string& /*user*/,
        NYTree::EPermission /*permission*/) const override
    { }

    void SetSchedulerAlert(ESchedulerAlertType /*alertType*/, const TError& /*alert*/) override
    { }

    TFuture<void> SetOperationAlert(
        TOperationId /*operationId*/,
        EOperationAlertType /*alertType*/,
        const TError& /*alert*/,
        std::optional<TDuration> /*timeout*/) override
    {
        return OKFuture;
    }

    NYson::IYsonConsumer* GetEventLogConsumer() override
    {
        return NYson::GetNullYsonConsumer();
    }

    const NLogging::TLogger* GetEventLogger() override
    {
        return nullptr;
    }

    TString FormatResources(const TJobResourcesWithQuota& resources) const override
    {
        YT_VERIFY(MediumDirectory_);
        return NScheduler::FormatResources(resources);
    }

    void SerializeResources(const TJobResourcesWithQuota& /*resources*/, NYson::IYsonConsumer* /*consumer*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void SerializeDiskQuota(const TDiskQuota& /*diskQuota*/, NYson::IYsonConsumer* /*consumer*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void LogResourceMetering(
        const TMeteringKey& /*key*/,
        const TMeteringStatistics& /*statistics*/,
        const THashMap<TString, TString>& /*otherTags*/,
        TInstant /*connectionTime*/,
        TInstant /*previousLogTime*/,
        TInstant /*currentTime*/) override
    { }

    int GetDefaultAbcId() const override
    {
        return -1;
    }

    const NChunkClient::TMediumDirectoryPtr& GetMediumDirectory() const
    {
        return MediumDirectory_;
    }

    void InvokeStoringStrategyState(TPersistentStrategyStatePtr /*persistentStrategyState*/) override
    { }

    TFuture<void> UpdateLastMeteringLogTime(TInstant /*time*/) override
    {
        return OKFuture;
    }

    const THashMap<std::string, TString>& GetUserDefaultParentPoolMap() const override
    {
        static const THashMap<std::string, TString> stub;
        return stub;
    }

    const TNodeStatePtr& GetNodeState(const TExecNodePtr node)
    {
        return GetOrCrash(NodeToState_, node);
    }

private:
    std::vector<IInvokerPtr> NodeShardInvokers_;
    std::vector<TExecNodePtr> ExecNodes_;
    THashMap<TExecNodePtr, TNodeStatePtr> NodeToState_;
    NChunkClient::TMediumDirectoryPtr MediumDirectory_;
};

using TSchedulerStrategyHostMockPtr = TIntrusivePtr<TSchedulerStrategyHostMock>;

////////////////////////////////////////////////////////////////////////////////

class TPoolTreeElementHostMock
    : public IPoolTreeElementHost
{
public:
    explicit TPoolTreeElementHostMock(const TStrategyTreeConfigPtr& treeConfig)
        : ResourceTree_(New<TResourceTree>(treeConfig, std::vector<IInvokerPtr>({GetCurrentInvoker()})))
    { }

    TResourceTree* GetResourceTree() override
    {
        return ResourceTree_.Get();
    }

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& /*treeSnapshot*/,
        const TPoolTreeElement* /*element*/,
        TDelimitedStringBuilderWrapper& /*delimitedBuilder*/) const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    TResourceTreePtr ResourceTree_;
};

using TPoolTreeElementHostMockPtr = TIntrusivePtr<TPoolTreeElementHostMock>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTestMocks
} // namespace NYT::NScheduler::NStrategy::NPolicy
