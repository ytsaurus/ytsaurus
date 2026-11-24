#pragma once

#include "public.h"

#include "options.h"
#include "source_engine.h"
#include "spec.h"
#include "state_access.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TYdbSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbSourceParameters);

    TYdbSourceController(
        TSourceControllerContextPtr context,
        TDynamicSourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) override;
    void Sync() override;
    void Commit() override;

    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses) override;
    void ProcessSuppressedGroups(const THashSet<std::string>& groups) override;
    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override;
    std::string GetGroup(const TKey& key) override;
    bool IsExcludedFromReadAlignment(const TKey& key) const override;
    std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() override;

    std::string GetSourceIdentity() const override;

private:
    const TYdbTopicInfoControllerPtr Info_;
    const TYdbStateAccess<TYdbPartitionGraphSnapshot> PartitionGraphState_;
    const IStatusErrorStatePtr ResolvePartitionsErrorState_;
    //! Keys of the latest #ListKeys whose reads wait for other partitions.
    THashSet<TKey> ReadAlignmentExcludedKeys_;

    void SeedLegacyPartitions();
    void OnTopicDescribed(const std::string& endpoint, const NYdb::NTopic::TTopicDescription& description);
    //! The partition as the topic describes it on its own, or null if the topic confirms it does not exist.
    std::optional<NYdb::NTopic::TPartitionInfo> DescribeMissingPartition(
        const std::shared_ptr<NYdb::NTopic::TTopicClient>& topicClient,
        i64 partitionId);
};

DEFINE_REFCOUNTED_TYPE(TYdbSourceController);

////////////////////////////////////////////////////////////////////////////////

class TYdbSourceBase
    : public TIntegerOffsetOrderedSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TYdbSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TYdbSourcePartitionSpec);
    using TSourceController = TYdbSourceController;

protected:
    using TPayloadSplitterFactory = TYdbPayloadSplitter (*)(const TYdbSourceParameters& parameters);

    TYdbSourceBase(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext,
        TPayloadSplitterFactory createPayloadSplitter);

    void ApplyPartitionInfoUpdate(const TYdbPartitionInfoUpdate& update);

    TFuture<std::optional<TOffset>> DoFetchTrimmedOffset() final;
    NYson::TYsonString CaptureReadSettings() final;
    void ApplyReadSettings(const NYson::TYsonString& settings) final;

private:
    const TYdbSourceEnginePtr Engine_;
    TYdbSourcePartitionSpecPtr AppliedPartitionSpec_;
    IStatusErrorStatePtr TopologyErrorState_;
    std::atomic<bool> InputExhausted_ = false;

    void ApplyPartitionSpec(TYdbSourcePartitionSpecPtr spec);
    bool IsReadPaused() const;
    bool IsFinite() final;
    void AdjustInflight(const TInflightStreamTraverseDataPtr& inflight) final;
    NYTree::IMapNodePtr GetPartitionStatus() final;

    void DoInit() final;
    void DoTerminate() final;

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimitExclusive) final;

    TFuture<std::vector<TRecord>> DoReadRange(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        TOffset endExclusive) final;

    void DoReportPersistedOffset(TOffset offset) final;
};

DEFINE_REFCOUNTED_TYPE(TYdbSourceBase);

////////////////////////////////////////////////////////////////////////////////

class TYdbSource
    : public TYdbSourceBase
{
public:
    static constexpr bool ReplayStable = true;

    TYdbSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);
};

DEFINE_REFCOUNTED_TYPE(TYdbSource);

////////////////////////////////////////////////////////////////////////////////

class TYdbFramingSource
    : public TYdbSourceBase
{
public:
    static constexpr bool ReplayStable = true;

    YT_FLOW_EXTEND_PARAMETERS(TYdbFramingSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbFramingSourceParameters);

    TYdbFramingSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);
};

DEFINE_REFCOUNTED_TYPE(TYdbFramingSource);

////////////////////////////////////////////////////////////////////////////////

class TYdbMultiLineSource
    : public TYdbSourceBase
{
public:
    static constexpr bool ReplayStable = true;

    YT_FLOW_EXTEND_PARAMETERS(TYdbMultiLineSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicYdbMultiLineSourceParameters);

    TYdbMultiLineSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);
};

DEFINE_REFCOUNTED_TYPE(TYdbMultiLineSource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
