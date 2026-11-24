#pragma once

#include "public.h"

#include <yt/yt/flow/extensions/framing/cpp/public.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb-cpp-sdk/client/topic/read_events.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

#include <functional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYdbWriteTarget
{
    std::string Endpoint;
    std::string Database;

    bool operator==(const TYdbWriteTarget&) const = default;
};

struct TYdbWriteTargetHash
{
    size_t operator()(const TYdbWriteTarget& target) const
    {
        return CombineHashes(THash<std::string>()(target.Endpoint), THash<std::string>()(target.Database));
    }
};

struct TYdbWriteTargetResolution
{
    std::optional<TYdbWriteTarget> Target;
    TError Error;
    bool Retryable = false;
    bool Federated = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TYdbTopologyOptions
{
    //! Discovery entry points; clusters behind them are discovered at runtime.
    std::vector<TYdbWriteTarget> DiscoveryEntries;
    //! Database every discovered cluster must report and topic clients connect to.
    std::string Database;
    //! Topic path used by clients.
    std::string Topic;
    TDuration ClientTimeout;
    TDuration UpdatePartitionCountPeriod;
    //! Discovery must report named clusters; a nameless single-cluster fallback is an error.
    bool ExpectFederation = false;
    //! Whether named clusters make the topology federated.
    bool AllowFederation = true;
    //! Federation known from configuration before any discovery answer.
    std::optional<bool> KnownFederated;
    bool IncludePartitionStats = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TYdbSourceDynamicOptions
{
    EMalformedYdbMessagePolicy MalformedMessagePolicy = EMalformedYdbMessagePolicy::Fail;
    std::optional<TInstant> ReadFromInstant;
    std::optional<TDuration> MaxTimeLag;
    TDuration CommitTimeout;
    TDuration SessionSilenceTimeout;
    //! The complete accepted dynamic parameters; a read session restarts whenever they change.
    NYTree::INodePtr RestartToken;
};

struct TYdbBatchingOptions
{
    i64 MaxRowsPerBatch = 1000;
    i64 MaxBytesPerBatch = 5_MB;
};

struct TYdbSinkControllerDynamicOptions
{
    TDuration FlowQueueMetaHeartbeatPeriod;
};

struct TYdbTwoPhaseCommitDynamicOptions
{
    TYdbBatchingOptions Batching;
    TDuration PreparationAttemptTimeout;
    TDuration PreparationTargetBackoff;
};

struct TYdbPayloadRecord
{
    TSharedRef Payload;
    //! Null if the splitter reports no frame metadata.
    NYson::TYsonString FrameMeta;
};

using TYdbPayloadSplitter = std::function<std::vector<TYdbPayloadRecord>(const TSharedRef&)>;

struct TYdbSourceMetadataColumn
{
    std::string Name;
    TCallback<NYson::TYsonString(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessageBase&)> Serialize;
};

struct TYdbSourceOptions
{
    TYdbTopologyOptions Topology;
    std::string Consumer;
    bool ReceivePartitionEnd = false;
    i64 MaxMemoryUsageBytes = 50_MB;
    TDuration StartSessionTimeout;
    TDuration UpdateInfoPeriod;
    bool TryParseFlowQueueMeta = true;
    std::string FlowQueueMetaName;
    bool IgnoreMalformedFlowQueueMeta = false;
    std::optional<std::string> AttributesColumn;
    //! Column that receives the splitter frame metadata; absent from the schema if unset.
    std::optional<std::string> FrameMetaColumn;
    std::optional<std::vector<std::pair<int, int>>> PartitionFilter;
    TYdbPayloadSplitter SplitPayload;
    TCallback<NYson::TYsonString(const std::string&)> SerializeUnparsed;
    std::optional<TYdbSourceMetadataColumn> MessageMetadataColumn = std::nullopt;
    std::optional<std::string> WriteTimeColumn = std::nullopt;
    bool StatusObservesIdleness = false;
};

struct TYdbSinkOptions
{
    TYdbTopologyOptions Topology;
    //! Target of non-federated writers.
    std::string Endpoint;
    //! Endpoint as configured, for logging.
    std::string DisplayEndpoint;
    std::string PayloadColumn;
    std::optional<std::string> AttributesColumn;
    i64 MaxMemoryUsageBytes = 50_MB;
    std::string Codec = "raw";
    i32 CompressionLevel = 4;
    bool WriteFlowQueueMeta = false;
    std::string FlowQueueMetaName;
    std::optional<EFramingFormat> FramingFormat;
};

struct TYdbTwoPhaseCommitOptions
{
    TYdbSinkOptions Sink;
    std::string WriterIdentityType;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<const TYdbSourceOptions> MakeYdbSourceOptions(
    const TYdbSourceParameters& parameters,
    TYdbPayloadSplitter splitPayload);
std::shared_ptr<const TYdbSourceDynamicOptions> MakeYdbSourceDynamicOptions(
    const TDynamicYdbSourceParameters& parameters);
TYdbTopologyOptions MakeYdbTopologyOptions(const TYdbTopicSpec& spec);
std::shared_ptr<const TYdbSinkOptions> MakeYdbSinkOptions(
    const TCommonYdbSinkParameters& parameters,
    std::optional<EFramingFormat> framingFormat);
std::shared_ptr<const TYdbBatchingOptions> MakeYdbBatchingOptions(i64 maxRowsPerBatch, i64 maxBytesPerBatch);
std::shared_ptr<const TYdbSinkControllerDynamicOptions> MakeYdbSinkControllerDynamicOptions(
    const TDynamicCommonYdbSinkParameters& parameters);
std::shared_ptr<const TYdbTwoPhaseCommitOptions> MakeYdbTwoPhaseCommitOptions(
    const TYdbTwoPhaseCommitSinkParameters& parameters);
std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> MakeYdbTwoPhaseCommitDynamicOptions(
    const TDynamicYdbTwoPhaseCommitSinkParameters& parameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
