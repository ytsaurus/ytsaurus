#pragma once

#include "public.h"

#include "kafka_client.h"
#include "read_session.h"
#include "spec.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/client/table_client/public.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TKey GenerateKafkaKey(std::string_view sourceIdentity, int partitionIndex);
int ExtractKafkaPartitionIndex(const TKey& key);
NTableClient::TTableSchemaPtr GetKafkaSourceSchema();

//! Partition indices in [0, |partitionCount|) that |filter| keeps, in ascending order. Each filter
//! entry is a half-open range [begin, end); no filter keeps every partition.
std::vector<int> SelectKafkaPartitions(
    int partitionCount,
    const std::optional<std::vector<std::pair<int, int>>>& filter);

//! Bytes one source row occupies besides its string data.
i64 GetKafkaRowOverhead();

//! Bytes |rowCount| rows occupy when their string columns hold |outputStringBytes| bytes in total
//! and each row owns a copy of a |keyBytes|-byte key.
i64 GetKafkaExpandedSize(i64 rowCount, i64 outputStringBytes, i64 keyBytes);

//! Bytes the rows unpacked from one record occupy.
i64 GetKafkaExpandedSize(const std::vector<TSharedRef>& frames, i64 keyBytes);

////////////////////////////////////////////////////////////////////////////////

struct TUnparsedKafkaPayload
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Data;

    REGISTER_YSON_STRUCT(TUnparsedKafkaPayload);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUnparsedKafkaPayload);

////////////////////////////////////////////////////////////////////////////////

//! Reads one Kafka partition. Flow owns the offset (persisted per epoch); the consumer is manually
//! assigned to the exact next offset via the background read session.
class TKafkaSource
    : public TIntegerOffsetOrderedSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicKafkaSourceParameters);

    using TSourceController = TKafkaSourceController;

    TKafkaSource(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext);

protected:
    //! Splits one Kafka message value into payload records; a throw is handled per the
    //! malformed-message policy. The identity base never throws — the policy is for unpacking
    //! variants that override this.
    virtual std::vector<TSharedRef> UnpackData(TSharedRef data) const;

    //! Per-record expansion limits; unbounded for the identity base.
    virtual i64 GetMaxFramesPerRecord() const;
    virtual i64 GetMaxExpandedBytesPerRecord() const;

private:
    void DoInit() final;
    void DoTerminate() final;

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffset,
        std::optional<TOffset> offsetLimitExclusive) final;

    void DoReportPersistedOffset(TOffset offset) final;

    std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> ProcessMessage(
        TKafkaReadSession::TKafkaMessage& message);

    void ValidateExpandedSize(
        i64 expandedSize,
        const TKafkaReadSession::TKafkaMessage& message) const;

    //! Applies the malformed-message policy to a record that failed to unpack.
    std::pair<std::vector<TPayload>, NTableClient::TTableSchemaPtr> HandleMalformedMessage(
        const TKafkaReadSession::TKafkaMessage& message,
        const std::string& data,
        TStringBuf keyBuf,
        i64 keyBytes,
        const std::exception& ex);

    //! Drops held messages below |nextOffset|. A larger offset is not a seek: compaction and
    //! transaction markers leave holes in the log.
    void TryAdvancePendingMessages(i64 nextOffset);

    void TryUpdatePartitionInfo();

    //! Hands a start offset ahead of the persisted one to the base as a repositioned partition info
    //! update; idempotent.
    void TryReposition();

    const NTableClient::TTableSchemaPtr Schema_;
    const std::string Topic_;
    const int PartitionIndex_;

protected:
    const NLogging::TLogger Logger;

private:
    const TKafkaClientPtr Client_;

    TKafkaReadSessionPtr ReadSession_;
    //! Fetched messages left over when a batch is cut by unpacked rows; the session cannot serve
    //! them again without a seek.
    std::deque<TKafkaReadSession::TKafkaMessage> PendingMessages_;

    NConcurrency::TPeriodicExecutorPtr PartitionInfoUpdater_;
    IStatusErrorStatePtr UpdatePartitionInfoErrorState_;
    IStatusErrorStatePtr ReadErrorState_;
    IStatusErrorStatePtr TopicIdentityErrorState_;
    //! Sticky; set when the watermarks contradict the persisted offset (topic recreated under the
    //! same name). Hard-stops #DoReadNextBatch so nothing is served across the discontinuity.
    TError TopicIdentityError_;
    NProfiling::TCounter MalformedMessagesCounter_;

    i64 PersistedOffsetExclusive_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TKafkaSource);

////////////////////////////////////////////////////////////////////////////////

class TKafkaSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TKafkaSourceParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TDynamicKafkaSourceParameters);

    TKafkaSourceController(
        TSourceControllerContextPtr context,
        TDynamicSourceControllerContextPtr dynamicContext);

    void Init(IInitContextPtr initContext) final;
    void Sync() final;
    void Commit() final;

    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override;

    std::string GetSourceIdentity() const override;

private:
    const std::string ClusterIdentity_;
    const TKafkaInfoControllerPtr Info_;
};

DEFINE_REFCOUNTED_TYPE(TKafkaSourceController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
