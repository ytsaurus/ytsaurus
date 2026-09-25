#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Per-stream topic addressing, shared by the source and sink parameters. The connection and auth
//! settings live on the TKafkaClient resource instead.
struct TKafkaTopicSpec
    : public virtual NYTree::TYsonStruct
{
    //! Kafka topic name.
    std::string Topic;

    REGISTER_YSON_STRUCT(TKafkaTopicSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaTopicSpec);

////////////////////////////////////////////////////////////////////////////////

//! Source-side topic parameters: addressing plus the broker-facing periods. Sinks take only
//! #TKafkaTopicSpec — nothing on the sink path polls metadata.
struct TKafkaInfoSpec
    : public virtual TKafkaTopicSpec
{
    //! How often the info controller refreshes the topic partition count.
    TDuration UpdatePartitionCountPeriod;

    //! Timeout for blocking metadata/offset requests to the broker (both the info controller and the
    //! read session's watermark queries).
    TDuration MetadataTimeout;

    REGISTER_YSON_STRUCT(TKafkaInfoSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaInfoSpec);

////////////////////////////////////////////////////////////////////////////////

struct TKafkaSourceParameters
    : public TOrderedSourceBase::TParameters
    , public TKafkaInfoSpec
{
    //! Kafka consumer group id. Required by the consumer handle even though partition assignment is
    //! manual; also used as the best-effort lag-reporting commit target.
    std::string GroupId;

    //! Optional partition ranges [begin, end) to restrict which partitions this source reads.
    std::optional<std::vector<std::pair<int, int>>> PartitionFilter;

    //! Soft cap on the per-partition in-memory read buffer; the poll loop pauses fetching above it.
    i64 MaxBufferBytes = 0;

    //! At partition start, adopt the group's committed offset when it is ahead of Flow's persisted
    //! one, as the queue source does with its consumer. Off, the group is a lag-telemetry target only
    //! and Flow resumes from its own state; for pipelines sharing a group id.
    bool UseConsumerGroupOffset = true;

    REGISTER_YSON_STRUCT(TKafkaSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicKafkaSourceParameters
    : public TOrderedSourceBase::TDynamicParameters
{
    //! Blocking poll timeout for a single poll_batch call on the background poll thread.
    TDuration PollTimeout;

    //! How often the poll thread refreshes the partition low/high watermarks from the broker.
    TDuration WatermarkUpdatePeriod;

    //! Policy for a message that fails to unpack.
    EMalformedKafkaMessagePolicy MalformedMessagePolicy = EMalformedKafkaMessagePolicy::Fail;

    REGISTER_YSON_STRUCT(TDynamicKafkaSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicKafkaSourceParameters);

////////////////////////////////////////////////////////////////////////////////

//! Fields shared by all Kafka sink flavors.
struct TCommonKafkaSinkParameters
    : public virtual TKafkaTopicSpec
{
    //! Row column whose bytes become the Kafka message value.
    std::string PayloadColumn;

    //! Optional row column whose bytes become the Kafka message key (used by librdkafka to route the
    //! record to a partition). Empty means no key (round-robin partitioning).
    std::string KeyColumn;

    //! Optional Kafka record header carrying the Flow message id, so a consumer can deduplicate
    //! records replayed after a restart. Empty means no header is written.
    std::string MessageIdHeader;

    REGISTER_YSON_STRUCT(TCommonKafkaSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCommonKafkaSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TKafkaSinkParameters
    : public TOrderedAsyncSinkBase::TParameters
    , public virtual TCommonKafkaSinkParameters
{
    REGISTER_YSON_STRUCT(TKafkaSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaSinkParameters);

struct TDynamicKafkaSinkParameters
    : public TOrderedAsyncSinkBase::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicKafkaSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicKafkaSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TAtLeastOnceKafkaSinkParameters
    : public TSyncSinkBase::TParameters
    , public virtual TCommonKafkaSinkParameters
{
    REGISTER_YSON_STRUCT(TAtLeastOnceKafkaSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAtLeastOnceKafkaSinkParameters);

struct TDynamicAtLeastOnceKafkaSinkParameters
    : public TSyncSinkBase::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicAtLeastOnceKafkaSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicAtLeastOnceKafkaSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TKafkaSinkControllerParameters
    : public virtual ISink::TParameters
    , public virtual TCommonKafkaSinkParameters
{
    REGISTER_YSON_STRUCT(TKafkaSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaSinkControllerParameters);

struct TDynamicKafkaSinkControllerParameters
    : public virtual ISink::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TDynamicKafkaSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicKafkaSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
