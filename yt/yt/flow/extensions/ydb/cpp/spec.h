#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sync_sink_base.h>

#include <yt/yt/flow/library/cpp/computation/swift_ordered_source_computation.h>

#include <yt/yt/flow/extensions/framing/cpp/public.h>

#include <ydb-cpp-sdk/client/topic/codecs.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYdbTopicSpec
    : public virtual NYTree::TYsonStruct
{
    std::string Endpoint;
    std::string Database;
    std::string Topic;

    TDuration ClientControllerTimeout;

    TDuration UpdatePartitionCountPeriod;

    REGISTER_YSON_STRUCT(TYdbTopicSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbTopicSpec);

////////////////////////////////////////////////////////////////////////////////

struct TYdbSourceParameters
    : public TOrderedSourceBase::TParameters
    , public TYdbTopicSpec
{
    std::string Consumer;

    i64 MaxMemoryUsageBytes{};

    TDuration StartSessionTimeout;

    bool TryParseFlowQueueMeta{};
    std::string FlowQueueMetaName;
    bool IgnoreMalformedFlowQueueMeta{};

    //! Column to materialize the attributes of the read topic message into, as a YSON map.
    std::optional<std::string> AttributesColumn;

    std::optional<std::vector<std::pair<int, int>>> PartitionFilter;

    REGISTER_YSON_STRUCT(TYdbSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbSourceParameters
    : public TOrderedSourceBase::TDynamicParameters
{
    EMalformedYdbMessagePolicy MalformedMessagePolicy{};

    std::optional<TInstant> ReadFromInstant;
    std::optional<TDuration> MaxTimeLag;
    TDuration CommitTimeout; // How long to wait ordinary commit before trying to do it in a hard way.
    // How long an established read session may deliver nothing at all - not a message, not an answer to
    // a status request - before it is considered wedged and recreated.
    TDuration SessionSilenceTimeout;

    REGISTER_YSON_STRUCT(TDynamicYdbSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbSourceParameters);

////////////////////////////////////////////////////////////////////////////////

//! Set by the source controller, which orders partitions that a topic split produced after their parents.
struct TYdbSourcePartitionSpec
    : public TOrderedSourceBase::TDynamicPartitionSpec
{
    bool ReadBlocked{};
    //! The partition no longer exists: the source only drains what Flow already read from it.
    bool Retired{};
    std::optional<i64> FinalOffsetExclusive;
    bool CompletionAllowed = true;
    //! The partition cannot read because its cluster fails to describe the topic.
    bool TopologyUnavailable{};

    REGISTER_YSON_STRUCT(TYdbSourcePartitionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbSourcePartitionSpec);

////////////////////////////////////////////////////////////////////////////////

struct TYdbSourcePartitionStatus
    : public NYTree::TYsonStruct
{
    bool InputExhausted{};

    REGISTER_YSON_STRUCT(TYdbSourcePartitionStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbSourcePartitionStatus);

////////////////////////////////////////////////////////////////////////////////

struct TYdbFramingSourceParameters
    : public TYdbSourceParameters
{
    EFramingFormat FramingFormat{};

    REGISTER_YSON_STRUCT(TYdbFramingSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbFramingSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbFramingSourceParameters
    : public TDynamicYdbSourceParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbFramingSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbFramingSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbMultiLineSourceParameters
    : public TYdbSourceParameters
{
    char RowDelimiter{};

    REGISTER_YSON_STRUCT(TYdbMultiLineSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbMultiLineSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbMultiLineSourceParameters
    : public TDynamicYdbSourceParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbMultiLineSourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbMultiLineSourceParameters);

////////////////////////////////////////////////////////////////////////////////

struct TCommonYdbSinkParameters
    : public TYdbTopicSpec
{
    std::string PayloadColumn;

    //! Column holding a YSON map of user attributes to attach to the topic message.
    //! See #PackYdbTopicAttributes(). Not supported by the framing sinks.
    std::optional<std::string> AttributesColumn;

    i64 MaxMemoryUsageBytes{};

    std::string Codec;
    i32 CompressionLevel{};

    bool WriteFlowQueueMeta{};
    std::string FlowQueueMetaName;

    REGISTER_YSON_STRUCT(TCommonYdbSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCommonYdbSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCommonYdbSinkParameters
    : public virtual NYTree::TYsonStruct
{
    TDuration FlowQueueMetaHeartbeatPeriod;

    REGISTER_YSON_STRUCT(TDynamicCommonYdbSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCommonYdbSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbSinkParameters
    : public virtual TOrderedAsyncSinkBase::TParameters
    , public virtual TCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TYdbSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbSinkParameters
    : public virtual TOrderedAsyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbFramingSinkParameters
    : public virtual TOrderedBatchingAsyncSinkBase::TParameters
    , public virtual TCommonYdbSinkParameters
{
    EFramingFormat FramingFormat{};

    REGISTER_YSON_STRUCT(TYdbFramingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbFramingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbFramingSinkParameters
    : public TOrderedBatchingAsyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbFramingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbFramingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbTwoPhaseCommitSinkParameters
    : public virtual TSinkBase::TParameters
    , public virtual TCommonYdbSinkParameters
{
    std::optional<EFramingFormat> FramingFormat;

    REGISTER_YSON_STRUCT(TYdbTwoPhaseCommitSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbTwoPhaseCommitSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbTwoPhaseCommitSinkParameters
    : public virtual TSinkBase::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    i64 MaxRowsPerBatch{};
    i64 MaxBytesPerBatch{};
    TDuration PreparationAttemptTimeout;
    TDuration PreparationTargetBackoff;

    REGISTER_YSON_STRUCT(TDynamicYdbTwoPhaseCommitSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbTwoPhaseCommitSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbAtLeastOnceSinkParameters
    : public virtual TSyncSinkBase::TParameters
    , public virtual TCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TYdbAtLeastOnceSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbAtLeastOnceSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbAtLeastOnceSinkParameters
    : public virtual TSyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbAtLeastOnceSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbAtLeastOnceSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbAtLeastOnceFramingSinkParameters
    : public virtual TSyncSinkBase::TParameters
    , public virtual TCommonYdbSinkParameters
{
    EFramingFormat FramingFormat{};

    REGISTER_YSON_STRUCT(TYdbAtLeastOnceFramingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYdbAtLeastOnceFramingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbAtLeastOnceFramingSinkParameters
    : public virtual TSyncSinkBase::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    i64 MaxRowsPerBatch{};
    i64 MaxBytesPerBatch{};

    REGISTER_YSON_STRUCT(TDynamicYdbAtLeastOnceFramingSinkParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicYdbAtLeastOnceFramingSinkParameters);

////////////////////////////////////////////////////////////////////////////////

struct TYdbSinkControllerParameters
    : public virtual ISink::TParameters
    , public virtual TCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TYdbSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicYdbSinkControllerParameters
    : public virtual ISink::TDynamicParameters
    , public virtual TDynamicCommonYdbSinkParameters
{
    REGISTER_YSON_STRUCT(TDynamicYdbSinkControllerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
