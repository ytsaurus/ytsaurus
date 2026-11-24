#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/misc/enum.h>

#include <map>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

inline const TResourceId YdbDriverDefaultResourceId = "YdbDriver";

inline const std::string YdbTwoPhaseCommitWriterIdentityType = "ytflow-ydb-two-phase-commit";

////////////////////////////////////////////////////////////////////////////////

//! YDB topic message attributes; sorted, so that the message meta order is deterministic.
using TYdbTopicAttributes = std::map<std::string, std::string>;

DEFINE_ENUM(EMalformedYdbMessagePolicy,
    ((Keep)   (0))
    ((Drop)   (1))
    ((Fail)   (2))
);

DEFINE_ENUM(EYdbAuthMode,
    ((Auto)       (0))
    ((Token)      (1))
    ((Login)      (2))
    ((Anonymous)  (3))
);

////////////////////////////////////////////////////////////////////////////////

struct TYdbWriteTarget;
struct TYdbWriteTargetHash;
struct TYdbWriteTargetResolution;

struct TYdbTopologyOptions;
struct TYdbSourceOptions;
struct TYdbSourceDynamicOptions;
struct TYdbBatchingOptions;
struct TYdbSinkOptions;
struct TYdbSinkControllerDynamicOptions;
struct TYdbTwoPhaseCommitOptions;
struct TYdbTwoPhaseCommitDynamicOptions;

DECLARE_REFCOUNTED_STRUCT(IYdbClientFactory);
DECLARE_REFCOUNTED_STRUCT(IYdbWriter);
DECLARE_REFCOUNTED_STRUCT(IDeferredPublicationClient);

DECLARE_REFCOUNTED_CLASS(TRetryableYdbWriter);
DECLARE_REFCOUNTED_CLASS(TYdbDriverCore);
DECLARE_REFCOUNTED_STRUCT(TYdbTopicDriverConfig);
DECLARE_REFCOUNTED_CLASS(TYdbTopicDriver);

DECLARE_REFCOUNTED_CLASS(TYdbSourceReadSession);
DECLARE_REFCOUNTED_CLASS(TYdbSourceEngine);
DECLARE_REFCOUNTED_CLASS(TYdbTopicInfoController);
DECLARE_REFCOUNTED_CLASS(TYdbSinkEngine);
DECLARE_REFCOUNTED_CLASS(TYdbSinkControllerEngine);
DECLARE_REFCOUNTED_CLASS(TYdbDeferredPublication);
DECLARE_REFCOUNTED_CLASS(TYdbTwoPhaseCommitEngine);

DECLARE_REFCOUNTED_STRUCT(TYdbTopicSpec);
DECLARE_REFCOUNTED_STRUCT(TYdbSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbSourcePartitionSpec);
DECLARE_REFCOUNTED_STRUCT(TYdbSourcePartitionStatus);
DECLARE_REFCOUNTED_STRUCT(TYdbFramingSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbFramingSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbMultiLineSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbMultiLineSourceParameters);
DECLARE_REFCOUNTED_STRUCT(TCommonYdbSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicCommonYdbSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbFramingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbFramingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbTwoPhaseCommitSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbTwoPhaseCommitSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbAtLeastOnceSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbAtLeastOnceSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TYdbAtLeastOnceFramingSinkParameters);
DECLARE_REFCOUNTED_STRUCT(TDynamicYdbAtLeastOnceFramingSinkParameters);

DECLARE_REFCOUNTED_STRUCT(TUnparsedYdbPayload);
DECLARE_REFCOUNTED_STRUCT(TYdbClusterInfo);
DECLARE_REFCOUNTED_STRUCT(TYdbTopicInfoControllerState);
DECLARE_REFCOUNTED_STRUCT(TYdbPartitionNodeState);
DECLARE_REFCOUNTED_STRUCT(TYdbPartitionGraphClusterState);
DECLARE_REFCOUNTED_STRUCT(TYdbPartitionGraphState);
DECLARE_REFCOUNTED_STRUCT(TYdbHeartbeatState);
DECLARE_REFCOUNTED_STRUCT(TYdbTwoPhaseCommitSinkState);
DECLARE_REFCOUNTED_STRUCT(TYdbPendingPublicationState);
DECLARE_REFCOUNTED_STRUCT(TYdbDeferredPublicationState);

DECLARE_REFCOUNTED_CLASS(TYdbSourceBase);
DECLARE_REFCOUNTED_CLASS(TYdbSource);
DECLARE_REFCOUNTED_CLASS(TYdbMultiLineSource);
DECLARE_REFCOUNTED_CLASS(TYdbFramingSource);
DECLARE_REFCOUNTED_CLASS(TYdbSourceController);
DECLARE_REFCOUNTED_CLASS(TYdbSink);
DECLARE_REFCOUNTED_CLASS(TYdbFramingSink);
DECLARE_REFCOUNTED_CLASS(TYdbAtLeastOnceSink);
DECLARE_REFCOUNTED_CLASS(TYdbAtLeastOnceFramingSink);
DECLARE_REFCOUNTED_CLASS(TYdbSinkController);
DECLARE_REFCOUNTED_CLASS(TYdbTwoPhaseCommitSink);
DECLARE_REFCOUNTED_CLASS(TYdbTwoPhaseCommitSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
