#pragma once
#include "public.h"
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NFlow::NProto::NCompanion {

class TCompanionResourceInstanceReference;

} // namespace NYT::NFlow::NProto::NCompanion

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Exact resource instance required by a job or another companion resource.
struct TCompanionResourceInstanceReference
    : public NYTree::TYsonStructLite
{
    TResourceId ResourceId;
    TResourceInstanceId IncarnationId;
    ui64 ConfigurationGeneration{};
    std::optional<TResourceId> Alias;

    bool operator==(const TCompanionResourceInstanceReference&) const = default;

    REGISTER_YSON_STRUCT_LITE(TCompanionResourceInstanceReference);

    static void Register(TRegistrar registrar);
};

void ToProto(
    NProto::NCompanion::TCompanionResourceInstanceReference* protoReference,
    const TCompanionResourceInstanceReference& reference);

void FromProto(
    TCompanionResourceInstanceReference* reference,
    const NProto::NCompanion::TCompanionResourceInstanceReference& protoReference);

////////////////////////////////////////////////////////////////////////////////

//! Creates local stream specs enriched with source stream schemas.
TStreamSpecsPtr CreateLocalStreamSpecs(
    const THashMap<TStreamId, NTableClient::TTableSchemaPtr>& sourceStreamsSchemas,
    const THashSet<TStreamId>& outputStreamIds,
    const TStreamSpecsPtr& streamSpecs);

////////////////////////////////////////////////////////////////////////////////

struct TCompanionState
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Payload;

    REGISTER_YSON_STRUCT(TCompanionState);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TStatePayload>
struct TStateItem
{
    TKey Key;
    bool Reset{};
    TStatePayload State;
};

template <typename TStatePayload>
struct TStateHolder
{
    std::string StateName;
    std::vector<TStateItem<TStatePayload>> StateItems;
    NTableClient::TTableSchemaPtr Schema;
};

struct TStreamWatermark
{
    TStreamId StreamId;
    TSystemTimestamp Watermark;
};

struct TNewTimer
{
    TSystemTimestamp TriggerTimestamp;
    std::optional<TSystemTimestamp> EventTimestamp;
    std::optional<TStreamId> StreamId;
};

struct TCompanionProcessRequest
    : public TRefCounted
{
    TJobId JobId;
    TComputationId ComputationId;
    std::vector<TInputMessageConstPtr> Messages;
    std::vector<TInputTimerConstPtr> Timers;
    std::vector<TInputVisitConstPtr> Visits;
    THashMap<std::string, TStateHolder<std::string>> InternalStates;
    THashMap<std::string, TStateHolder<TPayload>> ExternalStates;
    //! Read-only external state joined from another computation. Sent in the request only;
    //! never written back.
    THashMap<std::string, TStateHolder<TPayload>> JoinedExternalStates;
    std::vector<TStreamWatermark> Watermarks;
    // Flag indicating that companion client should send JobInfo along with request.
    bool SendJobInfo{};
    TComputationSpecPtr ComputationSpec;
    TDynamicComputationSpecPtr DynamicComputationSpec;
    //! Exact direct and transitive resources hosted in the companion.
    std::vector<TCompanionResourceInstanceReference> CompanionResources;
    // StreamSpecs for JobInfo publishing.
    TStreamSpecsPtr JobStreamSpecs;
    // StreamSpecs for streams overriding at source computations.
    TStreamSpecsPtr OverrideStreamSpecs;

    //! Returns TStreamSpecsPtr for message serialization and deserialization.
    TStreamSpecsPtr GetMessageStreamSpecs() const;
};

DEFINE_REFCOUNTED_TYPE(TCompanionProcessRequest);

struct TCompanionResponseGroup
{
    std::vector<TMessage> Messages;
    //! Per-message distribute flag, aligned with Messages. Empty means "distribute all".
    std::vector<bool> Distribute;
    std::vector<TNewTimer> Timers;
    std::vector<TMessageId> ParentIds;
};

struct TCompanionResponse
    : public TRefCounted
{
    ECompanionResponseStatus Status{};
    std::vector<TCompanionResponseGroup> Groups;
    std::vector<TStateHolder<std::string>> InternalStates;
    std::vector<TStateHolder<TPayload>> ExternalStates;
};

DEFINE_REFCOUNTED_TYPE(TCompanionResponse);

////////////////////////////////////////////////////////////////////////////////

struct TCompanionComputationInfo
    : public NYTree::TYsonStruct
{
    TComputationId ComputationId;
    ECompanionComputationType CompanionComputationType{};

    REGISTER_YSON_STRUCT(TCompanionComputationInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionComputationInfo);

struct TCompanionInfo
    : public NYTree::TYsonStruct
{
    //! Process serving the client channel used for this request.
    std::optional<i64> ProcessId;
    THashMap<TComputationId, TCompanionComputationInfoPtr> Computations;

    REGISTER_YSON_STRUCT(TCompanionInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionInfo);

////////////////////////////////////////////////////////////////////////////////

struct TCompanionPutJobRequest
    : public TRefCounted
{
    TJobId JobId;
    TComputationId ComputationId;
    TComputationSpecPtr ComputationSpec;
    TDynamicComputationSpecPtr DynamicComputationSpec;
    TStreamSpecsPtr JobStreamSpecs;
    //! Exact direct and transitive resources hosted in the companion.
    std::vector<TCompanionResourceInstanceReference> CompanionResources;
};

DEFINE_REFCOUNTED_TYPE(TCompanionPutJobRequest);

struct TCompanionPutJobResponse
    : public TRefCounted
{
    ECompanionResponseStatus Status{};
};

DEFINE_REFCOUNTED_TYPE(TCompanionPutJobResponse);

////////////////////////////////////////////////////////////////////////////////

struct TCompanionResourceExecuteResponse
    : public TRefCounted
{
    ECompanionResourceExecuteStatus Status{};
    TError Error;
};

DEFINE_REFCOUNTED_TYPE(TCompanionResourceExecuteResponse);

////////////////////////////////////////////////////////////////////////////////

//! Argument of the "init" resource command: the full static and dynamic resource specs.
struct TInitResourceCommandArg
    : public NYTree::TYsonStructLite
{
    TResourceSpecPtr Spec;
    TDynamicResourceSpecPtr DynamicSpec;
    TResourceInstanceId IncarnationId;
    ui64 IncarnationGeneration{};
    ui64 ConfigurationGeneration{};
    std::vector<TCompanionResourceInstanceReference> Dependencies;
    //! Worker-prepared revision exposed to the companion resource.
    TResourceRevisionPtr ResourceRevision;

    REGISTER_YSON_STRUCT_LITE(TInitResourceCommandArg);

    static void Register(TRegistrar registrar);
};

//! Argument of the "unload" resource command.
struct TUnloadResourceCommandArg
    : public NYTree::TYsonStructLite
{
    TResourceInstanceId IncarnationId;

    REGISTER_YSON_STRUCT_LITE(TUnloadResourceCommandArg);

    static void Register(TRegistrar registrar);
};

} // namespace NYT::NFlow::NCompanion
