#pragma once
#include "public.h"
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

namespace NYT::NFlow::NCompanion {

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
};

DEFINE_REFCOUNTED_TYPE(TCompanionPutJobRequest);

struct TCompanionPutJobResponse
    : public TRefCounted
{
    ECompanionResponseStatus Status{};
};

DEFINE_REFCOUNTED_TYPE(TCompanionPutJobResponse);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
