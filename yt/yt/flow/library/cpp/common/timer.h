#pragma once

#include "public.h"

#include "message.h"
#include "payload_validation.h"
#include <yt/yt/flow/library/cpp/common/proto/timer.pb.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Timer class.
//! Class has externalized yson serializer.
struct TTimer
    : public TTimerMeta
{
    TKey Key;
    NTableClient::TTableSchemaPtr KeySchema;
    TSystemTimestamp TriggerTimestamp;
};

void FormatValue(TStringBuilderBase* builder, const TTimer& timer, TStringBuf /*spec*/);

i64 GetTimerMetaByteSize(const TTimerMeta& meta);
i64 GetTimerByteSize(const TTimer& timer);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TTimer& timer, int columnId);
// Inefficient
NTableClient::TUnversionedValue GetColumn(const TTimer& timer, TStringBuf columnName);

template <class T>
T GetColumnValue(const TTimer& timer, int columnId);

// Inefficient
template <class T>
T GetColumnValue(const TTimer& timer, TStringBuf columnName);

////////////////////////////////////////////////////////////////////////////////

void ValidateTimer(const TTimer& timer, const TValidatePayloadOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

//! Validated input timer with computed size.
//! Consider it as immutable and pass it as TInputTimerConstPtr.
struct TInputTimer
    : public TMessageMetaOwner
    , public TTimer
{
    i64 ByteSize;

    TInputTimer(TTimer&& timer, const NTableClient::TTableSchemaPtr& expectedKeySchema = nullptr);

    const TMessageMeta& GetMeta() const final;
};

DEFINE_REFCOUNTED_TYPE(TInputTimer);

void FormatValue(TStringBuilderBase* builder, const TInputTimerConstPtr& timer, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TInputTimerConstPtr& timer, int columnId);
// Inefficient
NTableClient::TUnversionedValue GetColumn(const TInputTimerConstPtr& timer, TStringBuf columnName);

template <class T>
T GetColumnValue(const TInputTimerConstPtr& timer, int columnId);

// Inefficient
template <class T>
T GetColumnValue(const TInputTimerConstPtr& timer, TStringBuf columnName);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTimer* protoTimer, const TTimer& timer);

void FromProto(TTimer* timer, const NProto::TTimer& protoTimer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define TIMER_INL_H_
#include "timer-inl.h"
#undef TIMER_INL_H_
