#pragma once

#ifndef TIMER_INL_H_
    #error "Direct inclusion of this file is not allowed, include timer.h"
    // For the sake of sane code completion.
    #include "timer.h"
#endif

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetColumnValue(const TTimer& timer, int columnId)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(timer, columnId));
}

template <class T>
T GetColumnValue(const TTimer& timer, TStringBuf columnName)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(timer, columnName));
}

template <class T>
T GetColumnValue(const TInputTimerConstPtr& timer, int columnId)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(timer, columnId));
}

template <class T>
T GetColumnValue(const TInputTimerConstPtr& timer, TStringBuf columnName)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(timer, columnName));
}

////////////////////////////////////////////////////////////////////////////////

class TTimerSerializer
    : public virtual NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TTimer, TTimerSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalBaseClassParameter("message_id", &TMessageMeta::MessageId);
        registrar.ExternalBaseClassParameter("system_timestamp", &TMessageMeta::SystemTimestamp);
        registrar.ExternalBaseClassParameter("event_timestamp", &TMessageMeta::EventTimestamp);
        registrar.ExternalBaseClassParameter("stream_id", &TMessageMeta::StreamId);
        registrar.ExternalClassParameter("key", &TThat::Key);
        registrar.ExternalClassParameter("key_schema", &TThat::KeySchema);
        registrar.ExternalClassParameter("trigger_timestamp", &TThat::TriggerTimestamp);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TTimer, TTimerSerializer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
