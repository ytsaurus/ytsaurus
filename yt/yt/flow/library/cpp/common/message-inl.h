#pragma once

#ifndef MESSAGE_INL_H_
    #error "Direct inclusion of this file is not allowed, include message.h"
    // For the sake of sane code completion.
    #include "message.h"
#endif

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetColumnValue(const TMessage& message, int columnId)
{
    return GetColumnValue<T>(message.Payload, columnId);
}

template <class T>
T GetColumnValue(const TMessage& message, TStringBuf columnName)
{
    return GetColumnValue<T>(message.Payload, message.PayloadSchema, columnName);
}

////////////////////////////////////////////////////////////////////////////////

class TMessageSerializer
    : public virtual NYTree::TExternalizedYsonStruct
{
public:
    REGISTER_EXTERNALIZED_YSON_STRUCT(TMessage, TMessageSerializer);

    static void Register(TRegistrar registrar)
    {
        registrar.ExternalBaseClassParameter("message_id", &TMessageMeta::MessageId);
        registrar.ExternalBaseClassParameter("system_timestamp", &TMessageMeta::SystemTimestamp);
        registrar.ExternalBaseClassParameter("alignment_timestamp", &TMessageMeta::AlignmentTimestamp);
        registrar.ExternalBaseClassParameter("event_timestamp", &TMessageMeta::EventTimestamp);
        registrar.ExternalBaseClassParameter("stream_id", &TMessageMeta::StreamId);
        registrar.ExternalClassParameter("payload", &TThat::Payload);
        registrar.ExternalClassParameter("payload_schema", &TThat::PayloadSchema);
    }
};

ASSIGN_EXTERNAL_YSON_SERIALIZER(TMessage, TMessageSerializer);

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetColumnValue(const TInputMessageConstPtr& message, int columnId)
{
    return GetColumnValue<T>(*message, columnId);
}

template <class T>
T GetColumnValue(const TInputMessageConstPtr& message, TStringBuf columnName)
{
    return GetColumnValue<T>(*message, columnName);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires(std::is_base_of_v<TMessageMetaOwner, T>)
const TMessageId& TMessageHashMapOpsByMessageId::GetMessageId(const TIntrusivePtr<T>& value) const
{
    return value->MessageId;
}

template <class TLeft, class TRight>
bool TMessageHashMapOpsByMessageId::operator()(const TLeft& left, const TRight& right) const
{
    return GetMessageId(left) == GetMessageId(right);
}

template <typename T>
size_t TMessageHashMapOpsByMessageId::operator()(const T& value) const
{
    return THash<TMessageId>::operator()(GetMessageId(value));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
