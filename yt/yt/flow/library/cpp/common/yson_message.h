#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TYsonMessageMeta
    : public NYTree::TYsonStruct
{
    TMessageId MessageId;
    TSystemTimestamp SystemTimestamp;
    TSystemTimestamp AlignmentTimestamp;
    TSystemTimestamp EventTimestamp;
    TStreamId StreamId;

    REGISTER_YSON_STRUCT(TYsonMessageMeta);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYsonMessageMeta);

////////////////////////////////////////////////////////////////////////////////

struct TYsonMessage
    : public NYTree::TYsonStruct
{
    TYsonMessageMetaPtr Meta;

    template <CYsonMessage T>
    TIntrusivePtr<T> As()
    {
        auto this_ = DynamicPointerCast<T>(MakeStrong(this));
        if (!this_) {
            THROW_ERROR_EXCEPTION("Unexpected message type")
                << TErrorAttribute("stream_id", Meta->StreamId)
                << TErrorAttribute("expected_type", TypeName<T>())
                << TErrorAttribute("got_type", TypeName(this->GetMeta()->GetStructType()));
        }
        return this_;
    }

    REGISTER_YSON_STRUCT(TYsonMessage);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYsonMessage);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr GetYsonMessagePayloadSchema(const TYsonMessagePtr& ysonMessage);

template <CYsonMessage T>
NTableClient::TTableSchemaPtr GetYsonMessagePayloadSchema()
{
    return GetYsonMessagePayloadSchema(New<T>());
}

TMessage ConvertToMessage(const TYsonMessagePtr& ysonStruct, const NTableClient::TTableSchemaPtr& schema);
void ConvertToYsonMessage(const TMessage& message, const TYsonMessagePtr& ysonStruct);

template <CYsonMessage T>
TIntrusivePtr<T> ConvertToYsonMessage(const TMessage& message)
{
    auto yson = New<T>();
    ConvertToYsonMessage(message, yson);
    return yson;
}

template <CYsonMessage T>
TIntrusivePtr<T> ConvertToYsonMessage(const TInputMessageConstPtr& message)
{
    return ConvertToYsonMessage<T>(*message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
