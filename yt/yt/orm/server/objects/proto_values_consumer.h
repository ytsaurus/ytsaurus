#pragma once

#include "attribute_values_consumer.h"

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
class TProtoValuesConsumerGroupBase
    : public IAttributeValuesConsumerGroup
{
public:
    using TContainerValueType = typename TContainer::value_type;

    TProtoValuesConsumerGroupBase(TContainer& container);

    std::vector<IAttributeValuesConsumerPtr> CreateConsumers(int count) override;
    int TotalConsumers() const override;

protected:
    TContainer& Container_;
    int Allocated_ = 0;

    virtual IAttributeValuesConsumerPtr MakeConsumer(TContainerValueType& item) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TProtoFormatConsumerState
{
public:
    TProtoFormatConsumerState(
        const NYson::TProtobufMessageType* rootType,
        NYson::EUnknownYsonFieldsMode unknownFieldsMode,
        TString* outputBuffer);

    NYson::IYsonConsumer* GetConsumer();

private:
    google::protobuf::io::StringOutputStream Stream_;
    const std::unique_ptr<NYson::IYsonConsumer> Consumer_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObjectServiceProtoModule>
typename TObjectServiceProtoModule::TPayload YsonStringToPayload(
    const NYson::TYsonString& ysonString,
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    typename TObjectServiceProtoModule::EPayloadFormat format);

template <class TObjectServiceProtoModule, class TContainer>
IAttributeValuesConsumerGroupPtr MakeProtoConsumerGroup(
    TContainer& container,
    typename TObjectServiceProtoModule::EPayloadFormat format,
    const NYson::TProtobufMessageType* protobufMessageType,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    bool fetchRootObject);

template <class TObjectServiceProtoModule>
void MoveObjectResultToProto(
    typename TObjectServiceProtoModule::EPayloadFormat format,
    const NYson::TProtobufMessageType* protobufMessageType,
    NObjects::TAttributeValueList* object,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    typename TObjectServiceProtoModule::TAttributeList* protoResult,
    bool fetchRootObject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define PROTO_VALUES_CONSUMER_INL_H_
#include "proto_values_consumer-inl.h"
#undef PROTO_VALUES_CONSUMER_INL_H_
