#ifndef PROTO_VALUES_CONSUMER_INL_H_
#error "Direct inclusion of this file is not allowed, include proto_values_consumer.h"
// For the sake of sane code completion.
#include "proto_values_consumer.h"
#endif

#include "attribute_values_consumer.h"
#include "helpers.h"

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

#include <yt/yt/orm/library/attributes/merge_attributes.h>
#include <yt/yt/orm/library/attributes/yson_builder.h>

#include <google/protobuf/repeated_field.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TContainer>
TProtoValuesConsumerGroupBase<TContainer>::TProtoValuesConsumerGroupBase(
    TContainer& container)
    : Container_(container)
{ }

template <class TContainer>
std::vector<IAttributeValuesConsumerPtr> TProtoValuesConsumerGroupBase<TContainer>::CreateConsumers(int count)
{
    auto [iter, end] = EmplaceBatch(Container_, count, /*alreadyAllocatedCount*/ Allocated_);
    std::vector<IAttributeValuesConsumerPtr> result;
    result.reserve(count);
    for (; iter != end; ++iter) {
        result.push_back(MakeConsumer(*iter));
    }

    Allocated_ += count;
    return result;
}

template <class TContainer>
int TProtoValuesConsumerGroupBase<TContainer>::TotalConsumers() const
{
    return Allocated_;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TObjectServiceProtoModule, class TContainer>
class TPayloadAttributeValuesConsumerGroup
    : public TProtoValuesConsumerGroupBase<TContainer>
{
public:
    using TConsumerGroup = TPayloadAttributeValuesConsumerGroup;
    using TPayload = typename NClient::NProto::TPayload;
    using TRepeatedPayloads = google::protobuf::RepeatedPtrField<TPayload>;
    using EPayloadFormat = typename NClient::NProto::EPayloadFormat;
    using TAttributeList = typename TObjectServiceProtoModule::TAttributeList;
    using TContainerValueType = typename TContainer::value_type;

    struct TCommonOptions
    {
        const NYson::TProtobufMessageType* RootType;
        NYson::EUnknownYsonFieldsMode UnknownFieldsMode;
        EPayloadFormat Format;
    };

    class TConsumerBase
        : public IAttributeValuesConsumer
    {
    public:
        TConsumerBase(const TConsumerGroup* group, TContainerValueType& listHolder)
            : Group_(group)
            , ListHolder_(listHolder)
        { }

        void Initialize(const NYson::TProtobufMessageType* messageType) override
        {
            MessageType_ = messageType;
            List_ = AccessAttributeList<TAttributeList>(ListHolder_);
        }

        void Finalize(std::vector<TTimestamp> timestamps) override
        {
            List_->mutable_timestamps()->Reserve(timestamps.size());
            for (auto timestamp : timestamps) {
                List_->add_timestamps(timestamp);
            }
        }

    protected:
        const TConsumerGroup* Group_;
        TContainerValueType& ListHolder_;
        TAttributeList* List_;
        const NYson::TProtobufMessageType* MessageType_;
    };

    class TDeprecatedFormatConsumer
        : public TConsumerBase
    {
    public:
        using TConsumerBase::TConsumerBase;

        NYson::IYsonConsumer* OnValueBegin(const NYPath::TYPath& /*selector*/) override
        {
            return Builder_.GetConsumer();
        }

        void OnValueEnd() override
        {
            *this->List_->mutable_values()->Add() = this->Builder_.Flush().ToString();
        }

    private:
        TYsonStringBuilder Builder_;
    };

    class TYsonFormatConsumer
        : public TConsumerBase
    {
    public:
        using TConsumerBase::TConsumerBase;

        NYson::IYsonConsumer* OnValueBegin(const NYPath::TYPath& /*selector*/) override
        {
            return Builder_.GetConsumer();
        }

        void OnValueEnd() override
        {
            auto ysonString = Builder_.Flush();
            YT_VERIFY(ysonString);
            this->List_->mutable_value_payloads()->Add()->set_yson(ysonString.ToString());
        }

    private:
        TYsonStringBuilder Builder_;
    };

    class TProtoFormatConsumer
        : public TConsumerBase
    {
    public:
        using TConsumerBase::TConsumerBase;

        NYson::IYsonConsumer* OnValueBegin(const NYPath::TYPath& selector) override
        {
            const auto* type = this->MessageType_ ? this->MessageType_ : this->Group_->Options_.RootType;
            THROW_ERROR_EXCEPTION_UNLESS(type, "Protobuf message type was not given");
            const auto* payloadType = NAttributes::GetMessageTypeByYPath(
                type,
                selector,
                /*allowAttributeDictionary*/ true);
            State_.emplace(
                payloadType,
                this->Group_->Options_.UnknownFieldsMode,
                /*outputBuffer*/ this->List_->mutable_value_payloads()->Add()->mutable_protobuf());
            return State_->GetConsumer();
        }

        void OnValueEnd() override
        {
            State_.reset();
        }

    private:
        std::optional<TProtoFormatConsumerState> State_;
    };

public:
    TPayloadAttributeValuesConsumerGroup(
        const TCommonOptions& options,
        TContainer& container)
        : TProtoValuesConsumerGroupBase<TContainer>(container)
        , Options_(options)
    {}

private:
    const TCommonOptions Options_;

    IAttributeValuesConsumerPtr MakeConsumer(TContainerValueType& listHolder) const override
    {
        switch (Options_.Format) {
            case EPayloadFormat::PF_NONE:
                return std::make_unique<TDeprecatedFormatConsumer>(this, listHolder);
            case EPayloadFormat::PF_YSON:
                return std::make_unique<TYsonFormatConsumer>(this, listHolder);
            case EPayloadFormat::PF_PROTOBUF:
                return std::make_unique<TProtoFormatConsumer>(this, listHolder);
        }

        YT_ABORT();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TObjectServiceProtoModule>
inline typename NClient::NProto::TPayload YsonStringToPayload(
    const NYson::TYsonString& ysonString,
    const NYson::TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    typename NClient::NProto::EPayloadFormat format)
{
    typename NClient::NProto::TPayload payload;
    if (!ysonString) {
        payload.set_null(true);
        return payload;
    }
    switch (format) {
        case NClient::NProto::EPayloadFormat::PF_YSON: {
            payload.set_yson(ysonString.ToString());
            break;
        }

        case NClient::NProto::EPayloadFormat::PF_PROTOBUF: {
            const auto* payloadType = NAttributes::GetMessageTypeByYPath(
                rootType,
                path,
                /*allowAttributeDictionary*/ true);
            *payload.mutable_protobuf() = YsonStringToProto(ysonString, payloadType, unknownFieldsMode);
            break;
        }

        default:
            YT_ABORT();
    }
    return payload;
}

template <class TObjectServiceProtoModule, class TContainer>
IAttributeValuesConsumerGroupPtr MakeProtoConsumerGroup(
    TContainer& container,
    typename NClient::NProto::EPayloadFormat format,
    const NYson::TProtobufMessageType* protobufMessageType,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode)
{
    using TConsumerGroup = NObjects::NDetail::TPayloadAttributeValuesConsumerGroup<
        TObjectServiceProtoModule,
        TContainer>;
    typename TConsumerGroup::TCommonOptions options{
        .RootType = protobufMessageType,
        .UnknownFieldsMode = unknownFieldsMode,
        .Format = format,
    };

    return std::make_unique<TConsumerGroup>(options, container);
}

template <class TObjectServiceProtoModule>
inline void MoveObjectResultToProto(
    typename NClient::NProto::EPayloadFormat format,
    const NYson::TProtobufMessageType* protobufMessageType,
    const NObjects::TAttributeSelector& selector,
    NObjects::TAttributeValueList* object,
    NYson::EUnknownYsonFieldsMode unknownFieldsMode,
    typename TObjectServiceProtoModule::TAttributeList* protoResult,
    bool fetchRootObject)
{
    std::span<typename TObjectServiceProtoModule::TAttributeList> container(protoResult, protoResult + 1);
    auto consumer = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
        container,
        format,
        protobufMessageType,
        unknownFieldsMode);
    ConsumeValueLists(fetchRootObject, selector, consumer.get(), {object});
    YT_VERIFY(consumer->TotalConsumers() == 1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
