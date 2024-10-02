#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeValuesConsumer
{
    // Uninitialized consumer indicates that requested object is not present.
    virtual void Initialize(const NYson::TProtobufMessageType* messageType = nullptr) = 0;

    virtual NYson::IYsonConsumer* OnValueBegin(const NYPath::TYPath& selector) = 0;
    virtual void OnValueEnd() = 0;

    virtual void Finalize(std::vector<TTimestamp> timestamps) = 0;

    virtual ~IAttributeValuesConsumer() = default;
};

using IAttributeValuesConsumerPtr = std::unique_ptr<IAttributeValuesConsumer>;

////////////////////////////////////////////////////////////////////////////////

struct IAttributeValuesConsumerGroup
{
    // NB! Creating new batch of consumers invalidates previous one.
    // All previously created consumers must not be used anymore.
    virtual std::vector<IAttributeValuesConsumerPtr> CreateConsumers(int count) = 0;
    virtual int TotalConsumers() const = 0;

    virtual ~IAttributeValuesConsumerGroup() = default;
};

using IAttributeValuesConsumerGroupPtr = std::unique_ptr<IAttributeValuesConsumerGroup>;

////////////////////////////////////////////////////////////////////////////////

struct IDefaultAttributeValuesConsumerGroup
    : public virtual IAttributeValuesConsumerGroup
{
    virtual std::vector<TAttributeValueList> Finalize() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IOptionalAttributeValuesConsumerGroup
    : public virtual IAttributeValuesConsumerGroup
{
    virtual std::vector<std::optional<TAttributeValueList>> Finalize() = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDefaultAttributeValuesConsumerGroup> MakeDefaultAttributeValuesConsumerGroup();

std::unique_ptr<IOptionalAttributeValuesConsumerGroup> MakeOptionalAttributeValuesConsumerGroup();

////////////////////////////////////////////////////////////////////////////////

void ConsumeValueLists(
    bool fetchRootObject,
    const TAttributeSelector& selector,
    IAttributeValuesConsumerGroup* consumerGroup,
    std::vector<TAttributeValueList*> lists);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
