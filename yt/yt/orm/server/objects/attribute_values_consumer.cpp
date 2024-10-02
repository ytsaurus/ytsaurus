#include "attribute_values_consumer.h"

#include "helpers.h"

#include <library/cpp/iterator/zip.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TValueType>
    requires (std::is_same_v<TValueType, TAttributeValueList> ||
        std::is_same_v<TValueType, std::optional<TAttributeValueList>>)
class TAttributeValuesConsumer
    : public IAttributeValuesConsumer
{
public:
    explicit TAttributeValuesConsumer(TValueType& valueHolder)
        : ValueHolder_(valueHolder)
    { }

    void Initialize(const NYson::TProtobufMessageType*) override
    {
        if constexpr (std::is_same_v<TValueType, TAttributeValueList>) {
            ValueList_ = &ValueHolder_;
        } else {
            ValueList_ = &ValueHolder_.emplace();
        }
    }

    NYson::IYsonConsumer* OnValueBegin(const NYPath::TYPath& /*selector*/) override
    {
        YT_VERIFY(Builder_.IsEmpty());
        return Builder_.GetConsumer();
    }

    void OnValueEnd() override
    {
        ValueList_->Values.push_back(Builder_.Flush());
    }

    void Finalize(std::vector<TTimestamp> timestamps) override
    {
        ValueList_->Timestamps = std::move(timestamps);
    }

private:
    TValueType& ValueHolder_;
    TAttributeValueList* ValueList_;
    TYsonStringBuilder Builder_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TValueType>
class TAttributeValuesConsumerGroup
    : public virtual IAttributeValuesConsumerGroup
{
public:
    std::vector<IAttributeValuesConsumerPtr> CreateConsumers(int count) override
    {
        auto [iter, end] = EmplaceBatch<std::vector<TValueType>>(
            Result_,
            count,
            /*alreadyAllocatedCount*/ std::ssize(Result_));
        std::vector<IAttributeValuesConsumerPtr> result;
        result.reserve(count);
        for (; iter != end; ++iter) {
            result.push_back(std::make_unique<TAttributeValuesConsumer<TValueType>>(*iter));
        }

        return result;
    }

    virtual int TotalConsumers() const override
    {
        return Result_.size();
    }

protected:
    std::vector<TValueType> Result_;
};

////////////////////////////////////////////////////////////////////////////////

class TDefaultAttributeValuesConsumerGroup
    : public TAttributeValuesConsumerGroup<TAttributeValueList>
    , public IDefaultAttributeValuesConsumerGroup
{
public:
    using TAttributeValuesConsumerGroup::TAttributeValuesConsumerGroup;

    std::vector<TAttributeValueList> Finalize() override
    {
        return std::move(Result_);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOptionalAttributeValuesConsumerGroup
    : public TAttributeValuesConsumerGroup<std::optional<TAttributeValueList>>
    , public IOptionalAttributeValuesConsumerGroup
{
public:
    using TAttributeValuesConsumerGroup::TAttributeValuesConsumerGroup;

    std::vector<std::optional<TAttributeValueList>> Finalize() override
    {
        return std::move(Result_);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IDefaultAttributeValuesConsumerGroup> MakeDefaultAttributeValuesConsumerGroup()
{
    return std::make_unique<TDefaultAttributeValuesConsumerGroup>();
}

std::unique_ptr<IOptionalAttributeValuesConsumerGroup> MakeOptionalAttributeValuesConsumerGroup()
{
    return std::make_unique<TOptionalAttributeValuesConsumerGroup>();
}

////////////////////////////////////////////////////////////////////////////////

void ConsumeValueLists(
    bool fetchRootObject,
    const TAttributeSelector& selector,
    IAttributeValuesConsumerGroup* consumerGroup,
    std::vector<TAttributeValueList*> lists)
{
    auto consumers = consumerGroup->CreateConsumers(lists.size());
    for (auto [consumer, valueList] : Zip(consumers, lists)) {
        consumer->Initialize();
        if (fetchRootObject) {
            YT_VERIFY(std::size(valueList->Values) == 1);
            consumer->OnValueBegin(/*selector*/ {})->OnRaw(valueList->Values[0]);
            consumer->OnValueEnd();
        } else {
            YT_VERIFY(std::ssize(valueList->Values) == std::ssize(selector.Paths));
            for (const auto& [value, selectorPath] : Zip(valueList->Values, selector.Paths)) {
                consumer->OnValueBegin(selectorPath)->OnRaw(value);
                consumer->OnValueEnd();
            }
        }
        consumer->Finalize(valueList->Timestamps);

        // Reclaim memory.
        valueList->Values.clear();
        valueList->Values.shrink_to_fit();
        valueList->Timestamps.clear();
        valueList->Timestamps.shrink_to_fit();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
