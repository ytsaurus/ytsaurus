#ifndef PROTO_VISITOR_TRAITS_INL_H_
#error "Direct inclusion of this file is not allowed, include proto_visitor_traits.h"
// For the sake of sane code completion.
#include "proto_visitor_traits.h"
#endif

#include "helpers.h"

#include <type_traits>

namespace NYT::NOrm::NAttributes {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
void Reduce(
    TErrorOr<TValue>& currentValue,
    TErrorOr<TValue> newValue,
    EErrorCode mismatchErrorCode)
{
    if (currentValue.IsOK() && newValue.IsOK()) {
        if (currentValue.Value() != newValue.Value()) {
            auto result = TError(mismatchErrorCode, "Mismatched messages");
            if constexpr (std::is_integral_v<TValue>) {
                result <<= TErrorAttribute("value_current", currentValue.Value());
                result <<= TErrorAttribute("value_new", newValue.Value());
            }
            currentValue = result;
        }
    } else if (currentValue.GetCode() == EErrorCode::Empty) {
        currentValue = std::move(newValue);
    } else if (newValue.GetCode() != EErrorCode::Empty) {
        ReduceErrors(currentValue, std::move(newValue), mismatchErrorCode);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

// Traits for Message* and const Message*.
template <typename TQualifiedMessage>
    requires std::same_as<NProtoBuf::Message, std::remove_const_t<TQualifiedMessage>>
struct TProtoVisitorTraits<TQualifiedMessage*>
{
    using TMessageParam = TQualifiedMessage*;
    using TMessageReturn = TQualifiedMessage*;

    static TErrorOr<const NProtoBuf::Descriptor*> GetDescriptor(TMessageParam message)
    {
        if (message == nullptr) {
            return TError(EErrorCode::Empty, "Received nullptr instead of message");
        }

        return message->GetDescriptor();
    }

    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(!fieldDescriptor->is_repeated());

        if (message == nullptr) {
            return false;
        }

        if (!fieldDescriptor->has_presence()) {
            return true;
        }

        return message->GetReflection()->HasField(*message, fieldDescriptor);
    }

    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(!fieldDescriptor->is_repeated());

        if (message == nullptr) {
            return nullptr;
        }

        if constexpr (std::is_const_v<TQualifiedMessage>) {
            return &message->GetReflection()->GetMessage(*message, fieldDescriptor);
        } else {
            return message->GetReflection()->MutableMessage(message, fieldDescriptor);
        }
    }

    static TErrorOr<int> GetRepeatedFieldSize(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(fieldDescriptor->is_repeated());

        if (message == nullptr) {
            return 0;
        }

        return message->GetReflection()->FieldSize(*message, fieldDescriptor);
    }

    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        YT_VERIFY(fieldDescriptor->is_repeated());

        if (message == nullptr) {
            return nullptr;
        }

        if constexpr (std::is_const_v<TQualifiedMessage>) {
            return &message->GetReflection()->GetRepeatedMessage(
                *message,
                fieldDescriptor,
                index);
        } else {
            return message->GetReflection()->MutableRepeatedMessage(
                message,
                fieldDescriptor,
                index);
        }
    }

    static TErrorOr<TMessageReturn> GetMessageFromMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const NProtoBuf::Message* keyMessage)
    {
        YT_VERIFY(fieldDescriptor->is_map());

        if (message == nullptr) {
            return nullptr;
        }

        auto errorOrIndex = LocateMapEntry(message, fieldDescriptor, keyMessage);
        if (!errorOrIndex.IsOK()) {
            return TError(errorOrIndex);
        }

        return GetMessageFromRepeatedField(message, fieldDescriptor, errorOrIndex.Value());
    }

    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(fieldDescriptor->is_map());

        int size = GetRepeatedFieldSize(message, fieldDescriptor).Value();
        TMapReturn result;
        result.reserve(size);

        // Null message means zero size, empty return.

        auto* keyFieldDescriptor = fieldDescriptor->message_type()->map_key();

        for (int index = 0; index < size; ++index) {
            auto* entry = GetMessageFromRepeatedField(message, fieldDescriptor, index);
            auto errorOrKey = MapKeyFieldToString(entry, keyFieldDescriptor);
            if (!errorOrKey.IsOK()) {
                return TError(errorOrKey);
            }
            auto [_, inserted] = result.try_emplace(
                std::move(errorOrKey).Value(),
                std::move(entry));
            if (!inserted) {
                // Strictly speaking, this is allowed by protobuf, but not by yson.
                return TError(EErrorCode::InvalidMap, "Map has equal keys");
            }
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

// Traits for std::pair<[const] Message*, [const] Message*>&.
template <typename TQualifiedMessage>
    requires std::same_as<NProtoBuf::Message, std::remove_const_t<TQualifiedMessage>>
struct TProtoVisitorTraits<const std::pair<TQualifiedMessage*, TQualifiedMessage*>&>
{
    using TMessageParam = const std::pair<TQualifiedMessage*, TQualifiedMessage*>&;
    using TMessageReturn = std::pair<TQualifiedMessage*, TQualifiedMessage*>;

    using TSubTraits = TProtoVisitorTraits<TQualifiedMessage*>;

    template <typename TValue>
    static std::pair<TValue, TValue> Combine(TValue first, TValue second)
    {
        return std::pair<TValue, TValue>{std::move(first), std::move(second)};
    }

    template <typename TValue>
    static TErrorOr<std::pair<TValue, TValue>> Combine(
        TErrorOr<TValue> first,
        TErrorOr<TValue> second,
        EErrorCode mismatchErrorCode)
    {
        if (!first.IsOK() || !second.IsOK()) {
            ReduceErrors(first, std::move(second), mismatchErrorCode);
            return TError(first);
        } else {
            return Combine(std::move(first).Value(), std::move(second).Value());
        }
    }

    static TErrorOr<const NProtoBuf::Descriptor*> GetDescriptor(TMessageParam message)
    {
        TErrorOr<const NProtoBuf::Descriptor*> result = TSubTraits::GetDescriptor(message.first);
        NDetail::Reduce(
            result,
            TSubTraits::GetDescriptor(message.second),
            EErrorCode::MismatchingDescriptors);
        return result;
    }

    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TErrorOr<bool> result =
            TSubTraits::IsSingularFieldPresent(message.first, fieldDescriptor);
        NDetail::Reduce(
            result,
            TSubTraits::IsSingularFieldPresent(message.second, fieldDescriptor),
            EErrorCode::MismatchingPresence);
        return result;
    }

    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        return Combine(
            TSubTraits::GetMessageFromSingularField(message.first, fieldDescriptor),
            TSubTraits::GetMessageFromSingularField(message.second, fieldDescriptor));
    }

    static TErrorOr<int> GetRepeatedFieldSize(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TErrorOr<int> result =
            TSubTraits::GetRepeatedFieldSize(message.first, fieldDescriptor);
        NDetail::Reduce(
            result,
            TSubTraits::GetRepeatedFieldSize(message.second, fieldDescriptor),
            EErrorCode::MismatchingSize);
        return result;
    }

    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        return Combine(
            TSubTraits::GetMessageFromRepeatedField(
                message.first,
                fieldDescriptor,
                index),
            TSubTraits::GetMessageFromRepeatedField(
                message.second,
                fieldDescriptor,
                index));
    }

    static TErrorOr<TMessageReturn> GetMessageFromMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const NProtoBuf::Message* keyMessage)
    {
        return Combine(
            TSubTraits::GetMessageFromMapFieldEntry(
                message.first,
                fieldDescriptor,
                keyMessage),
            TSubTraits::GetMessageFromMapFieldEntry(
                message.second,
                fieldDescriptor,
                keyMessage),
            EErrorCode::MismatchingKeys);
    }

    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        auto errorsOrSubResults1 =
            TSubTraits::GetMessagesFromWholeMapField(message.first, fieldDescriptor);
        auto errorsOrSubResults2 =
            TSubTraits::GetMessagesFromWholeMapField(message.second, fieldDescriptor);

        if (!errorsOrSubResults1.IsOK() || !errorsOrSubResults2.IsOK()) {
            ReduceErrors(
                errorsOrSubResults1,
                std::move(errorsOrSubResults2),
                EErrorCode::MismatchingKeys);
            return TError(errorsOrSubResults1);
        }

        auto& subResults1 = errorsOrSubResults1.Value();
        auto& subResults2 = errorsOrSubResults2.Value();

        if (subResults1.size() != subResults2.size()) {
            // The key sets are different. MismatchingSize is reserved for repeated fields.
            return TError(EErrorCode::MismatchingKeys,
                "Mismatching map sizes %v vs %v",
                subResults1.size(),
                subResults2.size());
        }

        TMapReturn result;
        result.reserve(subResults1.size());

        for (auto& [key, value1] : subResults1) {
            auto it2 = subResults2.find(key);
            if (it2 == subResults2.end()) {
                return TError(EErrorCode::MismatchingKeys,
                    "Mismathing keys in maps: no match for %v",
                    key);
            }

            result.emplace(key, Combine(value1, it2->second));
        }

        return result;
    }
};


namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// Traits for std::vector<[const] Message*>& and TCompactVector<[const] Message*, N>&.
template <typename TVectorType, typename TQualifiedMessage>
    requires std::same_as<NProtoBuf::Message, std::remove_const_t<TQualifiedMessage>>
struct TProtoVisitorTraitsForVector
{
    using TMessageParam = const TVectorType&;
    using TMessageReturn = TVectorType;

    using TSubTraits = TProtoVisitorTraits<TQualifiedMessage*>;

    static void Accumulate(TMessageReturn& result, TQualifiedMessage* message, size_t size)
    {
        result.reserve(size);
        result.push_back(message);
    }

    static void Accumulate(
        TErrorOr<TMessageReturn>& result,
        TErrorOr<TQualifiedMessage*> message,
        size_t size,
        EErrorCode mismatchErrorCode)
    {
        if (result.IsOK()) {
            if (message.IsOK()) {
                Accumulate(result, message, size);
                return;
            } else if (result.Value().empty()) {
                result = TError(message);
                return;
            }
        }

        ReduceErrors(result, message, mismatchErrorCode);
    }

    static TErrorOr<const NProtoBuf::Descriptor*> GetDescriptor(TMessageParam message)
    {
        TErrorOr<const NProtoBuf::Descriptor*> result(
            TError(EErrorCode::Empty, "Empty message param"));

        for (TQualifiedMessage* entry : message) {
            NDetail::Reduce(
                result,
                TSubTraits::GetDescriptor(entry),
                EErrorCode::MismatchingDescriptors);
        }

        return result;
    }

    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TErrorOr<bool> result(TError(EErrorCode::Empty, "Empty message param"));

        for (TQualifiedMessage* entry : message) {
            NDetail::Reduce(
                result,
                TSubTraits::IsSingularFieldPresent(entry, fieldDescriptor),
                EErrorCode::MismatchingPresence);
        }

        return result;
    }

    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TMessageReturn result;

        for (TQualifiedMessage* entry : message) {
            Accumulate(
                result,
                TSubTraits::GetMessageFromSingularField(entry, fieldDescriptor),
                message.size());
        }

        return result;
    }

    static TErrorOr<int> GetRepeatedFieldSize(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TErrorOr<int> result(
            TError(EErrorCode::Empty, "Empty message param"));

        for (TQualifiedMessage* entry : message) {
            NDetail::Reduce(
                result,
                TSubTraits::GetRepeatedFieldSize(entry, fieldDescriptor),
                EErrorCode::MismatchingSize);
        }

        return result;
    }

    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        TMessageReturn result;

        for (TQualifiedMessage* entry : message) {
            Accumulate(
                result,
                TSubTraits::GetMessageFromRepeatedField(entry, fieldDescriptor, index),
                message.size());
        }

        return result;
    }

    static TErrorOr<TMessageReturn> GetMessageFromMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const NProtoBuf::Message* keyMessage)
    {
        TErrorOr<TMessageReturn> result;

        for (TQualifiedMessage* entry : message) {
            Accumulate(
                result,
                TSubTraits::GetMessageFromMapFieldEntry(
                    entry,
                    fieldDescriptor,
                    keyMessage),
                message.size());
        }

        return result;
    }

    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        TErrorOr<TMapReturn> errorOrResult;

        for (TQualifiedMessage* entry : message) {
            auto errorOrSubResult =
                TSubTraits::GetMessagesFromWholeMapField(entry, fieldDescriptor);
            if (!errorOrResult.IsOK || !errorOrSubResult.IsOK()) {
                ReduceErrors(
                    errorOrResult,
                    std::move(errorOrSubResult),
                    EErrorCode::MismatchingKeys);
                continue;
            }

            auto& result = errorOrResult.Value();
            auto& subResult = errorOrSubResult.Value();

            if (entry == message.front()) {
                result.reserve(subResult.size());
                for (auto& [key, value] : subResult) {
                    Accumulate(result[key], value);
                }
            } else if (result.size() != subResult.size()) {
                errorOrResult = TError(
                    EErrorCode::MismatchingKeys,
                    "Mismatching map sizes %v vs %v",
                    result.size(),
                    subResult.size());
            } else {
                for (auto it1 : result.Value()) {
                    auto it2 = subResult.find(it1->first);
                    if (it2 == subResult.end()) {
                        return TError(EErrorCode::MismatchingKeys,
                            "Mismathing keys in maps: no match for %v",
                            it1.first);
                    }
                    Accumulate(it1.second, it2.second);
                }
            }
        }

        return errorOrResult;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename TQualifiedMessage>
struct TProtoVisitorTraits<const std::vector<TQualifiedMessage*>&>
    : public NDetail::TProtoVisitorTraitsForVector<
        std::vector<TQualifiedMessage*>, TQualifiedMessage>
{ };

template <typename TQualifiedMessage, size_t N>
struct TProtoVisitorTraits<const TCompactVector<TQualifiedMessage*, N>&>
    : public NDetail::TProtoVisitorTraitsForVector<
        TCompactVector<TQualifiedMessage*, N>, TQualifiedMessage>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
