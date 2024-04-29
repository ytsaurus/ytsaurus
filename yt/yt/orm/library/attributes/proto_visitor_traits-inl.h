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
    const TErrorOr<TValue>& newValue,
    EErrorCode errorCode)
{
    if (currentValue.IsOK()) {
        if (newValue.IsOK()) {
            if (currentValue.Value() == newValue.Value) {
                return;
            } else {
                currentValue = TError(errorCode, "Mismatched messages")
                    << TErrorAttribute("first", currentValue.Value())
                    << TErrorAttribute("other", newValue.Value());
            }
        } else {
            currentValue = TError(errorCode, "Some messages had errors")
                << newValue;
        }
    } else {
        if (currentValue.GetCode() == EErrorCode::Empty) {
            currentValue = newValue;
        } else if (!newValue.IsOK()) {
            currentValue <<= newValue;
        }
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
            return TError(EErrorCode::MissingMessage, "Received nullptr instead of message");
        } else {
            return message->GetDescriptor();
        }
    }

    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(!fieldDescriptor->is_repeated());
        YT_VERIFY(fieldDescriptor->has_presence());
        return message->GetReflection()->HasField(*message, fieldDescriptor);
    }

    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        YT_VERIFY(!fieldDescriptor->is_repeated());
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
        return message->GetReflection()->FieldSize(*message, fieldDescriptor);
    }

    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        YT_VERIFY(fieldDescriptor->is_repeated());
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
        TErrorOr<TValue> second)
    {
        if (!first.IsOK() || !second.IsOK()) {
            return CombineErrors(std::move(first), std::move(second));
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
                keyMessage));
    }

    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor)
    {
        auto errorsOrSubResults = Combine(
            TSubTraits::GetMessagesFromWholeMapField(message.first, fieldDescriptor),
            TSubTraits::GetMessagesFromWholeMapField(message.second, fieldDescriptor));

        if (!errorsOrSubResults.IsOK()) {
            return errorsOrSubResults;
        }

        const auto& subResults1 = errorsOrSubResults.Value().first;
        const auto& subResults2 = errorsOrSubResults.Value().second;

        if (subResults1.size() != subResults2.size()) {
            return TError(EErrorCode::MismatchingSize,
                "Mismatching map sizes %v vs %v",
                subResults1.size(),
                subResults2.size());
        }

        TMapReturn result;
        result.reserve(subResults1.size());

        for (auto it1 : subResults1) {
            auto it2 = subResults2.find(it1->first);
            if (it2 == subResults2.end()) {
                return TError(EErrorCode::MismatchingKeys,
                    "Mismathing keys in maps: no match for %v",
                    it1.first);
            }

            result.emplace(it1.first, Combine(it1.second, it2.second));
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
        size_t size)
    {
        if (!result.IsOK() || !message.IsOK()) {
            result = message;
        } else {
            Accumulate(result, message, size);
        }
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
        TErrorOr<bool> result(
            TError(EErrorCode::Empty, "Empty message param"));

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
        TErrorOr<TMessageReturn> result(
            TError(EErrorCode::Empty, "Empty message param"));

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
        TErrorOr<TMapReturn> result(
            TError(EErrorCode::Empty, "Empty message param"));

        for (TQualifiedMessage* entry : message) {
            auto errorOrSubResult =
                TSubTraits::GetMessagesFromWholeMapField(entry, fieldDescriptor);
            if (!errorOrSubResult.IsOK()) {
                return errorOrSubResult;
            }
            const auto& subResult = errorOrSubResult.Value();

            if (result.GetCode() == EErrorCode::Empty) {
                result = TMapReturn();
                result.Value().reserve(subResult.size());
                for (const auto& [key, value] : subResult) {
                    TMessageReturn& entry = result.Value()[key];
                    entry.reserve(message.size());
                    entry.push_back(value);
                }
            } else if (result.Value().size() != subResult.size()) {
                return TError(EErrorCode::MismatchingSize,
                    "Mismatching map sizes %v vs %v",
                    result.Value().size(),
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

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename TQualifiedMessage>
struct TProtoVisitorTraits<const std::vector<TQualifiedMessage*>>
    : public NDetail::TProtoVisitorTraitsForVector<
        std::vector<TQualifiedMessage*>, TQualifiedMessage>
{ };

template <typename TQualifiedMessage, size_t N>
struct TProtoVisitorTraits<const TCompactVector<TQualifiedMessage*, N>>
    : public NDetail::TProtoVisitorTraitsForVector<
        TCompactVector<TQualifiedMessage*, N>, TQualifiedMessage>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
