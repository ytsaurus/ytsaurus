#ifndef SCALAR_ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include scalar_attribute.h"
// For the sake of sane code completion.
#include "scalar_attribute.h"
#endif

#include "helpers.h"
#include "proto_visitor.h"
#include "ytree.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

bool AreProtoMessagesEqual(
    const NProtoBuf::Message& lhs,
    const NProtoBuf::Message& rhs,
    const TComparisonOptions& options);

bool AreProtoMessagesEqualByPath(
    const NProtoBuf::Message& lhs,
    const NProtoBuf::Message& rhs,
    const NYPath::TYPath& path,
    const TComparisonOptions& options);

// List all supported types explicitly for safety.
template <class T>
concept CScalarAttributeTriviallyComparable =
    std::equality_comparable<T> && (
        std::same_as<T, TString> ||
        std::same_as<T, TStringBuf> ||
        std::same_as<T, TGuid> ||
        std::same_as<T, TInstant> ||
        std::same_as<T, double> ||
        std::integral<T> ||
        TEnumTraits<T>::IsEnum ||
        std::is_enum_v<T>);

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool AreScalarAttributesEqualAsYTrees(const T& lhs, const T& rhs, const NYPath::TYPath& path)
{
    auto lhsNode = GetNodeByPathOrEntity(NYTree::ConvertToNode(lhs), path);
    auto rhsNode = GetNodeByPathOrEntity(NYTree::ConvertToNode(rhs), path);
    return NYTree::AreNodesEqual(lhsNode, rhsNode);
}

////////////////////////////////////////////////////////////////////////////////

template <NDetail::CScalarAttributeTriviallyComparable T>
bool AreScalarAttributesEqualImpl(
    const T& lhs,
    const T& rhs,
    const TComparisonOptions& /*options*/)
{
    return lhs == rhs;
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const TIntrusivePtr<T>& lhs,
    const TIntrusivePtr<T>& rhs,
    const TComparisonOptions& /*options*/)
    requires std::convertible_to<T*, NYTree::INode*>
{
    return NYTree::AreNodesEqual(lhs, rhs);
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const T& lhs,
    const T& rhs,
    const TComparisonOptions& options)
    requires std::convertible_to<T*, NProtoBuf::Message*>
{
    return AreProtoMessagesEqual(lhs, rhs, options);
}

template <class TArray>
bool AreScalarAttributeArraysEqual(
    const TArray& lhs,
    const TArray& rhs,
    const TComparisonOptions& options)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (decltype(lhs.size()) i = 0; i < lhs.size(); ++i) {
        if (!AreScalarAttributesEqualImpl(lhs[i], rhs[i], options)) {
            return false;
        }
    }
    return true;
}

template <class TMapping>
bool AreScalarAttributeMappingsEqual(
    const TMapping& lhs,
    const TMapping& rhs,
    const TComparisonOptions& options)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (const auto& [key, value] : lhs) {
        auto it = rhs.find(key);
        if (it == rhs.end()) {
            return false;
        }
        if (!AreScalarAttributesEqualImpl(value, it->second, options)) {
            return false;
        }
    }
    return true;
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const std::vector<T>& lhs,
    const std::vector<T>& rhs,
    const TComparisonOptions& options)
{
    return AreScalarAttributeArraysEqual(lhs, rhs, options);
}

template <class TValue>
bool AreScalarAttributesEqualImpl(
    const NProtoBuf::RepeatedField<TValue>& lhs,
    const NProtoBuf::RepeatedField<TValue>& rhs,
    const TComparisonOptions& options)
{
    return AreScalarAttributeArraysEqual(lhs, rhs, options);
}

template <class TValue>
bool AreScalarAttributesEqualImpl(
    const NProtoBuf::RepeatedPtrField<TValue>& lhs,
    const NProtoBuf::RepeatedPtrField<TValue>& rhs,
    const TComparisonOptions& options)
{
    return AreScalarAttributeArraysEqual(lhs, rhs, options);
}

template <NDetail::CScalarAttributeTriviallyComparable TKey, class TValue>
bool AreScalarAttributesEqualImpl(
    const THashMap<TKey, TValue>& lhs,
    const THashMap<TKey, TValue>& rhs,
    const TComparisonOptions& options)
{
    return AreScalarAttributeMappingsEqual(lhs, rhs, options);
}

template <NDetail::CScalarAttributeTriviallyComparable TKey, class TValue>
bool AreScalarAttributesEqualImpl(
    const NProtoBuf::Map<TKey, TValue>& lhs,
    const NProtoBuf::Map<TKey, TValue>& rhs,
    const TComparisonOptions& options)
{
    return AreScalarAttributeMappingsEqual(lhs, rhs, options);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool AreScalarAttributesEqual(
    const T& lhs,
    const T& rhs,
    const TComparisonOptions& options)
{
    return NDetail::AreScalarAttributesEqualImpl(lhs, rhs, options);
}

template <class T>
bool AreScalarAttributesEqualByPath(
    const T& lhs,
    const T& rhs,
    const NYPath::TYPath& path,
    const TComparisonOptions& options)
{
    if (path.empty()) {
        return AreScalarAttributesEqual(lhs, rhs, options);
    } else {
        if constexpr (std::convertible_to<T*, NProtoBuf::Message*>) {
            return NDetail::AreProtoMessagesEqualByPath(lhs, rhs, path, options);
        } else {
            return NDetail::AreScalarAttributesEqualAsYTrees(lhs, rhs, path);
        }
    }
}

template <class T>
bool AreScalarAttributesEqualByPath(
    const std::vector<T>& lhs,
    const std::vector<T>& rhs,
    const NYPath::TYPath& path,
    const TComparisonOptions& options)
{
    if (path.empty()) {
        return AreScalarAttributesEqual(lhs, rhs, options);
    }

    if (!path.StartsWith("/*")) {
        return AreScalarAttributesEqualByPath<std::vector<T>>(lhs, rhs, path, options);
    }

    if (lhs.size() != rhs.size()) {
        return false;
    }

    auto suffix = path.substr(2);
    for (int i = 0; i < ssize(lhs); ++i) {
        if (!AreScalarAttributesEqualByPath(lhs[i], rhs[i], suffix, options)) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TClearVisitor final
    : public TProtoVisitor<NProtoBuf::Message*, TClearVisitor>
{
    friend class TPathVisitor<TClearVisitor>;
    friend class TProtoVisitor<NProtoBuf::Message*, TClearVisitor>;

protected:
    template <typename TVisitParam>
    void VisitWholeVector(TVisitParam&& target, EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            target.clear();
            return;
        }

        TProtoVisitor::VisitWholeVector(std::forward<TVisitParam>(target), reason);
    }

    template <typename TVisitParam>
    void VisitVectorEntry(TVisitParam&& target, int index, EVisitReason reason)
    {
        if (PathComplete()) {
            target.erase(target.begin() + index);
            return;
        }

        TProtoVisitor::VisitVectorEntry(std::forward<TVisitParam>(target), index, reason);
    }

    template <typename TVisitParam>
    void VisitWholeMap(TVisitParam&& target, EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            target.clear();
            return;
        }

        TProtoVisitor::VisitWholeMap(std::forward<TVisitParam>(target), reason);
    }

    template <typename TVisitParam, typename TMapIterator>
    void VisitMapEntry(
        TVisitParam&& target,
        TMapIterator mapIterator,
        TString key,
        EVisitReason reason)
    {
        if (PathComplete()) {
            target.erase(mapIterator);
            return;
        }

        TProtoVisitor::VisitMapEntry(
            std::forward<TVisitParam>(target),
            std::move(mapIterator),
            std::move(key),
            reason);
    }


    template<typename TVisitParam>
    void VisitNode(
        TVisitParam&& target,
        EVisitReason reason)
    {
        Y_UNUSED(reason);

        if (RemoveNodeByYPath(target, NYPath::TYPath{GetTokenizerInput()})) {
            return;
        }

        THROW_ERROR_EXCEPTION_IF(GetMissingFieldPolicy() == EMissingFieldPolicy::Throw,
            NAttributes::EErrorCode::MalformedPath,
            "Path not found in yson tree");
    }

    void VisitWholeMessage(
        NProtoBuf::Message* message,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // Asterisk means clear all fields but keep the message present.
            message->Clear();
            return;
        }

        TProtoVisitor::VisitWholeMessage(message, reason);
    }

    void VisitWholeMapField(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            VisitField(message, fieldDescriptor, EVisitReason::Manual);
            return;
        }

        TProtoVisitor::VisitWholeMapField(message, fieldDescriptor, reason);
    }

    void VisitWholeRepeatedField(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            VisitField(message, fieldDescriptor, EVisitReason::Manual);
            return;
        }

        TProtoVisitor::VisitWholeRepeatedField(message, fieldDescriptor, reason);
    }

    void VisitUnrecognizedField(
        NProtoBuf::Message* message,
        const NProtoBuf::Descriptor* descriptor,
        TString name,
        EVisitReason reason)
    {
        auto* unknownFields = message->GetReflection()->MutableUnknownFields(message);
        auto unknownFieldNumber = message->GetDescriptor()->options().GetExtension(
            NYson::NProto::unknown_yson_field_number);

        auto errorOrItem = LookupUnknownYsonFieldsItem(unknownFields, name, unknownFieldNumber);

        if (errorOrItem.IsOK()) {
            auto [index, value] = std::move(errorOrItem).Value();
            if (PathComplete()) {
                unknownFields->DeleteSubrange(index, 1);
                return;
            }

            auto root = value
                ? NYTree::ConvertToNode(value)
                : NYTree::GetEphemeralNodeFactory()->CreateMap();
            if (RemoveNodeByYPath(root, NYPath::TYPath{GetTokenizerInput()})) {
                value = NYson::ConvertToYsonString(root);
                auto* item = unknownFields->mutable_field(index)->mutable_length_delimited();
                *item = SerializeUnknownYsonFieldsItem(name, value.AsStringBuf());
                return;
            }
        }

        TProtoVisitor::VisitUnrecognizedField(message, descriptor, std::move(name), reason);
    }

    void VisitField(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            auto* reflection = message->GetReflection();
            if (!fieldDescriptor->has_presence() ||
                reflection->HasField(*message, fieldDescriptor))
            {
                reflection->ClearField(message, fieldDescriptor);
                return;
            } // Else let the basic implementation of MissingFieldPolicy do the check.
        }

        TProtoVisitor::VisitField(message, fieldDescriptor, reason);
    }

    void VisitMapFieldEntry(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        NProtoBuf::Message* entryMessage,
        TString key,
        EVisitReason reason)
    {
        if (PathComplete()) {
            int index = LocateMapEntry(message, fieldDescriptor, entryMessage).Value();
            DeleteRepeatedFieldEntry(message, fieldDescriptor, index);
            return;
        }

        TProtoVisitor::VisitMapFieldEntry(
            message,
            fieldDescriptor,
            entryMessage,
            std::move(key),
            reason);
    }

    void VisitRepeatedFieldEntry(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete()) {
            DeleteRepeatedFieldEntry(message, fieldDescriptor, index);
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }

    void DeleteRepeatedFieldEntry(
        NProtoBuf::Message* message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index)
    {
        auto* reflection = message->GetReflection();
        int size = reflection->FieldSize(*message, fieldDescriptor);
        for (++index; index < size; ++index) {
            reflection->SwapElements(message, fieldDescriptor, index - 1, index);
        }
        reflection->RemoveLast(message, fieldDescriptor);
    }
}; // TClearVisitor

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ClearFieldByPath(T&& from, NYPath::TYPathBuf path, bool skipMissing)
{
    TClearVisitor visitor;
    visitor.SetAllowAsterisk(true);
    if (skipMissing) {
        visitor.SetMissingFieldPolicy(EMissingFieldPolicy::Skip);
    }
    visitor.SetRelativeIndexPolicy(ERelativeIndexPolicy::Throw);
    visitor.Visit(std::forward<T>(from), path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
