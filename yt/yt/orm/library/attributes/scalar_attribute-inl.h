#ifndef SCALAR_ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include scalar_attribute.h"
// For the sake of sane code completion.
#include "scalar_attribute.h"
#endif

#include "ytree.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

bool AreProtoMessagesEqual(
    const google::protobuf::Message& lhs,
    const google::protobuf::Message& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer);

bool AreProtoMessagesEqualByPath(
    const google::protobuf::Message& lhs,
    const google::protobuf::Message& rhs,
    const NYPath::TYPath& path);

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
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    Y_UNUSED(messageDifferencer);
    return lhs == rhs;
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const TIntrusivePtr<T>& lhs,
    const TIntrusivePtr<T>& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
    requires std::convertible_to<T*, NYTree::INode*>
{
    Y_UNUSED(messageDifferencer);
    return NYTree::AreNodesEqual(lhs, rhs);
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const T& lhs,
    const T& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
    requires std::convertible_to<T*, google::protobuf::Message*>
{
    return AreProtoMessagesEqual(lhs, rhs, messageDifferencer);
}

template <class TArray>
bool AreScalarAttributeArraysEqual(
    const TArray& lhs,
    const TArray& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (decltype(lhs.size()) i = 0; i < lhs.size(); ++i) {
        if (!AreScalarAttributesEqualImpl(lhs[i], rhs[i], messageDifferencer)) {
            return false;
        }
    }
    return true;
}

template <class TMapping>
bool AreScalarAttributeMappingsEqual(
    const TMapping& lhs,
    const TMapping& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (const auto& [key, value] : lhs) {
        auto it = rhs.find(key);
        if (it == rhs.end()) {
            return false;
        }
        if (!AreScalarAttributesEqualImpl(value, it->second, messageDifferencer)) {
            return false;
        }
    }
    return true;
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const std::vector<T>& lhs,
    const std::vector<T>& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    return AreScalarAttributeArraysEqual(lhs, rhs, messageDifferencer);
}

template <class TValue>
bool AreScalarAttributesEqualImpl(
    const google::protobuf::RepeatedPtrField<TValue>& lhs,
    const google::protobuf::RepeatedPtrField<TValue>& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    return AreScalarAttributeArraysEqual(lhs, rhs, messageDifferencer);
}

template <NDetail::CScalarAttributeTriviallyComparable TKey, class TValue>
bool AreScalarAttributesEqualImpl(
    const THashMap<TKey, TValue>& lhs,
    const THashMap<TKey, TValue>& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    return AreScalarAttributeMappingsEqual(lhs, rhs, messageDifferencer);
}

template <NDetail::CScalarAttributeTriviallyComparable TKey, class TValue>
bool AreScalarAttributesEqualImpl(
    const ::google::protobuf::Map<TKey, TValue>& lhs,
    const ::google::protobuf::Map<TKey, TValue>& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    return AreScalarAttributeMappingsEqual(lhs, rhs, messageDifferencer);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool AreScalarAttributesEqual(
    const T& lhs,
    const T& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer)
{
    return NDetail::AreScalarAttributesEqualImpl(lhs, rhs, messageDifferencer);
}

template <class T>
bool AreScalarAttributesEqualByPath(
    const T& lhs,
    const T& rhs,
    const NYPath::TYPath& path)
{
    if (path.empty()) {
        return AreScalarAttributesEqual(lhs, rhs);
    } else {
        if constexpr (std::convertible_to<T*, google::protobuf::Message*>) {
            return NDetail::AreProtoMessagesEqualByPath(lhs, rhs, path);
        } else {
            return NDetail::AreScalarAttributesEqualAsYTrees(lhs, rhs, path);
        }
    }
}

template <class T>
bool AreScalarAttributesEqualByPath(
    const std::vector<T>& lhs,
    const std::vector<T>& rhs,
    const NYPath::TYPath& path)
{
    if (path.empty()) {
        return AreScalarAttributesEqual(lhs, rhs);
    }

    if (!path.StartsWith("/*")) {
        return AreScalarAttributesEqualByPath<std::vector<T>>(lhs, rhs, path);
    }

    if (lhs.size() != rhs.size()) {
        return false;
    }

    auto suffix = path.substr(2);
    for (int i = 0; i < ssize(lhs); ++i) {
        if (!AreScalarAttributesEqualByPath(lhs[i], rhs[i], suffix)) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
