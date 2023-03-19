#ifndef SCALAR_ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include scalar_attribute.h"
// For the sake of sane code completion.
#include "scalar_attribute.h"
#endif

#include "ytree.h"

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

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
        std::integral<T> ||
        TEnumTraits<T>::IsEnum);

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
bool AreScalarAttributesEqualImpl(const T& lhs, const T& rhs)
{
    return lhs == rhs;
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const TIntrusivePtr<T>& lhs,
    const TIntrusivePtr<T>& rhs)
    requires std::convertible_to<T*, NYTree::INode*>
{
    return NYTree::AreNodesEqual(lhs, rhs);
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const T& lhs,
    const T& rhs)
    requires std::convertible_to<T*, google::protobuf::Message*>
{
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

template <class TArray>
bool AreScalarAttributeArraysEqual(
    const TArray& lhs,
    const TArray& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (decltype(lhs.size()) i = 0; i < lhs.size(); ++i) {
        if (!AreScalarAttributesEqualImpl(lhs[i], rhs[i])) {
            return false;
        }
    }
    return true;
}

template <class TValue>
bool AreScalarAttributesEqualImpl(
    const google::protobuf::RepeatedPtrField<TValue>& lhs,
    const google::protobuf::RepeatedPtrField<TValue>& rhs)
{
    return AreScalarAttributeArraysEqual(lhs, rhs);
}

template <class T>
bool AreScalarAttributesEqualImpl(
    const std::vector<T>& lhs,
    const std::vector<T>& rhs)
{
    return AreScalarAttributeArraysEqual(lhs, rhs);
}

template <NDetail::CScalarAttributeTriviallyComparable TKey, class TValue>
bool AreScalarAttributesEqualImpl(
    const THashMap<TKey, TValue>& lhs,
    const THashMap<TKey, TValue>& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (const auto& [k, v] : lhs) {
        auto it = rhs.find(k);
        if (it == rhs.end()) {
            return false;
        }
        if (!AreScalarAttributesEqualImpl(v, it->second)) {
            return false;
        }
    }
    return true;
}

} // namespace NDetail

/////////////////////////////////////////////////////////////////////////////////

template <class T>
bool AreScalarAttributesEqual(
    const T& lhs,
    const T& rhs)
{
    return NDetail::AreScalarAttributesEqualImpl(lhs, rhs);
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

/////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
