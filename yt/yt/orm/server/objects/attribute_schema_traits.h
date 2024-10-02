#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/yt/core/misc/mpl.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
inline void UpdateScalarAttributeValue(
    const TAttributeSchema* schema,
    const NYPath::TYPath& path,
    const NYTree::INodePtr& newValue,
    TTypedValue& mutableValue,
    bool recursive,
    bool forceSetViaYson);

template <class TTypedValue>
inline NTableClient::EValueType ValidateAndExtractTypeFromScalarAttribute(
    const TAttributeSchema* attribute,
    NYPath::TYPathBuf path);

////////////////////////////////////////////////////////////////////////////////

void ValidatePathIsEmpty(const TAttributeSchema* attribute, const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires std::convertible_to<T*, ::google::protobuf::MessageLite*>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T* = nullptr);

template <class T>
    requires NMpl::IsSpecialization<T, std::vector>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T* = nullptr);

template <class T>
    requires NMpl::IsSpecialization<T, THashMap>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema* schema, const NYTree::INodePtr& node, const T* = nullptr);

template <class T>
T ParseScalarAttributeFromYsonNode(const TAttributeSchema*, const NYTree::INodePtr& node, const T* = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define ATTRIBUTE_SCHEMA_TRAITS_H_
#include "attribute_schema_traits-inl.h"
#undef ATTRIBUTE_SCHEMA_TRAITS_H_
