#pragma once

#include "public.h"

#include "wire_string.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/protobuf_interop_options.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

//! Checks that field with name `fieldName` is present in `message`.
// TODO(grigminakov): Upgrade to `HasProtobufFieldByPath` or unify with `ResolveProtobufElementByYPath`.
bool HasProtobufField(
    const google::protobuf::Message& message,
    const std::string& fieldName);

////////////////////////////////////////////////////////////////////////////////

//! Clears the field that the `path` points to in the `message`.
//! Throws an error if path is invalid, or specified field is not set unless `skipMissing` is true.
void ClearProtobufFieldByPath(
    google::protobuf::Message& message,
    const NYPath::TYPath& path,
    bool skipMissing = false);

template <class T>
void ClearFieldByPath(T&& from, NYPath::TYPathBuf path, bool skipMissing = false);

////////////////////////////////////////////////////////////////////////////////

//! Sets the field that the `path` points to in the `message`.
//! Throws an error if path is invalid, or there is a missing key along the path and `recursive` is false.
void SetProtobufFieldByPath(
    NProtoBuf::Message& message,
    const NYPath::TYPath& path,
    const NYTree::INodePtr& value,
    const NYson::TProtobufWriterOptions& options = {},
    bool recursive = false);
void SetProtobufFieldByPath(
    NProtoBuf::Message& message,
    const NYPath::TYPath& path,
    const TWireString& value,
    bool discardUnknownFields = false,
    bool recursive = false);

////////////////////////////////////////////////////////////////////////////////

struct TComparisonOptions
{
    NProtoBuf::MessageDifferencer* MessageDifferencer = nullptr;
    bool CompareAbsentAsDefault = false;
};

template <class T>
bool AreScalarAttributesEqual(
    const T& lhs,
    const T& rhs,
    const TComparisonOptions& options = {});

template <class T>
bool AreScalarAttributesEqualByPath(
    const T& lhs,
    const T& rhs,
    const NYPath::TYPath& path,
    const TComparisonOptions& options = {});

template <>
bool AreScalarAttributesEqualByPath(
    const NYson::TYsonString& lhs,
    const NYson::TYsonString& rhs,
    const NYPath::TYPath& path,
    const TComparisonOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#define SCALAR_ATTRIBUTE_INL_H_
#include "scalar_attribute-inl.h"
#undef SCALAR_ATTRIBUTE_INL_H_
