#pragma once

#include "public.h"

#include "wire_string.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/protobuf_interop_options.h>
#include <yt/yt/core/ytree/public.h>

namespace google::protobuf {
namespace util {

////////////////////////////////////////////////////////////////////////////////

class MessageDifferencer;

////////////////////////////////////////////////////////////////////////////////

} // namespace util

class Message;

////////////////////////////////////////////////////////////////////////////////

} // namespace google::protobuf

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

//! Clears the field that the `path` points to in the `message`.
//! Throws an error if path is invalid, or specified field is not set unless `skipMissing` is true.
void ClearProtobufFieldByPath(
    google::protobuf::Message& message,
    const NYPath::TYPath& path,
    bool skipMissing = false);

template <class T>
void ClearFieldByPath(T&& from, NYPath::TYPathBuf path);

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
    bool recursive = false);

template <class T>
bool AreScalarAttributesEqual(
    const T& lhs,
    const T& rhs,
    ::google::protobuf::util::MessageDifferencer* messageDifferencer = nullptr);

template <class T>
bool AreScalarAttributesEqualByPath(
    const T& lhs,
    const T& rhs,
    const NYPath::TYPath& path);

template <>
bool AreScalarAttributesEqualByPath(
    const NYson::TYsonString& lhs,
    const NYson::TYsonString& rhs,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#define SCALAR_ATTRIBUTE_INL_H_
#include "scalar_attribute-inl.h"
#undef SCALAR_ATTRIBUTE_INL_H_
