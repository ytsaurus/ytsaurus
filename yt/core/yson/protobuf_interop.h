#pragma once

#include "public.h"

#include <yt/core/ypath/public.h>

#include <variant>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! An opaque reflected counterpart of ::google::protobuf::Descriptor.
/*!
 *  Reflecting a descriptor takes the following options into account:
 *  NYT.NProto.NYson.field_name:      overrides the default name of field
 *  NYT.NProto.NYson.enum_value_name: overrides the default name of enum value
 */
class TProtobufMessageType;

//! An opaque reflected counterpart of ::google::protobuf::EnumDescriptor.
class TProtobufEnumType;

//! Reflects ::google::protobuf::Descriptor.
/*!
 *  The call caches its result in a static variable and is thus efficient.
 */
template <class T>
const TProtobufMessageType* ReflectProtobufMessageType();

//! Reflects ::google::protobuf::Descriptor.
/*!
 *  The call invokes the internal reflection registry and takes spinlocks.
 *  Should not be assumed to be efficient.
 */
const TProtobufMessageType* ReflectProtobufMessageType(const ::google::protobuf::Descriptor* descriptor);

//! Reflects ::google::protobuf::EnumDescriptor.
/*!
 *  The call invokes the internal reflection registry and takes spinlocks.
 *  Should not be assumed to be efficient.
 */
const TProtobufEnumType* ReflectProtobufEnumType(const ::google::protobuf::EnumDescriptor* descriptor);

//! Extracts the underlying ::google::protobuf::Descriptor from a reflected instance.
const ::google::protobuf::Descriptor* UnreflectProtobufMessageType(const TProtobufMessageType* type);

//! Extracts the underlying ::google::protobuf::EnumDescriptor from a reflected instance.
const ::google::protobuf::EnumDescriptor* UnreflectProtobufMessageType(const TProtobufEnumType* type);

////////////////////////////////////////////////////////////////////////////////

struct TProtobufMessageElement;
struct TProtobufScalarElement;
struct TProtobufAttributeDictionaryElement;
struct TProtobufRepeatedElement;
struct TProtobufMapElement;
struct TProtobufAnyElement;

using TProtobufElement = std::variant<
    std::unique_ptr<TProtobufMessageElement>,
    std::unique_ptr<TProtobufScalarElement>,
    std::unique_ptr<TProtobufAttributeDictionaryElement>,
    std::unique_ptr<TProtobufRepeatedElement>,
    std::unique_ptr<TProtobufMapElement>,
    std::unique_ptr<TProtobufAnyElement>
>;

struct TProtobufMessageElement
{
    const TProtobufMessageType* Type;
};

struct TProtobufScalarElement
{
};

struct TProtobufAttributeDictionaryElement
{
};

struct TProtobufRepeatedElement
{
    TProtobufElement Element;
};

struct TProtobufMapElement
{
    TProtobufElement Element;
};

struct TProtobufAnyElement
{
};

struct TProtobufElementResolveResult
{
    TProtobufElement Element;
    TStringBuf HeadPath;
    TStringBuf TailPath;
};

struct TResolveProtobufElementByYPathOptions
{
    bool AllowUnknownYsonFields = false;
};

//! Introspects a given #rootType and locates an element (represented
//! by TProtobufElement discriminated union) at a given #path.
//! Throws if some definite error occurs during resolve (i.e. a malformed
//! YPath or a reference to a non-existing field).
TProtobufElementResolveResult ResolveProtobufElementByYPath(
    const TProtobufMessageType* rootType,
    const NYPath::TYPath& path,
    const TResolveProtobufElementByYPathOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

constexpr int UnknownYsonFieldNumber = 3005;

DEFINE_ENUM(EUnknownYsonFieldsMode,
    (Skip)
    (Fail)
    (Keep)
);

struct TProtobufWriterOptions
{
    //! Keep: all unknown fields found during YSON parsing
    //! are translated into Protobuf unknown fields (each has number UnknownYsonFieldNumber
    //! and is a key-value pair with field name being its key and YSON being the value).
    //!
    //! Skip: all unknown fields are silently skipped;
    //!
    //! Fail: an exception is thrown whenever an unknown field is found.
    EUnknownYsonFieldsMode UnknownYsonFieldsMode = EUnknownYsonFieldsMode::Fail;

    //! If |true| then required fields not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipRequiredFields = false;
};

//! Creates a YSON consumer that converts IYsonConsumer calls into
//! a byte sequence in protobuf wire format.
/*!
 *  The resulting sequence of bytes is actually fed into the output stream
 *  only at the very end since constructing it involves an additional pass
 *  to compute lengths of nested submessages.
 */
std::unique_ptr<IYsonConsumer> CreateProtobufWriter(
    ::google::protobuf::io::ZeroCopyOutputStream* outputStream,
    const TProtobufMessageType* rootType,
    const TProtobufWriterOptions& options = TProtobufWriterOptions());

////////////////////////////////////////////////////////////////////////////////

struct TProtobufParserOptions
{
    //! If |true| then fields with numbers not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipUnknownFields = false;

    //! If |true| then required fields not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipRequiredFields = false;
};

//! Parses a byte sequence and translates it into IYsonConsumer calls.
/*!
 *  IMPORTANT! Due to performance reasons the implementation currently assumes
 *  that the byte sequence obeys the following additional condition (not enfored
 *  by protobuf wire format as it is): for each repeated field, its occurrences
 *  are sequential. This property is always true for byte sequences produced
 *  from message classes.
 *
 *  In case you need to handle generic protobuf sequences, you should extend the
 *  code appropriately and provide a fallback flag (since zero-overhead support
 *  does not seem possible).
 */
void ParseProtobuf(
    IYsonConsumer* consumer,
    ::google::protobuf::io::ZeroCopyInputStream* inputStream,
    const TProtobufMessageType* rootType,
    const TProtobufParserOptions& options = TProtobufParserOptions());

//! Invokes #ParseProtobuf to write #message into #consumer.
void WriteProtobufMessage(
    IYsonConsumer* consumer,
    const ::google::protobuf::Message& message,
    const TProtobufParserOptions& options = TProtobufParserOptions());


//! Given a enum type T, tries to convert a string literal to T.
//! Returns null if the literal is not known.
template <class T>
std::optional<T> FindProtobufEnumValueByLiteral(
    const TProtobufEnumType* type,
    TStringBuf literal);

//! Given a enum type T, tries to convert a value of T to string literals.
//! Returns null if no literal is known for this value.
template <class T>
TStringBuf FindProtobufEnumLiteralByValue(
    const TProtobufEnumType* type,
    T value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PROTOBUF_INTEROP_INL_H_
#include "protobuf_interop-inl.h"
#undef PROTOBUF_INTEROP_INL_H_
