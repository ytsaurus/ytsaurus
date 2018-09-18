#pragma once

#include "public.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

//! An opaque reflected counterpart of ::google::protobuf::Descriptor.
/*!
 *  Reflecting a descriptor takes the following options into account:
 *  NYT.NProto.NYson.field_name:      overrides the default name of field
 *  NYT.NProto.NYson.enum_value_name: overrides the default name of enum value
 */
class TProtobufMessageType;

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

////////////////////////////////////////////////////////////////////////////////

struct TProtobufWriterOptions
{
    //! If |true| then fields with name not found in protobuf metadata are
    //! silently skipped; otherwise an exception is thrown.
    bool SkipUnknownFields = false;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

#define PROTOBUF_INTEROP_INL_H_
#include "protobuf_interop-inl.h"
#undef PROTOBUF_INTEROP_INL_H_
