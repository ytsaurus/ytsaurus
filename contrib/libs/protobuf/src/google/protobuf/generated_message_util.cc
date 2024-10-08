// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#include "google/protobuf/generated_message_util.h"

#include <atomic>
#include <limits>
#include <vector>

#include "google/protobuf/arenastring.h"
#include "google/protobuf/extension_set.h"
#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "google/protobuf/message_lite.h"
#include "google/protobuf/metadata_lite.h"
#include "google/protobuf/repeated_field.h"
#include "google/protobuf/wire_format_lite.h"

// Must be included last
#include "google/protobuf/port_def.inc"

PROTOBUF_PRAGMA_INIT_SEG


namespace google {
namespace protobuf {
namespace internal {

void DestroyMessage(const void* message) {
  static_cast<const MessageLite*>(message)->~MessageLite();
}
void DestroyString(const void* s) {
  static_cast<const TProtoStringType*>(s)->~TBasicString();
}

PROTOBUF_ATTRIBUTE_NO_DESTROY PROTOBUF_CONSTINIT
    PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 ExplicitlyConstructedArenaString
        fixed_address_empty_string{};  // NOLINT


PROTOBUF_CONSTINIT std::atomic<bool> init_protobuf_defaults_state{false};
static bool InitProtobufDefaultsImpl() {
  fixed_address_empty_string.DefaultConstruct();
  OnShutdownDestroyString(fixed_address_empty_string.get_mutable());


  init_protobuf_defaults_state.store(true, std::memory_order_release);
  return true;
}

void InitProtobufDefaultsSlow() {
  static bool is_inited = InitProtobufDefaultsImpl();
  (void)is_inited;
}
// Force the initialization of the empty string.
// Normally, registration would do it, but we don't have any guarantee that
// there is any object with reflection.
PROTOBUF_ATTRIBUTE_INIT_PRIORITY1 static std::true_type init_empty_string =
    (InitProtobufDefaultsSlow(), std::true_type{});

size_t StringSpaceUsedExcludingSelfLong(const TProtoStringType& str) {
  const void* start = &str;
  const void* end = &str + 1;
  if (start <= str.data() && str.data() < end) {
    // The string's data is stored inside the string object itself.
    return 0;
  } else {
    return str.capacity();
  }
}

template <typename T>
const T& Get(const void* ptr) {
  return *static_cast<const T*>(ptr);
}

// PrimitiveTypeHelper is a wrapper around the interface of WireFormatLite.
// WireFormatLite has a very inconvenient interface with respect to template
// meta-programming. This class wraps the different named functions into
// a single Serialize / SerializeToArray interface.
template <int type>
struct PrimitiveTypeHelper;

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_BOOL> {
  typedef bool Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteBoolNoTag(Get<bool>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteBoolNoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_INT32> {
  typedef arc_i32 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteInt32NoTag(Get<arc_i32>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteInt32NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_SINT32> {
  typedef arc_i32 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteSInt32NoTag(Get<arc_i32>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteSInt32NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_UINT32> {
  typedef arc_ui32 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteUInt32NoTag(Get<arc_ui32>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteUInt32NoTagToArray(Get<Type>(ptr), buffer);
  }
};
template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_INT64> {
  typedef arc_i64 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteInt64NoTag(Get<arc_i64>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteInt64NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_SINT64> {
  typedef arc_i64 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteSInt64NoTag(Get<arc_i64>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteSInt64NoTagToArray(Get<Type>(ptr), buffer);
  }
};
template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_UINT64> {
  typedef arc_ui64 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteUInt64NoTag(Get<arc_ui64>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteUInt64NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED32> {
  typedef arc_ui32 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteFixed32NoTag(Get<arc_ui32>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteFixed32NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED64> {
  typedef arc_ui64 Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    WireFormatLite::WriteFixed64NoTag(Get<arc_ui64>(ptr), output);
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    return WireFormatLite::WriteFixed64NoTagToArray(Get<Type>(ptr), buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_ENUM>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_INT32> {};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_SFIXED32>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED32> {
  typedef arc_i32 Type;
};
template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_SFIXED64>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED64> {
  typedef arc_i64 Type;
};
template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_FLOAT>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED32> {
  typedef float Type;
};
template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_DOUBLE>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_FIXED64> {
  typedef double Type;
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_STRING> {
  typedef TProtoStringType Type;
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    const Type& value = *static_cast<const Type*>(ptr);
    output->WriteVarint32(value.size());
    output->WriteRawMaybeAliased(value.data(), value.size());
  }
  static uint8_t* SerializeToArray(const void* ptr, uint8_t* buffer) {
    const Type& value = *static_cast<const Type*>(ptr);
    return io::CodedOutputStream::WriteStringWithSizeToArray(value, buffer);
  }
};

template <>
struct PrimitiveTypeHelper<WireFormatLite::TYPE_BYTES>
    : PrimitiveTypeHelper<WireFormatLite::TYPE_STRING> {};

// We want to serialize to both CodedOutputStream and directly into byte arrays
// without duplicating the code. In fact we might want extra output channels in
// the future.
template <typename O, int type>
struct OutputHelper;

template <int type, typename O>
void SerializeTo(const void* ptr, O* output) {
  OutputHelper<O, type>::Serialize(ptr, output);
}

template <typename O>
void WriteTagTo(arc_ui32 tag, O* output) {
  SerializeTo<WireFormatLite::TYPE_UINT32>(&tag, output);
}

template <typename O>
void WriteLengthTo(arc_ui32 length, O* output) {
  SerializeTo<WireFormatLite::TYPE_UINT32>(&length, output);
}

// Specialization for coded output stream
template <int type>
struct OutputHelper<io::CodedOutputStream, type> {
  static void Serialize(const void* ptr, io::CodedOutputStream* output) {
    PrimitiveTypeHelper<type>::Serialize(ptr, output);
  }
};

// Specialization for writing into a plain array
struct ArrayOutput {
  uint8_t* ptr;
  bool is_deterministic;
};

template <int type>
struct OutputHelper<ArrayOutput, type> {
  static void Serialize(const void* ptr, ArrayOutput* output) {
    output->ptr = PrimitiveTypeHelper<type>::SerializeToArray(ptr, output->ptr);
  }
};

void SerializeMessageNoTable(const MessageLite* msg,
                             io::CodedOutputStream* output) {
  msg->SerializeWithCachedSizes(output);
}

void SerializeMessageNoTable(const MessageLite* msg, ArrayOutput* output) {
  io::ArrayOutputStream array_stream(output->ptr, INT_MAX);
  io::CodedOutputStream o(&array_stream);
  o.SetSerializationDeterministic(output->is_deterministic);
  msg->SerializeWithCachedSizes(&o);
  output->ptr += o.ByteCount();
}

// We need to use a helper class to get access to the private members
class AccessorHelper {
 public:
  static int Size(const RepeatedPtrFieldBase& x) { return x.size(); }
  static void const* Get(const RepeatedPtrFieldBase& x, int idx) {
    return x.raw_data()[idx];
  }
};

void SerializeNotImplemented(int field) {
  Y_ABSL_LOG(FATAL) << "Not implemented field number " << field;
}

// When switching to c++11 we should make these constexpr functions
#define SERIALIZE_TABLE_OP(type, type_class) \
  ((type - 1) + static_cast<int>(type_class) * FieldMetadata::kNumTypes)

template <int type>
bool IsNull(const void* ptr) {
  return *static_cast<const typename PrimitiveTypeHelper<type>::Type*>(ptr) ==
         0;
}

template <>
bool IsNull<WireFormatLite::TYPE_STRING>(const void* ptr) {
  return static_cast<const ArenaStringPtr*>(ptr)->Get().size() == 0;
}

template <>
bool IsNull<WireFormatLite::TYPE_BYTES>(const void* ptr) {
  return static_cast<const ArenaStringPtr*>(ptr)->Get().size() == 0;
}

template <>
bool IsNull<WireFormatLite::TYPE_GROUP>(const void* ptr) {
  return Get<const MessageLite*>(ptr) == nullptr;
}

template <>
bool IsNull<WireFormatLite::TYPE_MESSAGE>(const void* ptr) {
  return Get<const MessageLite*>(ptr) == nullptr;
}

void ExtensionSerializer(const MessageLite* extendee, const uint8_t* ptr,
                         arc_ui32 offset, arc_ui32 tag, arc_ui32 has_offset,
                         io::CodedOutputStream* output) {
  reinterpret_cast<const ExtensionSet*>(ptr + offset)
      ->SerializeWithCachedSizes(extendee, tag, has_offset, output);
}

void UnknownFieldSerializerLite(const uint8_t* ptr, arc_ui32 offset,
                                arc_ui32 /*tag*/, arc_ui32 /*has_offset*/,
                                io::CodedOutputStream* output) {
  output->WriteString(
      reinterpret_cast<const InternalMetadata*>(ptr + offset)
          ->unknown_fields<TProtoStringType>(&internal::GetEmptyString));
}

MessageLite* DuplicateIfNonNullInternal(MessageLite* message) {
  if (message) {
    MessageLite* ret = message->New();
    ret->CheckTypeAndMergeFrom(*message);
    return ret;
  } else {
    return nullptr;
  }
}

void GenericSwap(MessageLite* m1, MessageLite* m2) {
  std::unique_ptr<MessageLite> tmp(m1->New());
  tmp->CheckTypeAndMergeFrom(*m1);
  m1->Clear();
  m1->CheckTypeAndMergeFrom(*m2);
  m2->Clear();
  m2->CheckTypeAndMergeFrom(*tmp);
}

// Returns a message owned by this Arena.  This may require Own()ing or
// duplicating the message.
MessageLite* GetOwnedMessageInternal(Arena* message_arena,
                                     MessageLite* submessage,
                                     Arena* submessage_arena) {
  Y_ABSL_DCHECK(Arena::InternalGetOwningArena(submessage) == submessage_arena);
  Y_ABSL_DCHECK(message_arena != submessage_arena);
  Y_ABSL_DCHECK_EQ(submessage_arena, nullptr);
  if (message_arena != nullptr && submessage_arena == nullptr) {
    message_arena->Own(submessage);
    return submessage;
  } else {
    MessageLite* ret = submessage->New(message_arena);
    ret->CheckTypeAndMergeFrom(*submessage);
    return ret;
  }
}

}  // namespace internal
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"
