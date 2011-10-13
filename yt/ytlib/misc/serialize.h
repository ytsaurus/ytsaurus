#pragma once

#include "guid.h"
#include "zigzag.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template<class T>
bool Read(TInputStream& input, T* data)
{
    return input.Load(data, sizeof(T)) == sizeof(T);
}

template<class T>
bool Read(TFile& file, T* data)
{
    return file.Read(data, sizeof(T)) == sizeof(T);
}

template<class T>
void Write(TOutputStream& output, const T& data)
{
    output.Write(&data, sizeof(T));
}

template<class T>
void Write(TFile& file, const T& data)
{
    file.Write(&data, sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
const size_t YTAlignment = 8;

STATIC_ASSERT(!(YTAlignment & (YTAlignment - 1)));

//! Returns padding size: number of bytes required to make size
//! a factor of #YTAlignment.
int GetPaddingSize(i64 size);

//! Rounds up the #size to the nearest factor of #YTAlignment.
i64 AlignUp(i64 size);

//! Rounds up the #size to the nearest factor of #YTAlignment.
i32 AlignUp(i32 size);

//! Writes padding zeros.
void WritePadding(TOutputStream& output, i64 recordSize);

//! Writes padding zeros.
void WritePadding(TFile& file, i64 recordSize);

////////////////////////////////////////////////////////////////////////////////

template<class T>
struct TProtoTraits
{
    static const T& ToProto(const T& value)
    {
        return value;
    }

    static const T& FromProto(const T& value)
    {
        return value;
    }
};

// TODO: generify for other classes providing their own ToProto/FromProto methods
template<>
struct TProtoTraits<TGuid>
{
    static Stroka ToProto(const TGuid& value)
    {
        return value.ToProto();
    }

    static TGuid FromProto(const Stroka& value)
    {
        return TGuid::FromProto(value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template<class TArrayItem, class TProtoItem>
inline void ToProto(
    ::google::protobuf::RepeatedPtrField<TProtoItem>& proto,
    const yvector<TArrayItem>& array,
    bool clear = true)
{
    if (clear) {
        proto.Clear();
    }
    for (int i = 0; i < array.ysize(); ++i) {
        *proto.Add() = TProtoTraits<TArrayItem>::ToProto(array[i]);
    }
}

template<class TArrayItem, class TProtoItem>
inline yvector<TArrayItem> FromProto(
    const ::google::protobuf::RepeatedPtrField<TProtoItem>& proto)
{
    yvector<Stroka> array(proto.size());
    for (int i = 0; i < proto.size(); ++i) {
        array[i] = TProtoTraits<TArrayItem>::FromProto(proto.Get(i));
    }
    return array;
}

////////////////////////////////////////////////////////////////////////////////

//! Functions to read and write varints from stream

void WriteVarInt(ui64 value, TOutputStream* output);
void WriteVarInt32(i32 value, TOutputStream* output);
void WriteVarInt64(i64 value, TOutputStream* output);

ui64 ReadVarInt(TInputStream* input);
i32 ReadVarInt32(TInputStream* input);
i64 ReadVarInt64(TInputStream* input);

////////////////////////////////////////////////////////////////////////////////

} // namespace

