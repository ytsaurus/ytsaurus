#pragma once

#include "common.h"

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

// TODO: to cpp
inline void ToProto(const yvector<Stroka>& array, ::google::protobuf::RepeatedPtrField<TProtoStringType>& proto)
{
    for (int i = 0; i < array.ysize(); ++i) {
        *proto.Add() = array[i];
    }
}

// TODO: to cpp
inline yvector<Stroka> FromProto(const ::google::protobuf::RepeatedPtrField<TProtoStringType>& proto)
{
    yvector<Stroka> array(proto.size());
    for (int i = 0; i < proto.size(); ++i) {
        array[i] = proto.Get(i);
    }
    return array;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

