#pragma once

#include "guid.h"
#include "zigzag.h"
#include "foreach.h"
#include "ref.h"

#include <ytlib/misc/assert.h>

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/ysaveload.h>

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// TODO: consider getting rid of these functions and using analogs from ysaveload.h
template <class T>
bool Read(TInputStream& input, T* data)
{
    return input.Load(data, sizeof(T)) == sizeof(T);
}

template <class T>
bool Read(TFile& file, T* data)
{
    return file.Read(data, sizeof(T)) == sizeof(T);
}

template <class T>
void Write(TOutputStream& output, const T& data)
{
    output.Write(&data, sizeof(T));
}

template <class T>
void Write(TFile& file, const T& data)
{
    file.Write(&data, sizeof(T));
}

template <class TKey>
yvector <typename yhash_set<TKey>::const_iterator> GetSortedIterators(
    const yhash_set<TKey>& set)
{
    typedef typename yhash_set<TKey>::const_iterator TIterator;
    yvector<TIterator> iterators;
    iterators.reserve(set.size());
    for (auto it = set.begin(); it != set.end(); ++it) {
        iterators.push_back(it);
    }
    std::sort(
        iterators.begin(),
        iterators.end(),
        [] (TIterator lhs, TIterator rhs) {
            return *lhs < *rhs;
        });
    return iterators;
}

template <class TSet>
void SaveSet(TOutputStream* output, const TSet& set)
{
    typedef typename TSet::key_type TKey;
    auto iterators = GetSortedIterators(set);
    ::SaveSize(output, iterators.size());
    FOREACH(const auto& ptr, iterators) {
        ::Save(output, *ptr);
    }
}

template <class TSet>
void LoadSet(TInputStream* input, TSet& set)
{
    typedef typename TSet::key_type TKey;
    size_t size = ::LoadSize(input);
    for (size_t i = 0; i < size; ++i) {
        TKey key;
        ::Load(input, key);
        YVERIFY(set.insert(key).second);
    }
}

template <class TSet>
void SaveNullableSet(TOutputStream* output, const TAutoPtr<TSet>& set)
{
    if (!set) {
        ::SaveSize(output, 0);
    } else {
        SaveSet(output, *set);
    }
}

template <class TSet>
void LoadNullableSet(TInputStream* input, TAutoPtr<TSet>& set)
{
    typedef typename TSet::key_type TKey;
    size_t size = ::LoadSize(input);
    if (size == 0) {
        set.Destroy();
        return;
    }
    
    set = new TSet();
    for (size_t i = 0; i < size; ++i) {
        TKey key;
        ::Load(input, key);
        YVERIFY(set->insert(key).second);
    }
}

template <class TKey, class TValue>
yvector <typename yhash_map<TKey, TValue>::const_iterator> GetSortedIterators(
    const yhash_map<TKey, TValue>& map)
{
    typedef typename yhash_map<TKey, TValue>::const_iterator TIterator;
    yvector<TIterator> iterators;
    iterators.reserve(map.size());
    for (auto it = map.begin(); it != map.end(); ++it) {
        iterators.push_back(it);
    }
    std::sort(
        iterators.begin(),
        iterators.end(),
        [] (TIterator lhs, TIterator rhs) {
            return lhs->First() < rhs->First();
        });
    return iterators;
}

template <class TMap>
void SaveMap(TOutputStream* output, const TMap& map)
{
    auto iterators = GetSortedIterators(map);
    ::SaveSize(output, iterators.size());
    FOREACH(const auto& it, iterators) {
        ::Save(output, it->First());
        ::Save(output, it->Second());
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
const size_t YTAlignment = 8;

static_assert(!(YTAlignment & (YTAlignment - 1)), "YTAlignment should be a power of two.");

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

template <class T>
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
template <>
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

template <class TArrayItem, class TProtoItem>
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

template <class T>
inline void ToProto(
    ::google::protobuf::RepeatedField<T>& proto,
    const yvector<T>& array,
    bool clear = true)
{
    if (clear) {
        proto.Clear();
    }
    for (int i = 0; i < array.ysize(); ++i) {
        *proto.Add() = array[i];
    }
}

template <class TArrayItem, class TProtoItem>
inline yvector<TArrayItem> FromProto(
    const ::google::protobuf::RepeatedPtrField<TProtoItem>& proto)
{
    yvector<TArrayItem> array(proto.size());
    for (int i = 0; i < proto.size(); ++i) {
        array[i] = TProtoTraits<TArrayItem>::FromProto(proto.Get(i));
    }
    return array;
}

////////////////////////////////////////////////////////////////////////////////

// Various functions that read/write varints from/to a stream.

// Returns the number of bytes written.
int WriteVarUInt64(TOutputStream* output, ui64 value);
int WriteVarInt32(TOutputStream* output, i32 value);
int WriteVarInt64(TOutputStream* output, i64 value);

// Returns the number of bytes read.
int ReadVarUInt64(TInputStream* input, ui64* value);
int ReadVarInt32(TInputStream* input, i32* value);
int ReadVarInt64(TInputStream* input, i64* value);

////////////////////////////////////////////////////////////////////////////////

//! Serializes a given protobuf message into a given blob.
//! Return true iff everything was OK.
bool SerializeProtobuf(const google::protobuf::Message* message, TBlob* data);

//! Deserializes a given chunk of memory into a given protobuf message.
//! Return true iff everything was OK.
bool DeserializeProtobuf(google::protobuf::Message* message, TRef data);

////////////////////////////////////////////////////////////////////////////////

//! Serializes a given protobuf message into a given stream
//! Throw yexception() in case of error
void SaveProto(TOutputStream* output, const ::google::protobuf::Message& message);

//! Reads from a given stream protobuf message
//! Throw yexception() in case of error
void LoadProto(TInputStream* input, ::google::protobuf::Message& message);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
