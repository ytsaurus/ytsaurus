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

//! Alignment size; measured in bytes and must be a power of two.
const size_t YTAlignment = 8;

//! Auxiliary constants and functions.
namespace NDetails {

const ui8 Padding[YTAlignment] = { 0 };

} // namespace NDetails

static_assert(!(YTAlignment & (YTAlignment - 1)), "YTAlignment should be a power of two.");

//! Rounds up the #size to the nearest factor of #YTAlignment.
template <class T>
T GetPaddingSize(T size)
{
    T result = static_cast<T>(size % YTAlignment);
    return result == 0 ? 0 : YTAlignment - result;
}

template <class T>
T AlignUp(T size)
{
    return size + GetPaddingSize(size);
}

template <class OutputStream>
size_t WritePaddingZeroes(OutputStream& output, i64 writtenSize)
{
    output.Write(&NDetails::Padding, GetPaddingSize(writtenSize));
    return AlignUp(writtenSize);
}

template <class OutputStream>
void Write(OutputStream& output, const TRef& ref)
{
    output.Write(ref.Begin(), ref.Size());
}

template <class OutputStream>
void Append(OutputStream& output, const TRef& ref)
{
    output.Append(ref.Begin(), ref.Size());
}

template <class InputStream>
size_t Read(InputStream& input, TRef& ref)
{
    return input.Read(ref.Begin(), ref.Size());
}

template <class OutputStream, class T>
void WritePod(OutputStream& output, const T& obj)
{
    output.Write(&obj, sizeof(obj));
}

template <class OutputStream, class T>
void AppendPod(OutputStream& output, const T& obj)
{
    output.Append(&obj, sizeof(obj));
}

template <class InputStream, class T>
size_t ReadPod(InputStream& input, T& obj)
{
    return input.Read(&obj, sizeof(obj));
}

template <class OutputStream>
size_t WritePadded(OutputStream& output, const TRef& ref)
{
    output.Write(ref.Begin(), ref.Size());
    output.Write(&NDetails::Padding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class OutputStream>
size_t AppendPadded(OutputStream& output, const TRef& ref)
{
    output.Append(ref.Begin(), ref.Size());
    output.Append(&NDetails::Padding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class InputStream>
size_t ReadPadded(InputStream& input, TRef& ref)
{
    input.Read(ref.Begin(), ref.Size());
    input.Skip(GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class InputStream, class T>
size_t ReadPodPadded(InputStream& input, T& obj)
{
    auto objRef = TRef::FromPod(obj);
    return ReadPadded(input, objRef);
}

template <class OutputStream, class T>
size_t AppendPodPadded(OutputStream& output, const T& obj)
{
    auto objRef = TRef::FromPod(obj);
    return AppendPadded(output, objRef);
}

template <class OutputStream, class T>
size_t WritePodPadded(OutputStream& output, const T& obj)
{
    auto objRef = TRef::FromPod(obj);
    return WritePadded(output, objRef);
}

////////////////////////////////////////////////////////////////////////////////

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

using ::Load;
using ::Save;
using ::SaveSize;

template <class TSet>
void SaveSet(TOutputStream* output, const TSet& set)
{
    typedef typename TSet::key_type TKey;
    auto iterators = GetSortedIterators(set);
    SaveSize(output, iterators.size());
    FOREACH (const auto& ptr, iterators) {
        Save(output, *ptr);
    }
}

template <class TSet>
void LoadSet(TInputStream* input, TSet& set)
{
    typedef typename TSet::key_type TKey;
    size_t size = ::LoadSize(input);
    set.clear();
    for (size_t i = 0; i < size; ++i) {
        TKey key;
        Load(input, key);
        YCHECK(set.insert(key).second);
    }
}

template <class TSet>
void SaveNullableSet(TOutputStream* output, const THolder<TSet>& set)
{
    if (~set) {
        SaveSet(output, *set);
    } else {
        SaveSize(output, 0);
    }
}

template <class TSet>
void LoadNullableSet(TInputStream* input, THolder<TSet>& set)
{
    typedef typename TSet::key_type TKey;

    size_t size = ::LoadSize(input);
    if (size == 0) {
        set.Destroy();
        return;
    }

    set.Reset(new TSet());
    for (size_t index = 0; index < size; ++index) {
        TKey key;
        Load(input, key);
        YCHECK(set->insert(key).second);
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
            return lhs->first < rhs->first;
        });
    return iterators;
}

template <class TMap>
void SaveMap(TOutputStream* output, const TMap& map)
{
    auto iterators = GetSortedIterators(map);
    SaveSize(output, iterators.size());
    FOREACH (const auto& it, iterators) {
        Save(output, it->first);
        Save(output, it->second);
    }
}

template <class TMap>
void LoadMap(TInputStream* input, TMap& map)
{
    map.clear();
    size_t size = ::LoadSize(input);
    for (size_t index = 0; index < size; ++index) {
        typename TMap::key_type key;
        Load(input, key);
        typename TMap::mapped_type value;
        Load(input, value);
        YCHECK(map.insert(MakePair(key, value)).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef PackRefs(const std::vector<TSharedRef>& refs);
void UnpackRefs(const TSharedRef& packedRef, std::vector<TSharedRef>* refs);

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

} // namespace NYT
