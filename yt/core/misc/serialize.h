#pragma once

#include "public.h"
#include "guid.h"
#include "ref.h"
#include "assert.h"
#include "mpl.h"
#include "property.h"
#include "nullable.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
const size_t SerializationAlignment = 8;
static_assert(
    (SerializationAlignment & (SerializationAlignment - 1)) == 0,
    "SerializationAlignment should be a power of two.");

namespace NDetail {

const ui8 SerializationPadding[SerializationAlignment] = { 0 };

} // namespace NDetail

//! Returns the minimum number whose addition to #size makes
//! the result divisible by #SerializationAlignment.
FORCED_INLINE size_t GetPaddingSize(size_t size)
{
    return
        (SerializationAlignment - (size & (SerializationAlignment - 1))) &
        (SerializationAlignment - 1);
}

//! Rounds up #size to the nearest factor of #SerializationAlignment.
FORCED_INLINE size_t AlignUp(size_t size)
{
    return (size + SerializationAlignment - 1) & ~(SerializationAlignment - 1);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOutput>
void Write(TOutput& output, const TRef& ref)
{
    output.Write(ref.Begin(), ref.Size());
}

template <class TOutput>
void Append(TOutput& output, const TRef& ref)
{
    output.Append(ref.Begin(), ref.Size());
}

template <class TInput>
size_t Read(TInput& input, TRef& ref)
{
    return input.LoadOrFail(ref.Begin(), ref.Size());
}

template <class TOutput, class T>
void WritePod(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    output.Write(&obj, sizeof(obj));
}

template <class TOutput, class T>
void AppendPod(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    output.Append(&obj, sizeof(obj));
}

template <class TInput, class T>
void ReadPod(TInput& input, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    return input.LoadOrFail(&obj, sizeof(obj));
}

template <class TOutput>
size_t WritePadding(TOutput& output, size_t writtenSize)
{
    output.Write(&NDetail::SerializationPadding, GetPaddingSize(writtenSize));
    return AlignUp(writtenSize);
}

template <class TOutput>
size_t WritePadded(TOutput& output, const TRef& ref)
{
    output.Write(ref.Begin(), ref.Size());
    output.Write(&NYT::NDetail::SerializationPadding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class TOutput>
size_t AppendPadded(TOutput& output, const TRef& ref)
{
    output.Append(ref.Begin(), ref.Size());
    output.Append(&NYT::NDetail::SerializationPadding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class TInput>
size_t ReadPadded(TInput& input, const TRef& ref)
{
    input.LoadOrFail(ref.Begin(), ref.Size());
    input.Skip(GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class TInput, class T>
size_t ReadPodPadded(TInput& input, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    auto objRef = TRef::FromPod(obj);
    return ReadPadded(input, objRef);
}

template <class TOutput, class T>
size_t AppendPodPadded(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    auto objRef = TRef::FromPod(obj);
    return AppendPadded(output, objRef);
}

template <class TOutput, class T>
size_t WritePodPadded(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    auto objRef = TRef::FromPod(obj);
    return WritePadded(output, objRef);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TSharedRef PackRefs(const T& parts)
{
    size_t size = 0;

    // Number of bytes to hold vector size.
    size += sizeof(i32);
    // Number of bytes to hold ref sizes.
    size += sizeof(i64) * parts.size();
    // Number of bytes to hold refs.
    for (const auto& ref : parts) {
        size += ref.Size();
    }

    struct TPackedRefsTag { };
    auto result = TSharedRef::Allocate<TPackedRefsTag>(size, false);
    TMemoryOutput output(result.Begin(), result.Size());

    WritePod(output, static_cast<i32>(parts.size()));
    for (const auto& ref : parts) {
        WritePod(output, static_cast<i64>(ref.Size()));
        Write(output, ref);
    }

    return result;
}

template <class T>
void UnpackRefs(const TSharedRef& packedRef, T* parts)
{
    TMemoryInput input(packedRef.Begin(), packedRef.Size());

    i32 size;
    ReadPod(input, size);
    YCHECK(size >= 0);

    parts->clear();
    parts->reserve(size);

    for (int index = 0; index < size; ++index) {
        i64 partSize;
        ReadPod(input, partSize);

        TRef partRef(const_cast<char*>(input.Buf()), static_cast<size_t>(partSize));
        parts->push_back(packedRef.Slice(partRef));

        input.Skip(partSize);
    }
}

template <class T>
size_t GetTotalSize(const std::vector<T>& parts)
{
    size_t size = 0;
    for (const auto& part : parts) {
        size += part.Size();
    }
    return size;
}

template <class T>
TSharedRef MergeRefs(const std::vector<T>& parts)
{
    size_t size = GetTotalSize(parts);
    struct TMergedBlockTag { };
    auto result = TSharedRef::Allocate<TMergedBlockTag>(size, false);
    size_t pos = 0;
    for (const auto& part : parts) {
        std::copy(part.Begin(), part.End(), result.Begin() + pos);
        pos += part.Size();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TStreamSaveContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TOutputStream*, Output);

public:
    TStreamSaveContext();
    explicit TStreamSaveContext(TOutputStream* output);

};

////////////////////////////////////////////////////////////////////////////////

class TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInputStream*, Input);

public:
    TStreamLoadContext();
    explicit TStreamLoadContext(TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EPersistenceDirection,
    (Save)
    (Load)
);

template <class TSaveContext, class TLoadContext>
class TCustomPersistenceContext
{
public:
    TCustomPersistenceContext(TSaveContext& context)
        : Direction_(EPersistenceDirection::Save)
        , SaveContext_(&context)
        , LoadContext_(nullptr)
    { }

    TCustomPersistenceContext(TLoadContext& context)
        : Direction_(EPersistenceDirection::Load)
        , SaveContext_(nullptr)
        , LoadContext_(&context)
    { }

    EPersistenceDirection GetDirection() const
    {
        return Direction_;
    }

    TSaveContext& SaveContext()
    {
        YASSERT(SaveContext_);
        return *SaveContext_;
    }

    TLoadContext& LoadContext()
    {
        YASSERT(LoadContext_);
        return *LoadContext_;
    }

private:
    EPersistenceDirection Direction_;
    TSaveContext* SaveContext_;
    TLoadContext* LoadContext_;

};

typedef
    TCustomPersistenceContext<
        TStreamSaveContext,
        TStreamLoadContext
    > TStreamPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
void Save(C& context, const T& value);

template <class T, class C>
void Load(C& context, T& value);

template <class T, class C>
T Load(C& context);

////////////////////////////////////////////////////////////////////////////////

DEFINE_MPL_MEMBER_DETECTOR(Persist);

template <class T, class = void>
struct TPersistMemberTraits
{ };

template <class T>
struct TPersistMemberTraits<T, typename NMpl::TEnableIfC<THasPersistMember<T>::Value>::TType>
{
    template <class S>
    struct TSignatureCracker
    { };

    template <class U, class C>
    struct TSignatureCracker<void (U::*)(C&)>
    {
        typedef C TContext;
    };

    typedef typename TSignatureCracker<decltype(&T::Persist)>::TContext TContext;
};

////////////////////////////////////////////////////////////////////////////////
// Simple types

struct TValueBoundSerializer
{
    template <class T, class C, class = void>
    struct TSaver
    {
        static void Do(C& context, const T& value)
        {
            value.Save(context);
        }
    };

    template <class T, class C, class = void>
    struct TLoader
    {
        static void Do(C& context, T& value)
        {
            value.Load(context);
        }
    };

    template <class T, class C>
    struct TSaver<T, C, typename NMpl::TEnableIfC<THasPersistMember<T>::Value>::TType>
    {
        static void Do(C& context, const T& value)
        {
            typename TPersistMemberTraits<T>::TContext wrappedContext(context);
            const_cast<T&>(value).Persist(wrappedContext);
        }
    };

    template <class T, class C>
    struct TLoader<T, C, typename NMpl::TEnableIfC<THasPersistMember<T>::Value>::TType>
    {
        static void Do(C& context, T& value)
        {
            typename TPersistMemberTraits<T>::TContext wrappedContext(context);
            value.Persist(wrappedContext);
        }
    };


    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        TSaver<T, C>::Do(context, value);
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        TLoader<T, C>::Do(context, value);
    }
};

struct TDefaultSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        NYT::Save(context, value);
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        NYT::Load(context, value);
    }
};

struct TRangeSerializer
{
    template <class C>
    static void Save(C& context, const TRef& value)
    {
        auto* output = context.GetOutput();
        output->Write(value.Begin(), value.Size());
    }

    template <class C>
    static void Load(C& context, const TRef& value)
    {
        auto* input = context.GetInput();
        YCHECK(input->Load(value.Begin(), value.Size()) == value.Size());
    }
};

struct TPodSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        TRangeSerializer::Save(context, TRef::FromPod(value));
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        TRangeSerializer::Load(context, TRef::FromPod(value));
    }
};

struct TSizeSerializer
{
    template <class C>
    static void Save(C& context, size_t value)
    {
        ui32 fixedValue = static_cast<ui32>(value);
        TPodSerializer::Save(context, fixedValue);
    }

    template <class C>
    static void Load(C& context, size_t& value)
    {
        ui32 fixedValue;
        TPodSerializer::Load(context, fixedValue);
        value = static_cast<size_t>(fixedValue);
    }

    template <class C>
    static size_t Load(C& context)
    {
        size_t value;
        Load(context, value);
        return value;
    }
};

struct TSharedRefSerializer
{
    template <class C>
    static void Save(C& context, const TSharedRef& value)
    {
        TSizeSerializer::Save(context, value.Size());
        auto* output = context.GetOutput();
        output->Write(value.Begin(), value.Size());
    }

    template <class C>
    static void Load(C& context, TSharedRef& value)
    {
        size_t size = TSizeSerializer::Load(context);
        value = TSharedRef::Allocate(size, false);
        auto* input = context.GetInput();
        YCHECK(input->Load(value.Begin(), value.Size()) == value.Size());
    }
};

struct TSharedRefArraySerializer
{
    template <class C>
    static void Save(C& context, const TSharedRefArray& value)
    {
        using NYT::Save;
        TSizeSerializer::Save(context, value.Size());
        for (const auto& part : value) {
            Save(context, part);
        }
    }

    template <class C>
    static void Load(C& context, TSharedRefArray& value)
    {
        using NYT::Load;
        size_t size = TSizeSerializer::Load(context);
        std::vector<TSharedRef> parts;
        parts.reserve(size);
        for (int index = 0; index < static_cast<int>(size); ++index) {
            Load(context, parts[index]);
        }
        value = TSharedRefArray(std::move(parts));
    }
};

struct TEnumSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& value)
    {
        NYT::Save(context, static_cast<i32>(value));
    }

    template <class T, class C>
    static void Load(C& context, T& value)
    {
        value = T(NYT::Load<i32>(context));
    }
};

struct TStrokaSerializer
{
    template <class C>
    static void Save(C& context, const Stroka& value)
    {
        TSizeSerializer::Save(context, value.size());
        TRangeSerializer::Save(context, TRef::FromString(value));
    }

    template <class C>
    static void Load(C& context, Stroka& value)
    {
        size_t size = TSizeSerializer::Load(context);
        value.resize(size);
        TRangeSerializer::Load(context, TRef::FromString(value));
    }
};

struct TNullableSerializer
{
    template <class T, class C>
    static void Save(C& context, const T& nullable)
    {
        using NYT::Save;
        Save(context, nullable.HasValue());
        if (nullable) {
            Save(context, *nullable);
        }
    }

    template <class T, class C>
    static void Load(C& context, T& nullable)
    {
        using NYT::Load;
        bool hasValue = Load<bool>(context);
        if (hasValue) {
            typename T::TValueType temp;
            Load(context, temp);
            nullable.Assign(std::move(temp));
        } else {
            nullable.Reset();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// Ordered collections

template <class TItemSerializer = TDefaultSerializer>
struct TVectorSerializer
{
    template <class TVector, class C>
    static void Save(C& context, const TVector& objects)
    {
        using NYT::Save;
        TSizeSerializer::Save(context, objects.size());
        for (const auto& object : objects) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TVector, class C>
    static void Load(C& context, TVector& objects)
    {
        using NYT::Load;
        size_t size = TSizeSerializer::Load(context);
        objects.resize(size);
        for (size_t index = 0; index != size; ++index) {
            TItemSerializer::Load(context, objects[index]);
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TListSerializer
{
    template <class TList, class C>
    static void Save(C& context, const TList& objects)
    {
        using NYT::Save;
        TSizeSerializer::Save(context, objects.size());
        for (const auto& object : objects) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TList, class C>
    static void Load(C& context, TList& objects)
    {
        using NYT::Load;
        size_t size = TSizeSerializer::Load(context);
        for (size_t index = 0; index != size; ++index) {
            typename TList::value_type obj;
            TItemSerializer::Load(context, obj);
            objects.push_back(obj);
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TNullableListSerializer
{
    template <class TList, class C>
    static void Save(C& context, const std::unique_ptr<TList>& objects)
    {
        using NYT::Save;
        if (objects) {
            TListSerializer<TItemSerializer>::Save(context, *objects);
        } else {
            TSizeSerializer::Save(context, 0);
        }
    }

    template <class TList, class C>
    static void Load(C& context, std::unique_ptr<TList>& objects)
    {
        using NYT::Load;
        
        size_t size = TSizeSerializer::Load(context);
        if (size == 0) {
            objects.reset();
            return;
        }

        objects.reset(new TList());
        for (size_t index = 0; index != size; ++index) {
            typename TList::value_type obj;
            TItemSerializer::Load(context, obj);
            objects->push_back(obj);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// Possibly unordered collections

template <class T, class C>
class TNoopSorter
{
public:
    typedef typename T::const_iterator TIterator;

    explicit TNoopSorter(const T& any)
        : Any_(any)
    { }

    TIterator begin()
    {
        return Any_.begin();
    }

    TIterator end()
    {
        return Any_.end();
    }

private:
    const T& Any_;

};

template <class C>
struct TSetSorterComparer
{
    template <class TIterator>
    static bool Compare(TIterator lhs, TIterator rhs)
    {
        typedef typename std::remove_const<typename std::remove_reference<decltype(*lhs)>::type>::type T;
        typedef typename TSerializerTraits<T, C>::TComparer TComparer;
        return TComparer::Compare(*lhs, *rhs);
    }
};

template <class C>
struct TMapSorterComparer
{
    template <class TIterator>
    static bool Compare(TIterator lhs, TIterator rhs)
    {
        typedef typename std::remove_const<typename std::remove_reference<decltype(lhs->first)>::type>::type T;
        typedef typename TSerializerTraits<T, C>::TComparer TComparer;
        return TComparer::Compare(lhs->first, rhs->first);
    }
};

template <class T, class Q>
class TCollectionSorter
{
public:
    typedef typename T::const_iterator TIterator;

    class TIteratorWrapper
    {
    public:
        TIteratorWrapper(const std::vector<TIterator>* iterators, size_t index)
            : Iterators_(iterators)
            , Index_(index)
        { }

        const typename T::value_type& operator * ()
        {
            return *((*Iterators_)[Index_]);
        }

        TIteratorWrapper& operator ++ ()
        {
            ++Index_;
            return *this;
        }

        bool operator == (const TIteratorWrapper& other) const
        {
            return Index_ == other.Index_;
        }

        bool operator != (const TIteratorWrapper& other) const
        {
            return Index_ != other.Index_;
        }

    private:
        const std::vector<TIterator>* Iterators_;
        size_t Index_;

    };

    explicit TCollectionSorter(const T& set)
    {
        Iterators_.reserve(set.size());
        for (auto it = set.begin(); it != set.end(); ++it) {
            Iterators_.push_back(it);
        }

        std::sort(
            Iterators_.begin(),
            Iterators_.end(),
            [] (TIterator lhs, TIterator rhs) {
                return Q::Compare(lhs, rhs);
            });
    }

    TIteratorWrapper begin() const
    {
        return TIteratorWrapper(&Iterators_, 0);
    }

    TIteratorWrapper end() const
    {
        return TIteratorWrapper(&Iterators_, Iterators_.size());
    }

private:
    std::vector<TIterator> Iterators_;

};

struct TSortedTag { };
struct TUnsortedTag { };

template <class T, class C, class TTag>
struct TSorterSelector
{ };

template <class T, class C>
struct TSorterSelector<T, C, TUnsortedTag>
{
    typedef TNoopSorter<T, C> TSorter;
};

template <class K, class C>
struct TSorterSelector<std::set<K>, C, TSortedTag>
{
    typedef TNoopSorter<std::set<K>, C> TSorter;
};

template <class K, class V, class C>
struct TSorterSelector<std::map<K, V>, C, TSortedTag>
{
    typedef TNoopSorter<std::map<K, V>, C> TSorter;
};

template <class K, class C>
struct TSorterSelector<yhash_set<K>, C, TSortedTag>
{
    typedef TCollectionSorter<yhash_set<K>, TSetSorterComparer<C>> TSorter;
};

template <class K, class V, class C>
struct TSorterSelector<yhash_map<K, V>, C, TSortedTag>
{
    typedef TCollectionSorter<yhash_map<K, V>, TMapSorterComparer<C>> TSorter;
};

template <class K, class C>
struct TSorterSelector<yhash_multiset<K>, C, TSortedTag>
{
    typedef TCollectionSorter<yhash_multiset<K>, TSetSorterComparer<C>> TSorter;
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TSetSerializer
{
    template <class TSet, class C>
    static void Save(C& context, const TSet& set)
    {
        TSizeSerializer::Save(context, set.size());
        typename TSorterSelector<TSet, C, TSortTag>::TSorter sorter(set);
        for (const auto& item : sorter) {
            TItemSerializer::Save(context, item);
        }
    }

    template <class TSet, class C>
    static void Load(C& context, TSet& set)
    {
        typedef typename TSet::key_type TKey;

        size_t size = TSizeSerializer::Load(context);
        set.clear();
        for (size_t i = 0; i < size; ++i) {
            TKey key;
            TItemSerializer::Load(context, key);
            YCHECK(set.insert(key).second);
        }
    }
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TMultiSetSerializer
{
    template <class TSet, class C>
    static void Save(C& context, const TSet& set)
    {
        TSetSerializer<TItemSerializer, TSortTag>::Save(context, set);
    }

    template <class TSet, class C>
    static void Load(C& context, TSet& set)
    {
        typedef typename TSet::key_type TKey;

        size_t size = TSizeSerializer::Load(context);
        set.clear();
        for (size_t i = 0; i < size; ++i) {
            TKey key;
            TItemSerializer::Load(context, key);
            set.insert(key);
        }
    }
};

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TNullableSetSerializer
{
    template <class TSet, class C>
    static void Save(C& context, const std::unique_ptr<TSet>& set)
    {
        if (set) {
            TSetSerializer<TItemSerializer, TSortTag>::Save(context, *set);
        } else {
            TSizeSerializer::Save(context, 0);
        }
    }

    template <class TSet, class C>
    static void Load(C& context, std::unique_ptr<TSet>& set)
    {
        using NYT::Load;
        typedef typename TSet::key_type TKey;

        size_t size = TSizeSerializer::Load(context);
        if (size == 0) {
            set.reset();
            return;
        }

        set.reset(new TSet());
        for (size_t index = 0; index < size; ++index) {
            TKey key;
            TItemSerializer::Load(context, key);
            YCHECK(set->insert(key).second);
        }
    }
};

template <
    class TKeySerializer = TDefaultSerializer,
    class TValueSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
>
struct TMapSerializer
{
    template <class TMap, class C>
    static void Save(C& context, const TMap& map)
    {
        TSizeSerializer::Save(context, map.size());
        typename TSorterSelector<TMap, C, TSortTag>::TSorter sorter(map);
        for (const auto& pair : sorter) {
            TKeySerializer::Save(context, pair.first);
            TValueSerializer::Save(context, pair.second);
        }
    }

    template <class TMap, class C>
    static void Load(C& context, TMap& map)
    {
        size_t size = TSizeSerializer::Load(context);
        map.clear();
        for (size_t index = 0; index < size; ++index) {
            typename TMap::key_type key;
            TKeySerializer::Load(context, key);
            typename TMap::mapped_type value;
            TValueSerializer::Load(context, value);
            YCHECK(map.insert(std::make_pair(key, value)).second);
        }
    }
};

template <
    class TKeySerializer,
    class TValueSerializer,
    class TSortTag
>
struct TMultiMapSerializer
{
    template <class TMap, class C>
    static void Save(C& context, const TMap& map)
    {
        TMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TSortTag
        >::Save(context, map);
    }

    template <class TMap, class C>
    static void Load(C& context, TMap& map)
    {
        map.clear();
        size_t size = TSizeSerializer::Load(context);
        for (size_t index = 0; index < size; ++index) {
            typename TMap::key_type key;
            TKeySerializer::Load(context, key);
            typename TMap::mapped_type value;
            TValueSerializer::Load(context, value);
            map.insert(std::make_pair(key, value));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TValueBoundComparer
{
    template <class T>
    static bool Compare(const T& lhs, const T& rhs)
    {
        return lhs < rhs;
    }
};

template <class T, class C, class>
struct TSerializerTraits
{
    typedef TValueBoundSerializer TSerializer;
    typedef TValueBoundComparer TComparer;
};

template <class T, class C>
void Save(C& context, const T& value)
{
    TSerializerTraits<T, C>::TSerializer::Save(context, value);
}

template <class T, class C>
void Load(C& context, T& value)
{
    TSerializerTraits<T, C>::TSerializer::Load(context, value);
}

template <class T, class C>
T Load(C& context)
{
    T value;
    Load(context, value);
    return value;
}

template <class S, class T, class C>
void CustomPersist(C& context, T& value)
{
    switch (context.GetDirection()) {
        case  EPersistenceDirection::Save:
            S::Save(context.SaveContext(), value);
            break;
        case EPersistenceDirection::Load:
            S::Load(context.LoadContext(), value);
            break;
        default:
            YUNREACHABLE();
    }
}

template <class T, class C>
void Persist(C& context, T& value)
{
    CustomPersist<TDefaultSerializer, T, C>(context, value);
}

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<TSharedRef, C, void>
{
    typedef TSharedRefSerializer TSerializer;
};

template <class C>
struct TSerializerTraits<TSharedRefArray, C, void>
{
    typedef TSharedRefArraySerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC<
        NMpl::TAndC<
            NMpl::TIsPod<T>::Value,
            NMpl::TNotC<TTypeTraits<T>::IsPointer>::Value
        >::Value
    >::TType
>
{
    typedef TPodSerializer TSerializer;
    typedef TValueBoundComparer TComparer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T&, TEnumBase<T>&>
    >::TType
>
{
    typedef TEnumSerializer TSerializer;
    typedef TValueBoundComparer TComparer;
};

template <class C>
struct TSerializerTraits<Stroka, C, void>
{
    typedef TStrokaSerializer TSerializer;
    typedef TValueBoundComparer TComparer;
};

template <class T, class C>
struct TSerializerTraits<TNullable<T>, C, void>
{
    typedef TNullableSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::vector<T>, C, void>
{
    typedef TVectorSerializer<> TSerializer;
};

template <class T, unsigned size>
class SmallVector;

template <class T, unsigned size, class C>
struct TSerializerTraits<SmallVector<T, size>, C, void>
{
    typedef TVectorSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::list<T>, C, void>
{
    typedef TListSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::set<T>, C, void>
{
    typedef TSetSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<yhash_set<T>, C, void>
{
    typedef TSetSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<yhash_multiset<T>, C, void>
{
    typedef TMultiSetSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<std::list<T>>, C, void>
{
    typedef TNullableListSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<std::set<T>>, C, void>
{
    typedef TNullableSetSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<yhash_set<T>>, C, void>
{
    typedef TNullableSetSerializer<> TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<std::map<K, V>, C, void>
{
    typedef TMapSerializer<> TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<yhash_map<K, V>, C, void>
{
    typedef TMapSerializer<> TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
