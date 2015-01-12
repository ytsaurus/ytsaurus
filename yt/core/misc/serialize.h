#pragma once

#include "guid.h"
#include "ref.h"
#include "assert.h"
#include "mpl.h"
#include "property.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Alignment size; measured in bytes and must be a power of two.
const size_t YTAlignment = 8;

//! Auxiliary constants and functions.
namespace NDetail {

const ui8 Padding[YTAlignment] = { 0 };

} // namespace NDetail

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
    output.Write(&NDetail::Padding, GetPaddingSize(writtenSize));
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
    output.Write(&NDetail::Padding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class OutputStream>
size_t AppendPadded(OutputStream& output, const TRef& ref)
{
    output.Append(ref.Begin(), ref.Size());
    output.Append(&NDetail::Padding, GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class InputStream>
size_t ReadPadded(InputStream& input, const TRef& ref)
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

TSharedRef PackRefs(const std::vector<TSharedRef>& refs);
void UnpackRefs(const TSharedRef& packedRef, std::vector<TSharedRef>* refs);

////////////////////////////////////////////////////////////////////////////////

class TStreamSaveContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TOutputStream*, Output);

public:
    TStreamSaveContext();

};

class TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInputStream*, Input);

public:
    TStreamLoadContext();

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

template <class T>
std::vector<typename T::const_iterator> GetSortedSetIterators(const T& set)
{
    typedef typename T::const_iterator TIterator;
    std::vector<TIterator> iterators;
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

template <class T>
std::vector <typename T::const_iterator> GetSortedMapIterators(const T& map)
{
    typedef typename T::const_iterator TIterator;
    std::vector<TIterator> iterators;
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

template <class TKey>
std::vector <typename yhash_set<TKey>::const_iterator> GetSortedIterators(
    const yhash_set<TKey>& set)
{
    return GetSortedSetIterators(set);
}

template <class TKey, class TValue>
std::vector <typename std::map<TKey, TValue>::const_iterator> GetSortedIterators(
    const std::map<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector <typename yhash_map<TKey, TValue>::const_iterator> GetSortedIterators(
    const yhash_map<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector <typename yhash_multimap<TKey, TValue>::const_iterator> GetSortedIterators(
    const yhash_multimap<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

template <class TKey, class TValue>
std::vector <typename std::multimap<TKey, TValue>::const_iterator> GetSortedIterators(
    const std::multimap<TKey, TValue>& map)
{
    return GetSortedMapIterators(map);
}

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

struct TVectorSerializer
{
    template <class TVector, class C>
    static void Save(C& context, const TVector& objects)
    {
        using NYT::Save;
        TSizeSerializer::Save(context, objects.size());
        FOREACH (const auto& object, objects) {
            Save(context, object);
        }
    }

    template <class TVector, class C>
    static void Load(C& context, TVector& objects)
    {
        using NYT::Load;
        size_t size = TSizeSerializer::Load(context);
        objects.resize(size);
        for (size_t index = 0; index != size; ++index) {
            Load(context, objects[index]);
        }
    }
};

struct TListSerializer
{
    template <class TList, class C>
    static void Save(C& context, const TList& objects)
    {
        using NYT::Save;
        TSizeSerializer::Save(context, objects.size());
        FOREACH (const auto& object, objects) {
            Save(context, object);
        }
    }

    template <class TList, class C>
    static void Load(C& context, TList& objects)
    {
        using NYT::Load;
        size_t size = TSizeSerializer::Load(context);
        for (size_t index = 0; index != size; ++index) {
            typename TList::value_type obj;
            Load(context, obj);
            objects.push_back(obj);
        }
    }
};

struct TNullableListSerializer
{
    template <class TList, class C>
    static void Save(C& context, const std::unique_ptr<TList>& objects)
    {
        using NYT::Save;
        if (objects) {
            TListSerializer::Save(context, *objects);
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
            Load(context, obj);
            objects->push_back(obj);
        }
    }
};

template <class TItemSerializer>
struct TCustomSetSerializer
{
    template <class TSet, class C>
    static void Save(C& context, const TSet& set)
    {
        typedef typename TSet::key_type TKey;
        auto iterators = GetSortedIterators(set);
        TSizeSerializer::Save(context, iterators.size());
        FOREACH (const auto& ptr, iterators) {
            TItemSerializer::Save(context, *ptr);
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

typedef TCustomSetSerializer<TDefaultSerializer> TSetSerializer;

struct TNullableSetSerializer
{
    template <class TSet, class C>
    static void Save(C& context, const std::unique_ptr<TSet>& set)
    {
        if (set) {
            TSetSerializer::Save(context, *set);
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
            Load(context, key);
            YCHECK(set->insert(key).second);
        }
    }
};

template <class TKeySerializer, class TValueSerializer>
struct TCustomMapSerializer
{
    template <class TMap, class C>
    static void Save(C& context, const TMap& map)
    {
        using NYT::Save;
        auto iterators = GetSortedIterators(map);
        TSizeSerializer::Save(context, iterators.size());
        FOREACH (const auto& it, iterators) {
            TKeySerializer::Save(context, it->first);
            TValueSerializer::Save(context, it->second);
        }
    }

    template <class TMap, class C>
    static void Load(C& context, TMap& map)
    {
        using NYT::Load;
        map.clear();
        size_t size = TSizeSerializer::Load(context);
        for (size_t index = 0; index < size; ++index) {
            typename TMap::key_type key;
            TKeySerializer::Load(context, key);
            typename TMap::mapped_type value;
            TValueSerializer::Load(context, value);
            YCHECK(map.insert(std::make_pair(key, value)).second);
        }
    }
};

typedef TCustomMapSerializer<TDefaultSerializer, TDefaultSerializer> TMapSerializer;

template <class TKeySerializer, class TValueSerializer>
struct TCustomMultiMapSerializer
{
    template <class TMap, class C>
    static void Save(C& context, const TMap& map)
    {
        TCustomMapSerializer<TKeySerializer, TValueSerializer>::Save(context, map);
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

typedef TCustomMultiMapSerializer<TDefaultSerializer, TDefaultSerializer> TMultiMapSerializer;

////////////////////////////////////////////////////////////////////////////////

template <class T, class C, class = void>
struct TSerializerTraits
{
    typedef TValueBoundSerializer TSerializer;
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

template <class T, class C>
void Persist(C& context, T& value)
{
    switch (context.GetDirection()) {
        case  EPersistenceDirection::Save:
            Save(context.SaveContext(), value);
            break;
        case EPersistenceDirection::Load:
            Load(context.LoadContext(), value);
            break;
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

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
};

template <class C>
struct TSerializerTraits<Stroka, C, void>
{
    typedef TStrokaSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::vector<T>, C, void>
{
    typedef TVectorSerializer TSerializer;
};

template <class T, unsigned size>
class TSmallVector;

template <class T, unsigned size, class C>
struct TSerializerTraits<TSmallVector<T, size>, C, void>
{
    typedef TVectorSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::list<T>, C, void>
{
    typedef TListSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::set<T>, C, void>
{
    typedef TSetSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unordered_set<T>, C, void>
{
    typedef TSetSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<yhash_set<T>, C, void>
{
    typedef TSetSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<std::list<T>>, C, void>
{
    typedef TNullableListSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<std::set<T>>, C, void>
{
    typedef TNullableSetSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<std::unordered_set<T>>, C, void>
{
    typedef TNullableSetSerializer TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::unique_ptr<yhash_set<T>>, C, void>
{
    typedef TNullableSetSerializer TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<std::map<K, V>, C, void>
{
    typedef TMapSerializer TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<std::unordered_map<K, V>, C, void>
{
    typedef TMapSerializer TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<yhash_map<K, V>, C, void>
{
    typedef TMapSerializer TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<std::multimap<K, V>, C, void>
{
    typedef TMultiMapSerializer TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<yhash_multimap<K, V>, C, void>
{
    typedef TMultiMapSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
