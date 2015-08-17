#pragma once

#include "public.h"
#include "guid.h"
#include "ref.h"
#include "assert.h"
#include "mpl.h"
#include "property.h"
#include "nullable.h"
#include "enum.h"
#include "serialize_dump.h"

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

const ui8 SerializationPadding[SerializationAlignment] = {};

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
FORCED_INLINE size_t AlignUp(size_t size, size_t align = SerializationAlignment)
{
    return (size + align - 1) & ~(align - 1);
}

FORCED_INLINE char* AlignUp(char* ptr, size_t align = SerializationAlignment)
{
    return reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(ptr) + align - 1) & ~(align - 1));
}

////////////////////////////////////////////////////////////////////////////////

template <class TOutput>
void Write(TOutput& output, const TRef& ref)
{
    output.Write(ref.Begin(), ref.Size());
}

template <class TInput>
size_t Read(TInput& input, TRef& ref)
{
    auto loadBytes = input.Load(ref.Begin(), ref.Size());
    YCHECK(loadBytes == ref.Size());
    return loadBytes;
}

template <class TOutput, class T>
void WritePod(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    output.Write(&obj, sizeof(obj));
}

template <class TInput, class T>
void ReadPod(TInput& input, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    auto loadBytes = input.Load(&obj, sizeof(obj));
    YCHECK(loadBytes == sizeof(obj));
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

template <class TInput>
size_t ReadPadded(TInput& input, const TMutableRef& ref)
{
    auto loadBytes = input.Load(ref.Begin(), ref.Size());
    YCHECK(loadBytes == ref.Size());
    input.Skip(GetPaddingSize(ref.Size()));
    return AlignUp(ref.Size());
}

template <class TInput, class T>
size_t ReadPodPadded(TInput& input, T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    return ReadPadded(input, TMutableRef::FromPod(obj));
}

template <class TOutput, class T>
size_t WritePodPadded(TOutput& output, const T& obj)
{
    static_assert(TTypeTraits<T>::IsPod, "T must be a pod-type.");
    return WritePadded(output, TRef::FromPod(obj));
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
    auto result = TSharedMutableRef::Allocate<TPackedRefsTag>(size, false);
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

        parts->push_back(packedRef.Slice(input.Buf(), input.Buf() + partSize));

        input.Skip(partSize);
    }
}

template <class T>
TSharedRef MergeRefs(const std::vector<T>& parts)
{
    size_t size = GetByteSize(parts);
    struct TMergedBlockTag { };
    auto result = TSharedMutableRef::Allocate<TMergedBlockTag>(size, false);
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
    DEFINE_BYREF_RW_PROPERTY(TSerializationDumper, Dumper);

public:
    TStreamLoadContext();
    explicit TStreamLoadContext(TInputStream* input);

};

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext>
class TCustomPersistenceContext
{
public:
    TCustomPersistenceContext(TSaveContext& context)
        : SaveContext_(&context)
        , LoadContext_(nullptr)
    { }

    TCustomPersistenceContext(TLoadContext& context)
        : SaveContext_(nullptr)
        , LoadContext_(&context)
    { }

    bool IsSave() const
    {
        return SaveContext_ != nullptr;
    }

    TSaveContext& SaveContext()
    {
        YASSERT(SaveContext_);
        return *SaveContext_;
    }

    bool IsLoad() const
    {
        return LoadContext_ != nullptr;
    }

    TLoadContext& LoadContext()
    {
        YASSERT(LoadContext_);
        return *LoadContext_;
    }

private:
    TSaveContext* const SaveContext_;
    TLoadContext* const LoadContext_;

};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
void Save(C& context, const T& value);

template <class T, class C>
void Load(C& context, T& value);

template <class T, class C>
T Load(C& context);

////////////////////////////////////////////////////////////////////////////////
// TODO(babenko): move to inl

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
T LoadSuspended(C& context)
{
    SERIALIZATION_DUMP_SUSPEND(context) {
        return Load<T, C>(context);
    }
}

template <class S, class T, class C>
void Persist(C& context, T& value)
{
    if (context.IsSave()) {
        S::Save(context.SaveContext(), value);
    } else if (context.IsLoad()) {
        S::Load(context.LoadContext(), value);
    } else {
        YUNREACHABLE();
    }
}

struct TDefaultSerializer;

template <class T, class C>
void Persist(C& context, T& value)
{
    Persist<TDefaultSerializer, T, C>(context, value);
}

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
    static void Load(C& context, const TMutableRef& value)
    {
        auto* input = context.GetInput();
        YCHECK(input->Load(value.Begin(), value.Size()) == value.Size());

        SERIALIZATION_DUMP_WRITE(context, "raw[%v] %v", value.Size(), DumpRangeToHex(value));
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
        SERIALIZATION_DUMP_SUSPEND(context) {
            TRangeSerializer::Load(context, TMutableRef::FromPod(value));
        }
        TSerializationDumpPodWriter<T>::Do(context, value);
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


    // Helpers.
    template <class C>
    static size_t Load(C& context)
    {
        size_t value;
        Load(context, value);
        return value;
    }

    template <class C>
    static size_t LoadSuspended(C& context)
    {
        SERIALIZATION_DUMP_SUSPEND(context) {
            return Load(context);
        }
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
        size_t size = TSizeSerializer::LoadSuspended(context);
        auto mutableValue = TSharedMutableRef::Allocate(size, false);

        auto* input = context.GetInput();
        YCHECK(input->Load(mutableValue.Begin(), mutableValue.Size()) == mutableValue.Size());
        value = mutableValue;

        SERIALIZATION_DUMP_WRITE(context, "TSharedRef %v", DumpRangeToHex(value));
    }
};

struct TSharedRefArraySerializer
{
    template <class C>
    static void Save(C& context, const TSharedRefArray& value)
    {
        TSizeSerializer::Save(context, value.Size());

        for (const auto& part : value) {
            Save(context, part);
        }
    }

    template <class C>
    static void Load(C& context, TSharedRefArray& value)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        std::vector<TSharedRef> parts(size);

        SERIALIZATION_DUMP_WRITE(context, "TSharedRefArray[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (int index = 0; index < static_cast<int>(size); ++index) {
                SERIALIZATION_DUMP_SUSPEND(context) {
                    Load(context, parts[index]);
                }
                SERIALIZATION_DUMP_WRITE(context, "%v => %v", index, DumpRangeToHex(parts[index]));
            }
        }
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
        SERIALIZATION_DUMP_SUSPEND(context) {
            value = T(NYT::Load<i32>(context));
        }

        SERIALIZATION_DUMP_WRITE(context, "%v %v", TEnumTraits<T>::GetTypeName(), value);
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
        size_t size = TSizeSerializer::LoadSuspended(context);
        value.resize(size);

        SERIALIZATION_DUMP_SUSPEND(context) {
            TRangeSerializer::Load(context, TMutableRef::FromString(value));
        }

        SERIALIZATION_DUMP_WRITE(context, "Stroka %Qv", value);
    }
};

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TNullableSerializer
{
    template <class T, class C>
    static void Save(C& context, const TNullable<T>& nullable)
    {
        using NYT::Save;

        Save(context, nullable.HasValue());

        if (nullable) {
            TUnderlyingSerializer::Save(context, *nullable);
        }
    }

    template <class T, class C>
    static void Load(C& context, TNullable<T>& nullable)
    {
        using NYT::Load;

        bool hasValue = LoadSuspended<bool>(context);

        if (hasValue) {
            T temp;
            TUnderlyingSerializer::Load(context, temp);
            nullable.Assign(std::move(temp));
        } else {
            nullable.Reset();
            SERIALIZATION_DUMP_WRITE(context, "null");
        }
    }
};

template <class TUnderlyingSerializer = TDefaultSerializer>
struct TAtomicSerializer
{
    template <class T, class C>
    static void Save(C& context, const std::atomic<T>& value)
    {
        TUnderlyingSerializer::Save(context, value.load());
    }

    template <class T, class C>
    static void Load(C& context, std::atomic<T>& value)
    {
        T temp;
        TUnderlyingSerializer::Load(context, temp);
        value.store(std::move(temp));
    }
};

////////////////////////////////////////////////////////////////////////////////
// Sorters

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

template <class T, class C>
struct TSorterSelector<std::vector<T>, C, TSortedTag>
{
    typedef TCollectionSorter<std::vector<T>, TSetSorterComparer<C>> TSorter;
};

template <class T, class C, unsigned size>
struct TSorterSelector<SmallVector<T, size>, C, TSortedTag>
{
    typedef TCollectionSorter<SmallVector<T, size>, TSetSorterComparer<C>> TSorter;
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

template <class K, class H, class E, class A, class C>
struct TSorterSelector<yhash_set<K, H, E, A>, C, TSortedTag>
{
    typedef TCollectionSorter<yhash_set<K, H, E, A>, TSetSorterComparer<C>> TSorter;
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

////////////////////////////////////////////////////////////////////////////////
// Ordered collections

template <
    class TItemSerializer = TDefaultSerializer,
    class TSortTag = TUnsortedTag
>
struct TVectorSerializer
{
    template <class TVector, class C>
    static void Save(C& context, const TVector& objects)
    {
        TSizeSerializer::Save(context, objects.size());

        typename TSorterSelector<TVector, C, TSortTag>::TSorter sorter(objects);
        for (const auto& object : sorter) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TVector, class C>
    static void Load(C& context, TVector& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        objects.resize(size);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, objects[index]);
                }
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TListSerializer
{
    template <class TList, class C>
    static void Save(C& context, const TList& objects)
    {
        TSizeSerializer::Save(context, objects.size());

        for (const auto& object : objects) {
            TItemSerializer::Save(context, object);
        }
    }

    template <class TList, class C>
    static void Load(C& context, TList& objects)
    {
        size_t size = TSizeSerializer::LoadSuspended(context);
        objects.clear();

        SERIALIZATION_DUMP_WRITE(context, "list[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                typename TList::value_type obj;
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, obj);
                }
                objects.push_back(obj);
            }
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
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "list[%v]", size);

        if (size == 0) {
            objects.reset();
            return;
        }

        objects.reset(new TList());

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                typename TList::value_type obj;
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, obj);
                }
                objects->push_back(obj);
            }
        }
    }
};

template <class TItemSerializer = TDefaultSerializer>
struct TEnumIndexedVectorSerializer
{
    template <class T, class E, class C, E Min, E Max>
    static void Save(C& context, const TEnumIndexedVector<T, E, Min, Max>& vector)
    {
        using NYT::Save;

        auto keys = TEnumTraits<E>::GetDomainValues();
        TSizeSerializer::Save(context, keys.size());

        for (auto key : keys) {
            Save(context, key);
            TItemSerializer::Save(context, vector[key]);
        }
    }

    template <class T, class E, class C, E Min, E Max>
    static void Load(C& context, TEnumIndexedVector<T, E, Min, Max>& vector)
    {
        std::fill(vector.begin(), vector.end(), T());

        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "vector[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index != size; ++index) {
                auto key = LoadSuspended<E>(context);
                SERIALIZATION_DUMP_WRITE(context, "%v =>", key);
                SERIALIZATION_DUMP_INDENT(context) {
                    if (key < TEnumTraits<E>::GetMinValue() || key > TEnumTraits<E>::GetMaxValue()) {
                        T dummy;
                        TItemSerializer::Load(context, dummy);
                    } else {
                        TItemSerializer::Load(context, vector[key]);
                    }
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// Possibly unordered collections

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

        size_t size = TSizeSerializer::LoadSuspended(context);

        set.clear();

        SERIALIZATION_DUMP_WRITE(context, "set[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                YCHECK(set.insert(key).second);
            }
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

        size_t size = TSizeSerializer::LoadSuspended(context);

        set.clear();

        SERIALIZATION_DUMP_WRITE(context, "multiset[%v]", size);
        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                set.insert(key);
            }
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
        typedef typename TSet::key_type TKey;

        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "set[%v]", size);

        if (size == 0) {
            set.reset();
            return;
        }

        set.reset(new TSet());

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                SERIALIZATION_DUMP_WRITE(context, "%v =>", index);

                TKey key;
                SERIALIZATION_DUMP_INDENT(context) {
                    TItemSerializer::Load(context, key);
                }

                YCHECK(set->insert(key).second);
            }
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
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "map[%v]", size);

        map.clear();

        SERIALIZATION_DUMP_INDENT(context) {
            for (size_t index = 0; index < size; ++index) {
                typename TMap::key_type key;
                TKeySerializer::Load(context, key);

                SERIALIZATION_DUMP_WRITE(context, "=>");

                typename TMap::mapped_type value;
                SERIALIZATION_DUMP_INDENT(context) {
                    TValueSerializer::Load(context, value);
                }

                YCHECK(map.insert(std::make_pair(key, value)).second);
            }
        }
    }
};

template <
    class TKeySerializer = TDefaultSerializer,
    class TValueSerializer = TDefaultSerializer,
    class TSortTag = TSortedTag
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
        size_t size = TSizeSerializer::LoadSuspended(context);

        SERIALIZATION_DUMP_WRITE(context, "multimap[%v]", size);

        map.clear();

        for (size_t index = 0; index < size; ++index) {
            typename TMap::key_type key;
            TKeySerializer::Load(context, key);

            SERIALIZATION_DUMP_WRITE(context, "=>");

            typename TMap::mapped_type value;
            SERIALIZATION_DUMP_INDENT(context) {
                TValueSerializer::Load(context, value);
            }

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
    typename TEnumTraits<T>::TType
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
    typedef TNullableSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::atomic<T>, C, void>
{
    typedef TAtomicSerializer<> TSerializer;
};

template <class T, class A, class C>
struct TSerializerTraits<std::vector<T, A>, C, void>
{
    typedef TVectorSerializer<> TSerializer;
};

template <class T, unsigned size, class C>
struct TSerializerTraits<SmallVector<T, size>, C, void>
{
    typedef TVectorSerializer<> TSerializer;
};

template <class T, class A, class C>
struct TSerializerTraits<std::list<T, A>, C, void>
{
    typedef TListSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<std::deque<T>, C, void>
{
    typedef TListSerializer<> TSerializer;
};

template <class T, class Q, class A, class C>
struct TSerializerTraits<std::set<T, Q, A>, C, void>
{
    typedef TSetSerializer<> TSerializer;
};
template <class T, class H, class P, class A, class C>
struct TSerializerTraits<std::unordered_set<T, H, P, A>, C, void>
{
    typedef TSetSerializer<> TSerializer;
};

template <class T, class H, class E, class A, class C>
struct TSerializerTraits<yhash_set<T, H, E, A>, C, void>
{
    typedef TSetSerializer<> TSerializer;
};

template <class T, class C>
struct TSerializerTraits<yhash_multiset<T>, C, void>
{
    typedef TMultiSetSerializer<> TSerializer;
};

template <class T, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::list<T, A>>, C, void>
{
    typedef TNullableListSerializer<> TSerializer;
};

template <class T, class Q, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::set<T, Q, A>>, C, void>
{
    typedef TNullableSetSerializer<> TSerializer;
};

template <class T, class H, class P, class A, class C>
struct TSerializerTraits<std::unique_ptr<std::unordered_set<T, H, P, A>>, C, void>
{
    typedef TNullableSetSerializer<> TSerializer;
};

template <class T, class H, class E, class A, class C>
struct TSerializerTraits<std::unique_ptr<yhash_set<T, H, E, A>>, C, void>
{
    typedef TNullableSetSerializer<> TSerializer;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<std::map<K, V, Q, A>, C, void>
{
    typedef TMapSerializer<> TSerializer;
};

template <class K, class V, class H, class P, class A, class C>
struct TSerializerTraits<std::unordered_map<K, V, H, P, A>, C, void>
{
    typedef TMapSerializer<> TSerializer;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<yhash_map<K, V, Q, A>, C, void>
{
    typedef TMapSerializer<> TSerializer;
};

template <class K, class V, class Q, class A, class C>
struct TSerializerTraits<std::multimap<K, V, Q, A>, C, void>
{
    typedef TMultiMapSerializer<> TSerializer;
};

template <class K, class V, class C>
struct TSerializerTraits<yhash_multimap<K, V>, C, void>
{
    typedef TMultiMapSerializer<> TSerializer;
};

template <class T, class E, class C, E Min, E Max>
struct TSerializerTraits<TEnumIndexedVector<T, E, Min, Max>, C, void>
{
    typedef TEnumIndexedVectorSerializer<> TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
