#ifndef SERIALIZATION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include serialization_context.h"
#endif
#undef SERIALIZATION_CONTEXT_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/misc/small_vector.h>

#include <server/object_server/object.h>

#include <server/cypress_server/node.h>

// Some forward declarations.
namespace NYT {

template <class V, unsigned N>
class TSmallVector;

} // namespace NYT

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// Single object ref serialization.

template <class TObject>
void SaveObjectRef(const TSaveContext& context, TObject object)
{
	auto* output = context.GetOutput();
    auto id = GetObjectId(object);
    ::Save(output, id);
}

template <class TId, class TObject>
inline void SetObjectRefImpl(
    const TLoadContext& context,
    TId id,
    TObject*& object)
{
    object = id == TId() ? nullptr : context.Get<TObject>(id);
}

template <class T>
void LoadObjectRef(const TLoadContext& context, T& object)
{
    typedef decltype(GetObjectId(object)) TId;
    TId id;
    auto* input = context.GetInput();
    ::Load(input, id);
    SetObjectRefImpl(context, id, object);
}

////////////////////////////////////////////////////////////////////////////////

// Object ref collection serialization.

struct TObjectRefVectorSerializer
{
    template <class T>
    static void SaveRefs(const TSaveContext& context, const T& objects)
    {
        typedef typename T::value_type V;

        auto* output = context.GetOutput();
        ::SaveSize(output, objects.size());
        FOREACH (V object, objects) {
            SaveObjectRef(context, object);
        }
    }

    template <class T>
    static void LoadRefs(const TLoadContext& context, size_t size, T& objects)
    {
        typedef typename T::value_type V;

        objects.clear();
        objects.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(context, object);
            objects.push_back(object);
        }
    }
};

struct TObjectRefSetSerializer
{
    template <class T>
    static void SaveRefs(const TSaveContext& context, const T& objects)
    {
        typedef typename T::value_type V;

        auto* output = context.GetOutput();
        ::SaveSize(output, objects.size());

        std::vector<V> sortedObjects(objects.begin(), objects.end());
        std::sort(
            sortedObjects.begin(),
            sortedObjects.end(),
            [] (V lhs, V rhs) {
                return CompareObjectsForSerialization(lhs, rhs);
            });

        FOREACH (V object, sortedObjects) {
            SaveObjectRef(context, object);
        }
    }

    template <class T>
    static void LoadRefs(const TLoadContext& context, size_t size, T& objects)
    {
        typedef typename T::value_type V;

        objects.clear();
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(context, object);
            YCHECK(objects.insert(object).second);
        }
    }
};

struct TObjectRefMultisetSerializer
{
    template <class T>
    static void SaveRefs(const TSaveContext& context, const T& objects)
    {
        TObjectRefSetSerializer::SaveRefs(context, objects);
    }

    template <class T>
    static void LoadRefs(const TLoadContext& context, size_t size, T& objects)
    {
        typedef typename T::value_type V;

        objects.clear();
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(context, object);
            objects.insert(object);
        }
    }
};

struct TObjectRefHashMapSerializer
{
    template <class T>
    static void SaveRefs(const TSaveContext& context, const T& items)
    {
        typedef typename T::const_iterator I;

        auto* output = context.GetOutput();
        ::SaveSize(output, items.size());

        std::vector<I> sortedIterators;
        sortedIterators.reserve(items.size());
        for (auto it = items.begin(); it != items.end(); ++it) {
            sortedIterators.push_back(it);
        }

        std::sort(
            sortedIterators.begin(),
            sortedIterators.end(),
            [] (const I& lhs, const I& rhs) {
                return CompareObjectsForSerialization(lhs->first, rhs->first);
            });

        for (int index = 0; index < static_cast<int>(sortedIterators.size()) - 1; ++index) {
            YCHECK(CompareObjectsForSerialization(sortedIterators[index]->first, sortedIterators[index + 1]->first));
        }

        FOREACH (const auto& it, sortedIterators) {
            SaveObjectRef(context, it->first);
            Save(context, it->second);
        }
    }

    template <class T>
    static void LoadRefs(const TLoadContext& context, size_t size, T& items)
    {
        typedef typename T::key_type K;
        typedef typename T::mapped_type V;
        typedef typename T::value_type P;

        items.clear();
        for (size_t i = 0; i < size; ++i) {
            K key;
            LoadObjectRef(context, key);
            V value;
            Load(context, value);
            YCHECK(items.insert(std::make_pair(key, value)).second);
        }
    }
};

template <class T>
struct TObjectRefSerializerTraits
{ };

template <class K, class A>
struct TObjectRefSerializerTraits< std::vector<K, A> >
{
    typedef TObjectRefVectorSerializer TSerializer;
};

template <class V, unsigned N>
struct TObjectRefSerializerTraits< TSmallVector<V, N> >
{
    typedef TObjectRefVectorSerializer TSerializer;
};

template <class K, class H, class E, class A>
struct TObjectRefSerializerTraits< yhash_set<K, H, E, A> >
{
    typedef TObjectRefSetSerializer TSerializer;
};

template <class K, class H, class E, class A>
struct TObjectRefSerializerTraits< yhash_multiset<K, H, E, A> >
{
    typedef TObjectRefMultisetSerializer TSerializer;
};

template <class K, class V, class H, class E, class A>
struct TObjectRefSerializerTraits< yhash_map<K, V, H, E, A> >
{
    typedef TObjectRefHashMapSerializer TSerializer;
};

template <class T>
void SaveObjectRefs(const TSaveContext& context, const T& objects)
{
    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
    TSerializer::SaveRefs(context, objects);
}

template <class T>
void LoadObjectRefs(const TLoadContext& context, T& objects)
{
    auto* input = context.GetInput();
    size_t size = ::LoadSize(input);

    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;

    TSerializer::LoadRefs(context, size, objects);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveNullableObjectRefs(const TSaveContext& context, const THolder<T>& objects)
{
    if (objects.Get()) {
        SaveObjectRefs(context, *objects);
    } else {
    	auto* output = context.GetOutput();
        ::SaveSize(output, 0);
    }
}

template <class T>
void LoadNullableObjectRefs(const TLoadContext& context, THolder<T>& objects)
{
	auto* input = context.GetInput();
    size_t size = ::LoadSize(input);
    if (size == 0) {
        objects.Destroy();
    } else {
        objects.Reset(new T());
	    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
        TSerializer::LoadRefs(context, size, *objects);
    }
}
            
////////////////////////////////////////////////////////////////////////////////

template <class T>
T Load(const TLoadContext& context)
{
    T value;
    Load(context, value);
    return value;
}

template <class T>
void Load(const TLoadContext& context, T& value)
{
    Load(context.GetInput(), value);
}

template <class T>
void Save(const TSaveContext& context, const T& value)
{
    Save(context.GetOutput(), value);
}

template <class T>
void Save(const TSaveContext& context, const std::vector<T>& objects)
{
    auto* output = context.GetOutput();
    ::SaveSize(output, objects.size());

    FOREACH (const auto& object, objects) {
        Save(context, object);
    }
}

template <class T>
void Load(const TLoadContext& context, std::vector<T>& objects)
{
    auto* input = context.GetInput();
    size_t size = ::LoadSize(input);

    objects.resize(size);
    for (size_t index = 0; index != size; ++index) {
        Load(context, objects[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): merge this with the above
template <class T, unsigned N>
void Save(const TSaveContext& context, const TSmallVector<T, N>& objects)
{
    auto* output = context.GetOutput();
    ::SaveSize(output, objects.size());

    FOREACH (const auto& object, objects) {
        Save(context, object);
    }
}

// TODO(babenko): merge this with the above
template <class T, unsigned N>
void Load(const TLoadContext& context, TSmallVector<T, N>& objects)
{
    auto* input = context.GetInput();
    size_t size = ::LoadSize(input);

    objects.resize(size);
    for (size_t index = 0; index != size; ++index) {
        Load(context, objects[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
