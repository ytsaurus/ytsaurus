#ifndef SERIALIZATION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include serialization_context.h"
#endif
#undef SERIALIZATION_CONTEXT_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>

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
void SaveObjectRef(TOutputStream* output, TObject object)
{
    auto id = GetObjectId(object);
    ::Save(output, id);
}

template <class TId, class TObject>
inline void SetObjectRefImpl(
    TId id,
    TObject*& object,
    const TLoadContext& context)
{
    object = id == TId() ? nullptr : context.Get<TObject>(id);
}

template <class T>
void LoadObjectRef(TInputStream* input, T& object, const TLoadContext& context)
{
    typedef decltype(GetObjectId(object)) TId;
    TId id;
    ::Load(input, id);
    SetObjectRefImpl(id, object, context);
}

////////////////////////////////////////////////////////////////////////////////

// Object ref collection serialization.

struct TObjectRefVectorSerializer
{
    template <class T>
    static void SaveRefs(TOutputStream* output, const T& objects)
    {
        typedef typename T::value_type V;

        ::SaveSize(output, objects.size());
        FOREACH (V object, objects) {
            SaveObjectRef(output, object);
        }
    }

    template <class T>
    static void LoadRefs(TInputStream* input, size_t size, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        objects.clear();
        objects.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(input, object, context);
            objects.push_back(object);
        }
    }
};

struct TObjectRefSetSerializer
{
    template <class T>
    static void SaveRefs(TOutputStream* output, const T& objects)
    {
        typedef typename T::value_type V;

        ::SaveSize(output, objects.size());

        std::vector<V> sortedObjects(objects.begin(), objects.end());
        std::sort(
            sortedObjects.begin(),
            sortedObjects.end(),
            [] (V lhs, V rhs) {
                return CompareObjectsForSerialization(lhs, rhs);
            });

        FOREACH (V object, sortedObjects) {
            SaveObjectRef(output, object);
        }
    }

    template <class T>
    static void LoadRefs(TInputStream* input, size_t size, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        objects.clear();
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(input, object, context);
            YCHECK(objects.insert(object).second);
        }
    }
};

struct TObjectRefMultisetSerializer
{
    template <class T>
    static void SaveRefs(TOutputStream* output, const T& objects)
    {
        TObjectRefSetSerializer::SaveRefs(output, objects);
    }

    template <class T>
    static void LoadRefs(TInputStream* input, size_t size, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        objects.clear();
        for (size_t i = 0; i < size; ++i) {
            V object;
            LoadObjectRef(input, object, context);
            objects.insert(object);
        }
    }
};

struct TObjectRefHashMapSerializer
{
    template <class T>
    static void SaveRefs(TOutputStream* output, const T& items)
    {
        typedef typename T::const_iterator I;

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
                return GetObjectId(lhs->first) < GetObjectId(rhs->first);
            });

        FOREACH (const auto& it, sortedIterators) {
            SaveObjectRef(output, it->first);
            Save(output, it->second);
        }
    }

    template <class T>
    static void LoadRefs(TInputStream* input, size_t size, T& items, const TLoadContext& context)
    {
        typedef typename T::key_type K;
        typedef typename T::mapped_type V;
        typedef typename T::value_type P;

        items.clear();
        for (size_t i = 0; i < size; ++i) {
            K key;
            LoadObjectRef(input, key, context);
            V value;
            Load(input, value);
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
void SaveObjectRefs(TOutputStream* output, const T& objects)
{
    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
    TSerializer::SaveRefs(output, objects);
}

template <class T>
void LoadObjectRefs(TInputStream* input, T& objects, const TLoadContext& context)
{
    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
    size_t size = ::LoadSize(input);
    TSerializer::LoadRefs(input, size, objects, context);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveNullableObjectRefs(TOutputStream* output, const THolder<T>& objects)
{
    if (objects.Get()) {
        SaveObjectRefs(output, *objects);
    } else {
        ::SaveSize(output, 0);
    }
}

template <class T>
void LoadNullableObjectRefs(TInputStream* input, THolder<T>& objects, const TLoadContext& context)
{
    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
    size_t size = ::LoadSize(input);
    if (size == 0) {
        objects.Destroy();
    } else {
        objects.Reset(new T());
        TSerializer::LoadRefs(input, size, *objects, context);
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
    ::Load(context.GetInput(), value);
}

template <class T>
void Save(const TSaveContext& context, const T& value)
{
    ::Save(context.GetOutput(), value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
