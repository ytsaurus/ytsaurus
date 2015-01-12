#ifndef SERIALIZATION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include serialization_context.h"
#endif
#undef SERIALIZATION_CONTEXT_INL_H_

#include <core/misc/serialize.h>
#include <core/misc/small_vector.h>

#include <server/object_server/object.h>

#include <server/cypress_server/node.h>

#include <server/node_tracker_server/node.h>
#include <server/node_tracker_server/config.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_replica.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// Single object ref serialization.

template <class TObject>
void SaveObjectRef(TSaveContext& context, TObject object)
{
    NYT::Save(context, GetObjectId(object));
}

template <class TId, class TObject>
inline void SetObjectRefImpl(
    TLoadContext& context,
    TId id,
    TObject*& object)
{
    object = id == TId() ? nullptr : context.Get<TObject>(id);
}

template <class T>
void LoadObjectRef(TLoadContext& context, T& object)
{
    // XXX(babenko): no idea why this is needed but ADL just does not work.
    using NNodeTrackerServer::GetObjectId;
    using NObjectServer::GetObjectId;
    using NCypressServer::GetObjectId;
    typedef decltype(GetObjectId(object)) TId;

    TId id;
    NYT::Load(context, id);
    SetObjectRefImpl(context, id, object);
}

////////////////////////////////////////////////////////////////////////////////

// Object ref collection serialization.

struct TObjectRefVectorSerializer
{
    template <class T>
    static void SaveRefs(TSaveContext& context, const T& objects)
    {
        typedef typename T::value_type V;

        TSizeSerializer::Save(context, objects.size());
        FOREACH (V object, objects) {
            SaveObjectRef(context, object);
        }
    }

    template <class T>
    static void LoadRefs(TLoadContext& context, size_t size, T& objects)
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

struct TObjectRefListSerializer
{
    template <class T>
    static void SaveRefs(TSaveContext& context, const T& objects)
    {
        typedef typename T::value_type V;

        TSizeSerializer::Save(context, objects.size());
        FOREACH (V object, objects) {
            SaveObjectRef(context, object);
        }
    }

    template <class T>
    static void LoadRefs(TLoadContext& context, size_t size, T& objects)
    {
        typedef typename T::value_type V;

        objects.clear();
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
    static void SaveRefs(TSaveContext& context, const T& objects)
    {
        typedef typename T::value_type V;

        TSizeSerializer::Save(context, objects.size());

        std::vector<V> sortedObjects(objects.begin(), objects.end());
        std::sort(
            sortedObjects.begin(),
            sortedObjects.end(),
            [] (const V& lhs, const V& rhs) {
                return CompareObjectsForSerialization(lhs, rhs);
            });

        FOREACH (const V& object, sortedObjects) {
            SaveObjectRef(context, object);
        }
    }

    template <class T>
    static void LoadRefs(TLoadContext& context, size_t size, T& objects)
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
    static void SaveRefs(TSaveContext& context, const T& objects)
    {
        TObjectRefSetSerializer::SaveRefs(context, objects);
    }

    template <class T>
    static void LoadRefs(TLoadContext& context, size_t size, T& objects)
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
    static void SaveRefs(TSaveContext& context, const T& items)
    {
        typedef typename T::const_iterator I;

        TSizeSerializer::Save(context, items.size());

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
    static void LoadRefs(TLoadContext& context, size_t size, T& items)
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

template <class K, class A>
struct TObjectRefSerializerTraits< std::list<K, A> >
{
    typedef TObjectRefListSerializer TSerializer;
};

template <class K, class C, class A>
struct TObjectRefSerializerTraits< std::set<K, C, A> >
{
    typedef TObjectRefSetSerializer TSerializer;
};

template <class K, class H, class E, class A>
struct TObjectRefSerializerTraits< std::unordered_set<K, H, E, A> >
{
    typedef TObjectRefSetSerializer TSerializer;
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
void SaveObjectRefs(TSaveContext& context, const T& objects)
{
    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
    TSerializer::SaveRefs(context, objects);
}

template <class T>
void LoadObjectRefs(TLoadContext& context, T& objects)
{
    size_t size = TSizeSerializer::Load(context);

    typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;

    TSerializer::LoadRefs(context, size, objects);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveNullableObjectRefs(TSaveContext& context, const std::unique_ptr<T>& objects)
{
    if (objects) {
        SaveObjectRefs(context, *objects);
    } else {
        TSizeSerializer::Save(context, 0);
    }
}

template <class T>
void LoadNullableObjectRefs(TLoadContext& context, std::unique_ptr<T>& objects)
{
    size_t size = TSizeSerializer::Load(context);
    if (size == 0) {
        objects.reset();
    } else {
        objects.reset(new T());
        typedef typename TObjectRefSerializerTraits<T>::TSerializer TSerializer;
        TSerializer::LoadRefs(context, size, *objects);
    }
}
            
////////////////////////////////////////////////////////////////////////////////

template <class T>
void SaveObjectRef(TSaveContext& context, NChunkServer::TPtrWithIndex<T> value)
{
    SaveObjectRef(context, value.GetPtr());
    NYT::Save(context, value.GetIndex());
}

template <class T>
void LoadObjectRef(NCellMaster::TLoadContext& context, NChunkServer::TPtrWithIndex<T>& value)
{
    T* ptr;
    LoadObjectRef(context, ptr);

    int index;
    NYT::Load(context, index);

    value = NChunkServer::TPtrWithIndex<T>(ptr, index);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
