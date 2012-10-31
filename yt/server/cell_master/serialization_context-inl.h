#ifndef SERIALIZATION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include serialization_context.h"
#endif
#undef SERIALIZATION_CONTEXT_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>

#include <server/chunk_server/chunk_tree_ref.h>

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

template <class T>
void SaveObjectRef(TOutputStream* output, T object)
{
    auto id = NObjectServer::GetObjectId(object);
    ::Save(output, id);
}

template <class T>
inline void SetObjectRefImpl(
    const typename NObjectServer::TObjectIdTraits<T*>::TId& id,
    T*& object,
    const TLoadContext& context)
{
    object = id == NObjectServer::NullObjectId ? NULL : context.Get<T>(id);
}

inline void SetObjectRefImpl(
    const NObjectServer::TObjectId& id,
    NChunkServer::TChunkTreeRef& object,
    const TLoadContext& context)
{
    auto type = NObjectClient::TypeFromId(id);
    switch (type) {
        case NObjectClient::EObjectType::Chunk:
            object = context.Get<NChunkServer::TChunk>(id);
            break;
        case NObjectClient::EObjectType::ChunkList:
            object = context.Get<NChunkServer::TChunkList>(id);
            break;
        default:
            YUNREACHABLE();
    }  
}

template <class T>
void LoadObjectRef(TInputStream* input, T& object, const TLoadContext& context)
{
    typename NObjectServer::TObjectIdTraits<T>::TId id;
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
    static void LoadRefs(TInputStream* input, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        auto size = ::LoadSize(input);
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
                return NObjectServer::GetObjectId(lhs) < NObjectServer::GetObjectId(rhs);
            });

        FOREACH (V object, sortedObjects) {
            SaveObjectRef(output, object);
        }
    }

    template <class T>
    static void LoadRefs(TInputStream* input, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        auto size = ::LoadSize(input);
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
    static void LoadRefs(TInputStream* input, T& objects, const TLoadContext& context)
    {
        typedef typename T::value_type V;

        auto size = ::LoadSize(input);
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
                return NObjectServer::GetObjectId(lhs->first) < NObjectServer::GetObjectId(rhs->first);
            });

        FOREACH (const auto& it, sortedIterators) {
            SaveObjectRef(output, it->first);
            Save(output, it->second);
        }
    }

    template <class T>
    static void LoadRefs(TInputStream* input, T& items, const TLoadContext& context)
    {
        typedef typename T::key_type K;
        typedef typename T::mapped_type V;
        typedef typename T::value_type P;

        auto size = ::LoadSize(input);
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
    TSerializer::LoadRefs(input, objects, context);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCellMaster
} // namespace NYT
