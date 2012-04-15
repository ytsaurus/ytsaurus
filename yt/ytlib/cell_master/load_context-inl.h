#ifndef LOAD_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include load_context.h"
#endif
#undef LOAD_CONTEXT_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/chunk_server/chunk_tree_ref.h>

// Some forward declarations.
namespace NYT {

template <class V, unsigned N>
class TSmallVector;

} // namespace NYT

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

// Single object serialization.

template <class T>
void SaveObject(TOutputStream* output, T object)
{
    auto id = NObjectServer::GetObjectId(object);
    ::Save(output, id);
}

template <class T>
void SetObjectImpl(
    const NObjectServer::TObjectId& id,
    T& object,
    const TLoadContext& context);

template <class T>
inline void SetObjectImpl(
    const NObjectServer::TObjectId& id,
    T*& object,
    const TLoadContext& context)
{
    object = id == NObjectServer::NullObjectId ? NULL : context.Get<T>(id);
}

template <>
inline void SetObjectImpl(
    const NObjectServer::TObjectId& id,
    NChunkServer::TChunkTreeRef& object,
    const TLoadContext& context)
{
    auto type = NObjectServer::TypeFromId(id);
    switch (type) {
        case NObjectServer::EObjectType::Chunk:
            object = NChunkServer::TChunkTreeRef(context.Get<NChunkServer::TChunk>(id));
            break;
        case NObjectServer::EObjectType::ChunkList:
            object = NChunkServer::TChunkTreeRef(context.Get<NChunkServer::TChunkList>(id));
            break;
        default:
            YUNREACHABLE();
    }  
}

template <class T>
void LoadObject(TInputStream* input, T& object, const TLoadContext& context)
{
    NObjectServer::TObjectId id;
    ::Load(input, id);
    SetObjectImpl(id, object, context);
}

////////////////////////////////////////////////////////////////////////////////

// Object collection serialization.

struct TObjectVectorSerializer
{
    template <class T>
    static void Save(TOutputStream* output, const T& objects)
    {
        ::SaveSize(output, objects.size());
        FOREACH (typename T::value_type object, objects) {
            auto id = NObjectServer::GetObjectId(object);
            ::Save(output, id);
        }
    }

    template <class T>
    static void Load(TInputStream* input, T& objects, const TLoadContext& context)
    {
        auto size = ::LoadSize(input);
        objects.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            typename T::value_type object;
            LoadObject(input, object, context);
            objects.push_back(object);
        }
    }
};

struct TObjectSetSerializer
{
    template <class T>
    static void Save(TOutputStream* output, const T& objects)
    {
        ::SaveSize(output, objects.size());
        auto iterators = GetSortedIterators(objects);
        FOREACH (const auto it, iterators) {
            const auto& object = *it;
            ::Save(output, NObjectServer::GetObjectId(object));
        }
    }

    template <class T>
    static void Load(TInputStream* input, T& objects, const TLoadContext& context)
    {
        auto size = ::LoadSize(input);
        for (size_t i = 0; i < size; ++i) {
            typename T::value_type object;
            LoadObject(input, object, context);
            YVERIFY(objects.insert(object).second);
        }
    }
};

template <class T>
struct TObjectCollectionSerializerTraits
{ };

template <class V, class A>
struct TObjectCollectionSerializerTraits< std::vector<V, A> >
{
    typedef TObjectVectorSerializer TSerializer;
};

template <class V, unsigned N>
struct TObjectCollectionSerializerTraits< TSmallVector<V, N> >
{
    typedef TObjectVectorSerializer TSerializer;
};

template <class V, class E, class A>
struct TObjectCollectionSerializerTraits< yhash_set<V, E, A> >
{
    typedef TObjectSetSerializer TSerializer;
};


template <class T>
void SaveObjects(TOutputStream* output, const T& objects)
{
    typedef TObjectCollectionSerializerTraits<T>::TSerializer TSerializer;
    TSerializer::Save(output, objects);
}

template <class T>
void LoadObjects(TInputStream* input, T& objects, const TLoadContext& context)
{
    typedef TObjectCollectionSerializerTraits<T>::TSerializer TSerializer;
    TSerializer::Load(input, objects, context);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NCellMaster
} // namespace NYT
