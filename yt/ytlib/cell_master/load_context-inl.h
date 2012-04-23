#ifndef LOAD_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include load_context.h"
#endif
#undef LOAD_CONTEXT_INL_H_

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/chunk_server/chunk_tree_ref.h>
#include <ytlib/cypress/node.h>

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
void SetObjectRefImpl(
    const NObjectServer::TObjectId& id,
    T& object,
    const TLoadContext& context);

template <class T>
inline void SetObjectRefImpl(
    const typename NObjectServer::TObjectIdTraits<T*>::TId& id,
    T*& object,
    const TLoadContext& context)
{
    object = id == NObjectServer::NullObjectId ? NULL : context.Get<T>(id);
}

template <>
inline void SetObjectRefImpl(
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
            YVERIFY(objects.insert(object).second);
        }
    }
};

template <class T>
struct TObjectRefSerializerTraits
{ };

template <class V, class A>
struct TObjectRefSerializerTraits< std::vector<V, A> >
{
    typedef TObjectRefVectorSerializer TSerializer;
};

template <class V, unsigned N>
struct TObjectRefSerializerTraits< TSmallVector<V, N> >
{
    typedef TObjectRefVectorSerializer TSerializer;
};

template <class V, class E, class A>
struct TObjectRefSerializerTraits< yhash_set<V, E, A> >
{
    typedef TObjectRefSetSerializer TSerializer;
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
