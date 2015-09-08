#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif

#include <core/misc/serialize.h>
#include <core/misc/mpl.h>

#include <server/object_server/object.h>

#include <server/cypress_server/node.h>

#include <server/node_tracker_server/node.h>

#include <server/chunk_server/chunk.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct TNonversionedObjectRefSerializer
{
    template <class T, class C>
    static void Save(C& context, T object)
    {
        if (object) {
            auto key = object->GetDynamicData()->SerializationKey;
            YASSERT(key != NHydra::TEntitySerializationKey());
            NYT::Save(context, key);
        } else {
            NYT::Save(context, NHydra::TEntitySerializationKey());
        }
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = LoadSuspended<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey()) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetId(), key.Index);
        }
    }
};

struct TNonversionedObjectRefComparer
{
    template <class T>
    static bool Compare(T* lhs, T* rhs)
    {
        return lhs->GetId() < rhs->GetId();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVersionedObjectRefSerializer
{
    template <class T, class C>
    static void Save(C& context, T object)
    {
        auto key = object
            ? object->GetDynamicData()->SerializationKey
            : NHydra::TEntitySerializationKey();
        NYT::Save(context, key);
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = NYT::Load<NHydra::TEntitySerializationKey>(context);
        object  = (key == NHydra::TEntitySerializationKey())
            ? nullptr
            : context.template GetEntity<TObject>(key);
    }
};

struct TVersionedObjectRefComparer
{
    template <class T>
    static bool Compare(T* lhs, T* rhs)
    {
        return lhs->GetVersionedId() < rhs->GetVersionedId();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC <
        NMpl::TAndC<
            NMpl::TIsConvertible<T, NObjectServer::TObjectBase*>::Value,
            NMpl::TNotC<
                NMpl::TIsConvertible<T, NCypressServer::TCypressNodeBase*>::Value
            >::Value
        >::Value
    >::TType
>
{
    typedef NCellMaster::TNonversionedObjectRefSerializer TSerializer;
    typedef NCellMaster::TNonversionedObjectRefComparer TComparer;
};

template <class C>
struct TSerializerTraits<NNodeTrackerServer::TNode*, C>
{
    typedef NCellMaster::TNonversionedObjectRefSerializer TSerializer;
    typedef NCellMaster::TNonversionedObjectRefComparer TComparer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T, NCypressServer::TCypressNodeBase*>
    >::TType
>
{
    typedef NCellMaster::TVersionedObjectRefSerializer TSerializer;
    typedef NCellMaster::TVersionedObjectRefComparer TComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

