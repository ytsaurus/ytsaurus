#pragma once
#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
#endif

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/mpl.h>

#include <yt/server/object_server/object.h>
#include <yt/server/object_server/public.h>

#include <yt/server/cypress_server/node.h>

#include <yt/server/node_tracker_server/node.h>

#include <yt/server/chunk_server/chunk.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct TNonversionedObjectRefSerializer
{
    template <class T, class C>
    static void Save(C& context, T object)
    {
        if (object) {
            // Zombies are serialized as usual, but ghosts need special treatment.
            if (object->IsDestroyed()) {
                // Ephemeral ghosts aren't supposed to be a part of the
                // persistent state. Weak ghosts are.
                YCHECK(object->GetObjectWeakRefCounter() > 0);
                auto key = NHydra::TEntitySerializationKey::DestroyedObjectKey();
                NYT::Save(context, key);
                NYT::Save(context, object->GetId());
            } else {
                auto key = object->GetDynamicData()->SerializationKey;
                Y_ASSERT(key != NHydra::TEntitySerializationKey::NullObjectKey());
                NYT::Save(context, key);
            }
        } else {
            NYT::Save(context, NHydra::TEntitySerializationKey::NullObjectKey());
        }
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = LoadSuspended<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey::NullObjectKey()) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else if (key == NHydra::TEntitySerializationKey::DestroyedObjectKey()) {
            auto objectId = LoadSuspended<NObjectServer::TObjectId>(context);
            object = context.GetWeakGhostObject(objectId)->template As<TObject>();
            SERIALIZATION_DUMP_WRITE(context, "objref %v <destroyed>", objectId);
        } else {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetId(), key.Index);
        }
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
            : NHydra::TEntitySerializationKey::NullObjectKey();
        NYT::Save(context, key);
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = NYT::Load<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey::NullObjectKey()) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetVersionedId(), key.Index);
        }
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
            NMpl::TIsConvertible<T, const NObjectServer::TObjectBase*>::Value,
            NMpl::TNotC<
                NMpl::TIsConvertible<T, const NCypressServer::TCypressNodeBase*>::Value
            >::Value
        >::Value
    >::TType
>
{
    typedef NCellMaster::TNonversionedObjectRefSerializer TSerializer;
    typedef NObjectServer::TObjectRefComparer TComparer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T, const NCypressServer::TCypressNodeBase*>
    >::TType
>
{
    typedef NCellMaster::TVersionedObjectRefSerializer TSerializer;
    typedef NCypressServer::TCypressNodeRefComparer TComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

