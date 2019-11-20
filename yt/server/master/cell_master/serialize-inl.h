#pragma once
#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/mpl.h>

#include <yt/server/master/object_server/object.h>
#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cypress_server/node.h>
#include <yt/server/master/cypress_server/serialize.h>

#include <yt/server/master/node_tracker_server/node.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct TNonversionedObjectRefSerializer
{
    static inline const TEntitySerializationKey DestroyedKey = TEntitySerializationKey(-2);

    template <class T>
    static void Save(NCellMaster::TSaveContext& context, T object)
    {
        if (object) {
            // Zombies are serialized as usual, but ghosts need special treatment.
            if (object->IsDestroyed()) {
                // Ephemeral ghosts aren't supposed to be a part of the
                // persistent state. Weak ghosts are.
                YT_VERIFY(object->GetObjectWeakRefCounter() > 0);
                NYT::Save(context, DestroyedKey);
                NYT::Save(context, object->GetId());
            } else {
                auto key = object->GetDynamicData()->SerializationKey;
                YT_ASSERT(key);
                NYT::Save(context, key);
            }
        } else {
            NYT::Save(context, TEntitySerializationKey());
        }
    }

    template <class T>
    static void Save(NCypressServer::TBeginCopyContext& context, T object)
    {
        using NYT::Save;
        if (object && !NObjectServer::IsObjectAlive(object)) {
            THROW_ERROR_EXCEPTION("Object %v is not alive",
                object->GetId());
        }
        Save(context, GetObjectId(object));
    }

    template <class T>
    static void Load(NCellMaster::TLoadContext& context, T& object)
    {
        using TObject = typename std::remove_pointer<T>::type;
        auto key = LoadSuspended<TEntitySerializationKey>(context);
        if (!key) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else if (key == DestroyedKey) {
            auto objectId = LoadSuspended<NObjectServer::TObjectId>(context);
            object = context.GetWeakGhostObject(objectId)->template As<TObject>();
            SERIALIZATION_DUMP_WRITE(context, "objref %v <destroyed>", objectId);
        } else {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetId(), key.Index);
        }
    }

    template <class T>
    static void Load(NCypressServer::TEndCopyContext& context, T& object)
    {
        using NYT::Load;
        using TObject = typename std::remove_pointer<T>::type;
        auto id = Load<NObjectServer::TObjectId>(context);
        object = id
            ? context.template GetObject<TObject>(id)
            : nullptr;
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
            : TEntitySerializationKey();
        NYT::Save(context, key);
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = NYT::Load<TEntitySerializationKey>(context);
        if (key) {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetVersionedId(), key.Index);
        } else {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIfC<
        NMpl::TAndC<
            NMpl::TIsConvertible<T, const NObjectServer::TObject*>::Value,
            NMpl::TNotC<
                NMpl::TIsConvertible<T, const NCypressServer::TCypressNode*>::Value
            >::Value
        >::Value
    >::TType
>
{
    using TSerializer = NCellMaster::TNonversionedObjectRefSerializer;
    using TComparer = NObjectServer::TObjectRefComparer;
};

template <class T, class C>
struct TSerializerTraits<
    T,
    C,
    typename NMpl::TEnableIf<
        NMpl::TIsConvertible<T, const NCypressServer::TCypressNode*>
    >::TType
>
{
    using TSerializer = NCellMaster::TVersionedObjectRefSerializer;
    using TComparer = NCypressServer::TCypressNodeRefComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

