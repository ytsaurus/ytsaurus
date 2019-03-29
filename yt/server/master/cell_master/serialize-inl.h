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

#include <yt/server/master/node_tracker_server/node.h>

#include <yt/server/master/chunk_server/chunk.h>

namespace NYT::NCellMaster {

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
                auto key = NHydra::TEntitySerializationKey::Destroyed;
                NYT::Save(context, key);
                NYT::Save(context, object->GetId());
            } else {
                auto key = object->GetDynamicData()->SerializationKey;
                Y_ASSERT(key != NHydra::TEntitySerializationKey::Null);
                NYT::Save(context, key);
            }
        } else {
            NYT::Save(context, NHydra::TEntitySerializationKey::Null);
        }
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = LoadSuspended<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey::Null) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else if (key == NHydra::TEntitySerializationKey::Destroyed) {
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
            : NHydra::TEntitySerializationKey::Null;
        NYT::Save(context, key);
    }

    template <class T, class C>
    static void Load(C& context, T& object)
    {
        typedef typename std::remove_pointer<T>::type TObject;
        auto key = NYT::Load<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey::Null) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else {
            object = context.template GetEntity<TObject>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetVersionedId(), key.Index);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TInternedObjectSerializer
{
    template <class T, class C>
    static void Save(C& context, const TInternedObject<T>& object)
    {
        using NYT::Save;

        auto it = context.SavedInternedObjects().find(object.ToRaw());
        if (it == context.SavedInternedObjects().end()) {
            Save(context, NHydra::TEntitySerializationKey::Inline);
            Save(context, *object);
            YCHECK(context.SavedInternedObjects().emplace(object.ToRaw(), context.GenerateSerializationKey()).second);
        } else {
            Save(context, it->second);
        }
    }

    template <class T, class C>
    static void Load(C& context, TInternedObject<T>& object)
    {
        using NYT::Load;

        auto key = NYT::LoadSuspended<NHydra::TEntitySerializationKey>(context);
        if (key == NHydra::TEntitySerializationKey::Inline) {
            T value;
            SERIALIZATION_DUMP_INDENT(context) {
                Load(context, value);
                const auto& registry = context.template GetInternRegistry<T>();
                object = registry->Intern(std::move(value));
                auto key = context.RegisterEntity(object.ToRaw());
                SERIALIZATION_DUMP_WRITE(context, "objref %v", key.Index);
            }
        } else {
            object = TInternedObject<T>::FromRaw(context.template GetEntity<void*>(key));
            SERIALIZATION_DUMP_WRITE(context, "objref %v", key.Index);
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
            NMpl::TIsConvertible<T, const NObjectServer::TObjectBase*>::Value,
            NMpl::TNotC<
                NMpl::TIsConvertible<T, const NCypressServer::TCypressNodeBase*>::Value
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
        NMpl::TIsConvertible<T, const NCypressServer::TCypressNodeBase*>
    >::TType
>
{
    using TSerializer = NCellMaster::TVersionedObjectRefSerializer;
    using TComparer = NCypressServer::TCypressNodeRefComparer;
};

template <class T, class C>
struct TSerializerTraits<
    TInternedObject<T>,
    C,
    void
>
{
    using TSerializer = NCellMaster::TInternedObjectSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

