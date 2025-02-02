#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/mpl.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/server/master/object_server/object.h>
#include <yt/yt/server/master/object_server/yson_intern_registry.h>

#include <yt/yt/server/master/cypress_server/node.h>
#include <yt/yt/server/master/cypress_server/serialize.h>

#include <yt/yt/server/master/table_server/master_table_schema.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
[[nodiscard]]
T* MaybeVerifyAndUnwrapRawObjectPtrOnSerialization(NObjectServer::TRawObjectPtr<T> ptr)
{
#if defined(YT_ROPSAN_ENABLE_SERIALIZATION_CHECK) && !defined(YT_ROPSAN_ENABLE_ACCESS_CHECK)
    ptr.VerifyRopSanTag();
#else
    // NB: If YT_ROPSAN_ENABLE_ACCESS_CHECK is defined then Get() will do the job.
#endif
    return ptr.Get();
}

} // namespace NDetail

struct TRawNonversionedObjectPtrSerializer
{
    static constexpr TEntitySerializationKey DestroyedKey = TEntitySerializationKey(-2);

    template <class T>
    static void Save(NCellMaster::TSaveContext& context, const T* object)
    {
        if (object) {
            // Zombies are serialized as usual, but ghosts need special treatment.
            if (object->IsGhost()) {
                // Ephemeral ghosts aren't supposed to be a part of the
                // persistent state. Weak ghosts are.
                YT_VERIFY(object->GetObjectWeakRefCounter() > 0);
                NYT::Save(context, DestroyedKey);
                NYT::Save(context, object->GetId());
            } else {
                auto key = object->GetDynamicData()->SerializationKey;
                YT_ASSERT(key != NullEntitySerializationKey);
                NYT::Save(context, key);
            }
        } else {
            NYT::Save(context, NullEntitySerializationKey);
        }
    }

    template <class T>
    static void Save(NCellMaster::TSaveContext& context, NObjectServer::TRawObjectPtr<T> object)
    {
        Save(context, NDetail::MaybeVerifyAndUnwrapRawObjectPtrOnSerialization(object));
    }

    template <class T>
    static void Save(NCypressServer::TSerializeNodeContext& context, const T* object)
    {
        using NYT::Save;
        if (object && !NObjectServer::IsObjectAlive(object)) {
            THROW_ERROR_EXCEPTION("Object %v is not alive",
                object->GetId());
        }
        Save(context, GetObjectId(object));
    }

    template <class T>
    static void Save(NCypressServer::TSerializeNodeContext& context, NObjectServer::TRawObjectPtr<T> object)
    {
        Save(context, NDetail::MaybeVerifyAndUnwrapRawObjectPtrOnSerialization(object));
    }

    template <class T>
    static void Load(NCellMaster::TLoadContext& context, T*& object)
    {
        auto key = LoadSuspended<TEntitySerializationKey>(context);
        if (key == NullEntitySerializationKey) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else if (key == DestroyedKey) {
            auto objectId = LoadSuspended<NObjectServer::TObjectId>(context);
            object = context.GetWeakGhostObject(objectId)->template As<T>();
            SERIALIZATION_DUMP_WRITE(context, "objref %v <destroyed>", objectId);
        } else {
            object = context.template GetRawEntity<T>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetId(), key);
        }
    }

    template <class T>
    static void Load(NCellMaster::TLoadContext& context, NObjectServer::TRawObjectPtr<T>& object)
    {
        object = LoadWith<TRawNonversionedObjectPtrSerializer, T*>(context);
    }

    template <class T>
    static void Load(NCypressServer::TMaterializeNodeContext& context, T*& object)
    {
        using NYT::Load;
        auto id = Load<NObjectServer::TObjectId>(context);
        object = id
            ? context.template GetObject<T>(id)
            : nullptr;
    }

    template <class T>
    static void Load(NCypressServer::TMaterializeNodeContext& context, NObjectServer::TRawObjectPtr<T>& object)
    {
        object = LoadWith<TRawNonversionedObjectPtrSerializer, T*>(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNonversionedObjectPtrSerializer
{
    template <class C1, class C2, class T>
    static void Save(C1& context, const NObjectServer::TObjectPtr<T, C2>& object)
    {
        SaveWith<TRawNonversionedObjectPtrSerializer>(context, object.GetUnsafe());
    }

    template <class C1, class C2, class T>
    static void Load(C1& context, NObjectServer::TObjectPtr<T, C2>& object)
    {
        auto* rawObject = LoadWith<TRawNonversionedObjectPtrSerializer, T*>(context);
        object.Assign(rawObject, NObjectServer::TObjectPtrLoadTag());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRawVersionedObjectPtrSerializer
{
    template <class T, class C>
    static void Save(C& context, const T* object)
    {
        auto key = object
            ? object->GetDynamicData()->SerializationKey
            : NullEntitySerializationKey;
        NYT::Save(context, key);
    }

    template <class T, class C>
    static void Save(C& context, NObjectServer::TRawObjectPtr<T> object)
    {
        Save(context, NDetail::MaybeVerifyAndUnwrapRawObjectPtrOnSerialization(object));
    }

    template <class T, class C>
    static void Load(C& context, T*& object)
    {
        auto key = NYT::Load<TEntitySerializationKey>(context);
        if (key == NullEntitySerializationKey) {
            object = nullptr;
            SERIALIZATION_DUMP_WRITE(context, "objref <null>");
        } else {
            object = context.template GetRawEntity<T>(key);
            SERIALIZATION_DUMP_WRITE(context, "objref %v aka %v", object->GetVersionedId(), key);
        }
    }

    template <class T, class C>
    static void Load(C& context, NObjectServer::TRawObjectPtr<T>& object)
    {
        object = LoadWith<TRawVersionedObjectPtrSerializer, T*>(context);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStrongVersionedObjectPtrSerializer
{
    template <class C, class T>
    static void Save(C& context, const NObjectServer::TStrongObjectPtr<T>& object)
    {
        SaveWith<TRawVersionedObjectPtrSerializer>(context, object.GetUnsafe());
    }

    template <class C, class T>
    static void Load(C& context, NObjectServer::TStrongObjectPtr<T>& object)
    {
        auto* rawObject = LoadWith<TRawVersionedObjectPtrSerializer, T*>(context);
        object.Assign(rawObject, NObjectServer::TObjectPtrLoadTag());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TInternedYsonStringSerializer
{
    static constexpr TEntitySerializationKey UninternedKey = TEntitySerializationKey(-4);

    template <class C>
    static void Save(C& context, const NYson::TYsonString& str)
    {
        using NYT::Save;

        if (!str || str.AsStringBuf().length() < NObjectServer::DefaultYsonStringInternLengthThreshold) {
            Save(context, UninternedKey);
            Save(context, str);
            return;
        }

        auto key = context.RegisterInternedYsonString(str);
        Save(context, key);
        if (key == TEntityStreamSaveContext::InlineKey) {
            Save(context, str);
        }
    }

    template <class C>
    static void Load(C& context, NYson::TYsonString& str)
    {
        using NYT::Load;

        auto key = LoadSuspended<TEntitySerializationKey>(context);
        if (key == UninternedKey) {
            Load(context, str);
        } else if (key == TEntityStreamSaveContext::InlineKey) {
            SERIALIZATION_DUMP_INDENT(context) {
                auto loadedStr = Load<NYson::TYsonString>(context);
                const auto& ysonInternRegistry = context.GetBootstrap()->GetYsonInternRegistry();
                str = ysonInternRegistry->Intern(std::move(loadedStr));
                auto loadedKey = context.RegisterInternedYsonString(str);
                SERIALIZATION_DUMP_WRITE(context, "ysonref %v", loadedKey);
            }
        } else {
            str = context.GetInternedYsonString(key);
            SERIALIZATION_DUMP_WRITE(context, "ysonref %v", key);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMasterTableSchemaRefSerializer
{
    static void Save(NCellMaster::TSaveContext& context, NTableServer::TMasterTableSchemaRawPtr schema)
    {
        SaveWith<TRawNonversionedObjectPtrSerializer>(context, schema);
    }

    static void Load(NCellMaster::TLoadContext& context, NTableServer::TMasterTableSchemaRawPtr& schema)
    {
        LoadWith<TRawNonversionedObjectPtrSerializer>(context, schema);
    }

    static void Save(NCypressServer::TSerializeNodeContext& context, NTableServer::TMasterTableSchemaRawPtr schema)
    {
        YT_VERIFY(schema);

        // NB: Each SerializeNodeContext contains a single node,
        // thus no more than one schema should be saved.
        YT_VERIFY(!context.GetSchemaId());

        auto id = schema->GetId();
        context.RegisterSchema(id);
    }

    static void Load(NCypressServer::TMaterializeNodeContext& context, NTableServer::TMasterTableSchemaRawPtr& schema)
    {
        // NB: See comment in Save.
        schema = context.GetSchema();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
    requires
        (!std::derived_from<T, NCypressServer::TCypressNode>) &&
        (!std::is_same_v<T, NTableServer::TMasterTableSchema>)
struct TSerializerTraits<NObjectServer::TRawObjectPtr<T>, C>
{
    using TSerializer = NCellMaster::TRawNonversionedObjectPtrSerializer;
    using TComparer = NObjectServer::TObjectIdComparer;
};

template <class T, class C>
    requires
        std::derived_from<T, NCypressServer::TCypressNode>
struct TSerializerTraits<NObjectServer::TRawObjectPtr<T>, C>
{
    using TSerializer = NCellMaster::TRawVersionedObjectPtrSerializer;
    using TComparer = NCypressServer::TCypressNodeIdComparer;
};

template <class T, class C1, class C2>
    requires
        (!std::derived_from<T, NCypressServer::TCypressNode>)
struct TSerializerTraits<NObjectServer::TObjectPtr<T, C1>, C2>
{
    using TSerializer = NCellMaster::TNonversionedObjectPtrSerializer;
    using TComparer = NObjectServer::TObjectIdComparer;
};

template <class T, class C>
    requires std::derived_from<T, NCypressServer::TCypressNode>
struct TSerializerTraits<NObjectServer::TStrongObjectPtr<T>, C>
{
    using TSerializer = NCellMaster::TStrongVersionedObjectPtrSerializer;
    using TComparer = NCypressServer::TCypressNodeIdComparer;
};

// Unlike most (non-versioned) objects, schemas are cell-local, which necessitates
// special handling for cross-cell copying.
template <class C>
struct TSerializerTraits<NTableServer::TMasterTableSchemaRawPtr, C>
{
    using TSerializer = NCellMaster::TMasterTableSchemaRefSerializer;
    using TComparer = NObjectServer::TObjectIdComparer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

