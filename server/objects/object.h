#pragma once

#include "persistence.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/small_vector.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EObjectState,
    (Normal)
    (Creating)
    (Created)
    (Removing)
    (Removed)
    (CreatedRemoving)
    (CreatedRemoved)
    (Missing)
);

class TObject
{
public:
    TObject(
        const TObjectId& id,
        const TObjectId& parentId,
        IObjectTypeHandler* typeHandler,
        ISession* session);
    virtual ~TObject() = default;

    static const TScalarAttributeSchema<TObject, TObjectId> IdSchema;
    const TObjectId& GetId() const;
    const TObjectId& GetParentId() const;

    virtual EObjectType GetType() const = 0;

    IObjectTypeHandler* GetTypeHandler() const;
    ISession* GetSession() const;

    void Remove();

    DEFINE_BYVAL_RW_PROPERTY(EObjectState, State, EObjectState::Normal);

    using TAttributeList = SmallVector<IPersistentAttribute*, 16>;
    DEFINE_BYREF_RO_PROPERTY(TAttributeList, Attributes);

    static const TScalarAttributeSchema<TObject, TInstant> CreationTimeSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TInstant>, CreationTime);

    static const TScalarAttributeSchema<TObject, NYT::NYTree::IMapNodePtr> LabelsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<NYT::NYTree::IMapNodePtr>, Labels);

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAnnotationsAttribute, Annotations);

    void ScheduleExists() const;
    bool Exists() const;
    void ValidateExists() const;

    bool RemovalPending() const;

    template <class T>
    void ValidateAs() const;
    template <class T>
    const T* As() const;
    template <class T>
    T* As();

private:
    friend class TAttributeBase;

    const TObjectId Id_;
    IObjectTypeHandler* const TypeHandler_;
    ISession* const Session_;

    TObjectExistenceChecker ExistenceChecker_;
    TParentIdAttribute ParentIdAttribute_;

    void RegisterAttribute(IPersistentAttribute* attribute);
};

////////////////////////////////////////////////////////////////////////////////

TClusterTag ClusterTagFromId(const TTransactionId& id);
TMasterInstanceTag MasterInstanceTagFromId(const TTransactionId& id);
TObjectId GetObjectId(TObject* object);
void ValidateObjectId(EObjectType type, const TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

#define PERSISTENCE_INL_H_
#include "persistence-inl.h"
#undef PERSISTENCE_INL_H_
