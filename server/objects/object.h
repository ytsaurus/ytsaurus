#pragma once

#include "persistence.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yp/server/objects/proto/objects.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/small_vector.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObject
{
public:
    TObject(
        const TObjectId& id,
        const TObjectId& parentId,
        IObjectTypeHandler* typeHandler,
        ISession* session);
    virtual ~TObject() = default;

    void InitializeCreating();
    void InitializeInstantiated();

    const TObjectId& GetId() const;
    const TObjectId& GetParentId() const;

    virtual EObjectType GetType() const = 0;

    IObjectTypeHandler* GetTypeHandler() const;
    ISession* GetSession() const;

    void Remove();

    DEFINE_BYVAL_RW_PROPERTY(EObjectState, State, EObjectState::Unknown);

    using TAttributeList = SmallVector<IPersistentAttribute*, 16>;
    DEFINE_BYREF_RO_PROPERTY(TAttributeList, Attributes);

    static const TScalarAttributeSchema<TObject, TInstant> CreationTimeSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TInstant>, CreationTime);

    using TMetaEtc = NProto::TMetaEtc;
    static const TScalarAttributeSchema<TObject, TMetaEtc> MetaEtcSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TMetaEtc>, MetaEtc);

    static const TScalarAttributeSchema<TObject, NYT::NYTree::IMapNodePtr> LabelsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<NYT::NYTree::IMapNodePtr>, Labels);

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAnnotationsAttribute, Annotations);

    static const TScalarAttributeSchema<TObject, bool> InheritAclSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<bool>, InheritAcl);

    using TAcl = std::vector<NClient::NApi::NProto::TAccessControlEntry>;
    static const TScalarAttributeSchema<TObject, TAcl> AclSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TAcl>, Acl);

    bool DoesExist() const;
    bool DidExist() const;
    void ValidateExists() const;

    void ScheduleTombstoneCheck();
    bool IsTombstone() const;

    void ScheduleStore();
    bool IsStoreScheduled() const;

    bool IsRemoving() const;

    virtual bool IsBuiltin() const;

    template <class T>
    void ValidateAs() const;
    template <class T>
    const T* As() const;
    template <class T>
    T* As();

    bool HasHistoryEnabledAttributes() const;
    NYT::NYson::TYsonString GetHistoryEnabledAttributes();

private:
    friend class TAttributeBase;

    const TObjectId Id_;
    IObjectTypeHandler* const TypeHandler_;
    ISession* const Session_;
    bool StoreScheduled_ = false;

    TObjectExistenceChecker ExistenceChecker_;
    TObjectTombstoneChecker TombstoneChecker_;
    TParentIdAttribute ParentIdAttribute_;

    void RegisterAttribute(IPersistentAttribute* attribute);
};

////////////////////////////////////////////////////////////////////////////////

TClusterTag ClusterTagFromId(const TTransactionId& id);
TMasterInstanceTag MasterInstanceTagFromId(const TTransactionId& id);
TObjectId GetObjectId(TObject* object);
void ValidateObjectId(EObjectType type, const TObjectId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

#define PERSISTENCE_INL_H_
#include "persistence-inl.h"
#undef PERSISTENCE_INL_H_
