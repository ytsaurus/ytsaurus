#pragma once

#include "public.h"

#include <yt/core/yson/protobuf_interop.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IObjectTypeHandler
{
    virtual ~IObjectTypeHandler() = default;

    virtual void Initialize() = 0;

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() = 0;
    virtual const TDBTable* GetTable() = 0;
    virtual const TDBField* GetIdField() = 0;
    virtual EObjectType GetType() = 0;

    virtual EObjectType GetParentType() = 0;
    virtual TObject* GetParent(TObject* object) = 0;
    virtual const TDBField* GetParentIdField() = 0;
    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) = 0;

    virtual TObjectId GetSchemaObjectId() = 0;
    virtual TObject* GetSchemaObject(TObject* object) = 0;

    virtual TAttributeSchema* GetRootAttributeSchema() = 0;
    virtual TAttributeSchema* GetIdAttributeSchema() = 0;
    virtual TAttributeSchema* GetParentIdAttributeSchema() = 0;

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) = 0;
    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) = 0;
    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) = 0;
    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) = 0;
    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
