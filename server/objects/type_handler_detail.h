#pragma once

#include "type_handler.h"
#include "attribute_schema.h"
#include "private.h"

#include <yp/server/master/public.h>

#include <yp/server/access_control/public.h>

#include <yt/client/api/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    TObjectTypeHandlerBase(
        NMaster::TBootstrap* bootstrap,
        EObjectType type);

    virtual void Initialize() override;
    virtual void PostInitialize() override;

    virtual EObjectType GetType() override;

    virtual EObjectType GetParentType() override;
    virtual TObject* GetParent(TObject* object) override;
    virtual const TDBField* GetParentIdField() override;
    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override;

    virtual TObjectId GetSchemaObjectId() override;
    virtual TObject* GetSchemaObject(TObject* object) override;

    virtual TAttributeSchema* GetRootAttributeSchema() override;
    virtual TAttributeSchema* GetIdAttributeSchema() override;
    virtual TAttributeSchema* GetParentIdAttributeSchema() override;

    virtual bool HasHistoryEnabledAttributes() override;
    virtual const NYT::NYson::TYsonString& GetHistoryEnabledAttributePaths() override;
    virtual bool HasHistoryEnabledAttributeForStore(TObject* object) override;

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override;

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override;

    virtual void BeforeObjectRemoved(
        TTransaction* transaction,
        TObject* object) override;

    virtual void AfterObjectRemoved(
        TTransaction* transaction,
        TObject* object) override;

protected:
    NMaster::TBootstrap* const Bootstrap_;
    const EObjectType Type_;

    const TObjectId SchemaId_;

    std::vector<std::unique_ptr<TAttributeSchema>> AttributeSchemas_;
    TAttributeSchema* RootAttributeSchema_ = nullptr;
    TAttributeSchema* IdAttributeSchema_ = nullptr;
    TAttributeSchema* ParentIdAttributeSchema_ = nullptr;
    TAttributeSchema* MetaAttributeSchema_ = nullptr;
    TAttributeSchema* LabelsAttributeSchema_ = nullptr;
    TAttributeSchema* SpecAttributeSchema_ = nullptr;
    TAttributeSchema* StatusAttributeSchema_ = nullptr;
    TAttributeSchema* AnnotationsAttributeSchema_ = nullptr;
    TAttributeSchema* ControlAttributeSchema_ = nullptr;

    TAttributeSchema* MakeAttributeSchema(const TString& name);
    TAttributeSchema* MakeEtcAttributeSchema();

protected:
    virtual std::vector<NAccessControl::EAccessControlPermission> GetDefaultPermissions();
    virtual bool IsObjectNameSupported() const;

private:
    bool HasHistoryEnabledAttributes_;
    NYT::NYson::TYsonString SerializedHistoryEnabledAttributePaths_;

private:
    void ValidateMetaEtc(TTransaction* transaction, TObject* object);
    void ValidateAcl(TTransaction* transaction, TObject* object);

    void PrepareHistoryEnabledAttributeSchemaCache();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
