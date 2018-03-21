#pragma once

#include "type_handler.h"
#include "attribute_schema.h"
#include "private.h"

#include <yp/server/master/public.h>

#include <yt/ytlib/api/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectTypeHandlerBase
    : public IObjectTypeHandler
{
public:
    TObjectTypeHandlerBase(
        NMaster::TBootstrap* bootstrap,
        EObjectType type);

    virtual EObjectType GetType() override;
    virtual EObjectType GetParentType() override;
    virtual const TDbField* GetParentIdField() override;
    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override;

    virtual TAttributeSchema* GetRootAttributeSchema() override;
    virtual TAttributeSchema* GetIdAttributeSchema() override;
    virtual TAttributeSchema* GetParentIdAttributeSchema() override;

    virtual void BeforeObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override;

    virtual void AfterObjectCreated(
        const TTransactionPtr& transaction,
        TObject* object) override;

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override;

    virtual void AfterObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override;

protected:
    NMaster::TBootstrap* const Bootstrap_;
    const EObjectType Type_;

    std::vector<std::unique_ptr<TAttributeSchema>> AttributeSchemas_;
    TAttributeSchema* RootAttributeSchema_ = nullptr;
    TAttributeSchema* IdAttributeSchema_ = nullptr;
    TAttributeSchema* ParentIdAttributeSchema_ = nullptr;
    TAttributeSchema* MetaAttributeSchema_ = nullptr;
    TAttributeSchema* SpecAttributeSchema_ = nullptr;
    TAttributeSchema* StatusAttributeSchema_ = nullptr;
    TAttributeSchema* AnnotationsAttributeSchema_ = nullptr;
    TAttributeSchema* ControlAttributeSchema_ = nullptr;

    TAttributeSchema* MakeAttributeSchema(const TString& name);
    TAttributeSchema* MakeFallbackAttributeSchema();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
