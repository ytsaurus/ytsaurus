#include "type_handler_detail.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yt/ytlib/query_client/ast.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NYTree;
using namespace NYT::NQueryClient;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TObjectTypeHandlerBase::TObjectTypeHandlerBase(
    NMaster::TBootstrap* bootstrap,
    EObjectType type)
    : Bootstrap_(bootstrap)
    , Type_(type)
{
    RootAttributeSchema_ = MakeAttributeSchema(TString())
        ->AddChildren({
            MetaAttributeSchema_ = MakeAttributeSchema("meta")
                ->AddChildren({
                    IdAttributeSchema_ = MakeAttributeSchema("id")
                        ->SetAttribute(TObject::IdSchema),

                    MakeAttributeSchema("type")
                        ->SetExpressionBuilder(
                            [type = Type_] (IQueryContext* /*context*/) {
                                return New<TLiteralExpression>(NYT::NQueryClient::TSourceLocation(), FormatEnum(type));
                            }),

                    MakeAttributeSchema("creation_time")
                        ->SetAttribute(TObject::CreationTimeSchema),

                    MakeAttributeSchema("owner")
                        ->SetAttribute(TObject::OwnerSchema),

                    MakeAttributeSchema("inherit_acl")
                        ->SetAttribute(TObject::InheritAclSchema)
                        ->SetUpdatable(),

                    MakeAttributeSchema("acl")
                        ->SetAttribute(TObject::AclSchema)
                        ->SetUpdatable()
                }),

            MakeAttributeSchema("labels")
                ->SetAttribute(TObject::LabelsSchema)
                ->SetUpdatable(),

            SpecAttributeSchema_ = MakeAttributeSchema("spec")
                ->SetUpdatable(),

            StatusAttributeSchema_ = MakeAttributeSchema("status"),

            AnnotationsAttributeSchema_ = MakeAttributeSchema("annotations")
                ->SetAnnotationsAttribute(),

            ControlAttributeSchema_ = MakeAttributeSchema("control")
                ->SetControlAttribute()
        });
}

EObjectType TObjectTypeHandlerBase::GetType()
{
    return Type_;
}

EObjectType TObjectTypeHandlerBase::GetParentType()
{
    return EObjectType::Null;
}

const TDBField* TObjectTypeHandlerBase::GetParentIdField()
{
    return nullptr;
}

TChildrenAttributeBase* TObjectTypeHandlerBase::GetParentChildrenAttribute(TObject* /*parent*/)
{
    Y_UNREACHABLE();
}

TAttributeSchema* TObjectTypeHandlerBase::GetRootAttributeSchema()
{
    return RootAttributeSchema_;
}

TObject* TObjectTypeHandlerBase::GetAccessControlParent(TObject* object)
{
    if (GetType() == EObjectType::Schema) {
        return nullptr;
    }
    static const auto SchemaId = FormatEnum(GetType());
    auto* session = object->GetSession();
    return session->GetObject(EObjectType::Schema, SchemaId);
}

TAttributeSchema* TObjectTypeHandlerBase::GetIdAttributeSchema()
{
    return IdAttributeSchema_;
}

TAttributeSchema* TObjectTypeHandlerBase::GetParentIdAttributeSchema()
{
    return ParentIdAttributeSchema_;
}

void TObjectTypeHandlerBase::BeforeObjectCreated(
    const TTransactionPtr& transaction,
    TObject* object)
{
    for (auto* attribute : object->Attributes()) {
        attribute->OnObjectCreated();
    }

    object->CreationTime() = TInstant::Now();
    object->Labels() = GetEphemeralNodeFactory()->CreateMap();

    const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
    object->InheritAcl() = true;
    object->Owner() = accessControlManager->GetAuthenticatedUser();
}

void TObjectTypeHandlerBase::AfterObjectCreated(
    const TTransactionPtr& /*transaction*/,
    TObject* /*object*/)
{ }

void TObjectTypeHandlerBase::BeforeObjectRemoved(
    const TTransactionPtr& /*transaction*/,
    TObject* /*object*/)
{ }

void TObjectTypeHandlerBase::AfterObjectRemoved(
    const TTransactionPtr& /*transaction*/,
    TObject* object)
{ }

TAttributeSchema* TObjectTypeHandlerBase::MakeAttributeSchema(
    const TString& name)
{
    auto schemaHolder = std::make_unique<TAttributeSchema>(this, name);
    auto* schema = schemaHolder.get();
    AttributeSchemas_.push_back(std::move(schemaHolder));
    return schema;
}

TAttributeSchema* TObjectTypeHandlerBase::MakeFallbackAttributeSchema()
{
    return MakeAttributeSchema(TString())
        ->SetFallback();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

