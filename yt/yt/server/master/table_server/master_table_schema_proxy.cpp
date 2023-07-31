#include "master_table_schema_proxy.h"

#include "private.h"
#include "master_table_schema.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TMasterTableSchemaProxy
    : public TNonversionedObjectProxyBase<TMasterTableSchema>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TMasterTableSchema>;

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::ExportRefCounter);
        descriptors->push_back(EInternedAttributeKey::MemoryUsage);
        descriptors->push_back(EInternedAttributeKey::ReferencingAccounts);
        descriptors->push_back(EInternedAttributeKey::Value);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* schema = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ExportRefCounter:
                BuildYsonFluently(consumer)
                    .DoMapFor(schema->CellTagToExportCount(), [&] (TFluentMap fluent, const auto& pair) {
                        const auto& multicellManager = Bootstrap_->GetMulticellManager();
                        auto [cellTag, refCounter] = pair;
                        fluent
                            .Item(multicellManager->GetMasterCellName(pair.first))
                            .Value(refCounter);
                    });
                return true;

            case EInternedAttributeKey::MemoryUsage:
                BuildYsonFluently(consumer)
                    .Value(schema->AsTableSchema()->GetMemoryUsage());
                return true;

            case EInternedAttributeKey::ReferencingAccounts:
                BuildYsonFluently(consumer)
                    .DoMapFor(schema->ReferencingAccounts(), [] (TFluentMap fluent, const auto& pair) {
                        auto [account, refCounter] = pair;
                        fluent
                            .Item(account->GetName())
                            .Value(refCounter);
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override
    {
        const auto* schema = GetThisImpl();

        switch(key) {
            case EInternedAttributeKey::Value:
                return schema->AsYsonAsync();

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    void GetSelf(TReqGet* /*request*/, TRspGet* response, const TCtxGetPtr& context) override
    {
        GetThisImpl()->AsYsonAsync().Subscribe(BIND([=] (const TErrorOr<TYsonString>& resultOrError) {
            if (resultOrError.IsOK()) {
                response->set_value(resultOrError.Value().ToString());
                context->Reply();
            } else {
                context->Reply(resultOrError);
            }
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateMasterTableSchemaProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterTableSchema* schema)
{
    return New<TMasterTableSchemaProxy>(bootstrap, metadata, schema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
