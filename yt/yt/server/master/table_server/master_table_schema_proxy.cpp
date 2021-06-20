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
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

class TMasterTableSchemaProxy
    : public TNonversionedObjectProxyBase<TMasterTableSchema>
{
private:
    using TBase = TNonversionedObjectProxyBase<TMasterTableSchema>;

public:
    using TBase::TBase;

    virtual void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::MemoryUsage);
        descriptors->push_back(EInternedAttributeKey::ReferencingAccounts);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* schema = GetThisImpl();

        switch (key) {
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
};

IObjectProxyPtr CreateMasterTableSchemaProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterTableSchema* schema)
{
    return New<TMasterTableSchemaProxy>(bootstrap, metadata, schema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
