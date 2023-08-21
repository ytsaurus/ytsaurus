#include "schema.h"
#include "private.h"
#include "type_handler.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NObjectServer {

using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TSchemaProxy
    : public TNonversionedObjectProxyBase<TSchemaObject>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TSchemaObject>;

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        switch (key) {
            case EInternedAttributeKey::Type: {
                auto type = TypeFromSchemaType(TypeFromId(GetId()));
                BuildYsonFluently(consumer)
                    .Value(Format("schema:%v", FormatEnum(type)));
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateSchemaProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TSchemaObject* object)
{
    return New<TSchemaProxy>(bootstrap, metadata, object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
