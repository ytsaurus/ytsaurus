#include "replication_card_type_handler.h"

#include "type_handler_detail.h"
#include "client_impl.h"

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi::NNative {

using namespace NYson;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NChaosClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardTypeHandler
    : public TVirtualTypeHandler
{
public:
    explicit TReplicationCardTypeHandler(TClient* client)
        : Client_(client)
    { }

private:
    TClient* const Client_;


    EObjectType GetSupportedObjectType() override
    {
        return EObjectType::ReplicationCard;
    }

    TYsonString GetObjectYson(TReplicationCardId replicationCardId) override
    {
        TGetReplicationCardOptions getCardOptions;
        getCardOptions.IncludeCoordinators = true;
        getCardOptions.IncludeProgress = true;
        getCardOptions.IncludeHistory = true;
        getCardOptions.BypassCache = true;
        auto card = WaitFor(Client_->GetReplicationCard(replicationCardId, getCardOptions))
            .ValueOrThrow();

        return BuildYsonStringFluently()
            .BeginAttributes()
                .Item("id").Value(replicationCardId)
                .Item("type").Value(EObjectType::ReplicationCard)
                .Do([&] (auto fluent) {
                    Serialize(
                        *card,
                        fluent,
                        static_cast<TReplicationCardFetchOptions&>(getCardOptions));
                })
            .EndAttributes()
            .Entity();
    }

    void RemoveObject(TReplicationCardId /*replicationCardId*/) override
    {
        THROW_ERROR_EXCEPTION("Unsupported");
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeHandlerPtr CreateReplicationCardTypeHandler(TClient* client)
{
    return New<TReplicationCardTypeHandler>(client);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
