#include "endpoint_set_type_handler.h"
#include "type_handler_detail.h"
#include "endpoint_set.h"
#include "db_schema.h"

#include <yt/core/ytree/fluent.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NYson;
using namespace NYT::NYTree;

using std::placeholders::_1;
using std::placeholders::_2;
using std::placeholders::_3;

////////////////////////////////////////////////////////////////////////////////

class TEndpointSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TEndpointSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::EndpointSet)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TEndpointSet::SpecSchema)
            ->SetValidator<TEndpointSet>(std::bind(&TEndpointSetTypeHandler::ValidateLivenessLimitRatio, this, _1, _2));

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("last_endpoints_update_timestamp")
                    ->SetPreevaluator<TEndpointSet>(std::bind(&TEndpointSetTypeHandler::PreevaluateLastEndpointsUpdateTimestamp, this, _1, _2))
                    ->SetEvaluator<TEndpointSet>(std::bind(&TEndpointSetTypeHandler::EvaluateLastEndpointsUpdateTimestamp, this, _1, _2, _3)),
              });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TEndpointSet>();
    }

    virtual void AfterObjectCreated(TTransaction* transaction, TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);
        object->As<TEndpointSet>()->Status().LastEndpointsUpdateTimestamp().Touch();
    }

    virtual const TDBTable* GetTable() override
    {
        return &EndpointSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &EndpointSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TEndpointSet(id, this, session));
    }

private:
    void PreevaluateLastEndpointsUpdateTimestamp(TTransaction* /*transaction*/, TEndpointSet* endpointSet)
    {
        endpointSet->Status().LastEndpointsUpdateTimestamp().ScheduleLoad();
    }

    void EvaluateLastEndpointsUpdateTimestamp(TTransaction* /*transaction*/, TEndpointSet* endpointSet, NYT::NYson::IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .Value(endpointSet->Status().LastEndpointsUpdateTimestamp().Load());
    }

    void ValidateLivenessLimitRatio(TTransaction* /*transaction*/, TEndpointSet* endpointSet)
    {
        const auto livenessLimitRatio = endpointSet->Spec().Get()->liveness_limit_ratio();
        if (livenessLimitRatio < 0 || livenessLimitRatio > 1) {
            THROW_ERROR_EXCEPTION("Liveness limit ratio value %v must be in range [0, 1]",
                livenessLimitRatio);
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateEndpointSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TEndpointSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

