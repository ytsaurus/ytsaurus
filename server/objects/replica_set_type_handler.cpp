#include "replica_set_type_handler.h"
#include "type_handler_detail.h"
#include "replica_set.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TReplicaSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::ReplicaSet)
    {
        SpecAttributeSchema_
            ->SetAttribute(TReplicaSet::SpecSchema);

        StatusAttributeSchema_
            ->SetAttribute(TReplicaSet::StatusSchema)
            ->SetUpdatable();
    }

    virtual const TDBTable* GetTable() override
    {
        return &ReplicaSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &ReplicaSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TReplicaSet(id, this, session));
    }
};

std::unique_ptr<IObjectTypeHandler> CreateReplicaSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TReplicaSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

