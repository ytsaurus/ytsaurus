#include "node_segment_type_handler.h"
#include "type_handler_detail.h"
#include "node_segment.h"
#include "pod_set.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegmentTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TNodeSegmentTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::NodeSegment)
    {
        SpecAttributeSchema_
            ->SetAttribute(TNodeSegment::SpecSchema);

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDBTable* GetTable() override
    {
        return &NodeSegmentsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &NodeSegmentsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TNodeSegment(id, this, session));
    }

    virtual void BeforeObjectRemoved(
        const TTransactionPtr& transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* segment = object->As<TNodeSegment>();
        const auto& podSets = segment->PodSets().Load();
        if (!podSets.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove node segment %Qv since it has %v pod set(s) assigned",
                segment->GetId(),
                podSets.size());
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreateNodeSegmentTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TNodeSegmentTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

