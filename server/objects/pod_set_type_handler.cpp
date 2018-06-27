#include "pod_set_type_handler.h"
#include "type_handler_detail.h"
#include "pod_set.h"
#include "pod.h"
#include "node_segment.h"
#include "db_schema.h"

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NAccessControl;

////////////////////////////////////////////////////////////////////////////////

class TPodSetTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TPodSetTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PodSet)
    {
        SpecAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("antiaffinity_constraints")
                    ->SetAttribute(TPodSet::TSpec::AntiaffinityConstraintsSchema)
                    ->SetUpdatable(),

                MakeAttributeSchema("node_segment_id")
                    ->SetAttribute(TPodSet::TSpec::NodeSegmentSchema)
                    ->SetUpdatable()
            });

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PodSetsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PodSetsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YCHECK(!parentId);
        return std::unique_ptr<TObject>(new TPodSet(id, this, session));
    }

private:
    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        auto result = TObjectTypeHandlerBase::GetDefaultPermissions();
        result.push_back(EAccessControlPermission::SshAccess);
        result.push_back(EAccessControlPermission::RootSshAccess);
        return result;
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePodSetTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPodSetTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

