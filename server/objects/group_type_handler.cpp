#include "group_type_handler.h"
#include "subject_type_handler_detail.h"
#include "group.h"
#include "db_schema.h"

#include <yp/server/access_control/public.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;


////////////////////////////////////////////////////////////////////////////////

class TGroupTypeHandler
    : public TSubjectTypeHandlerBase
{
public:
    explicit TGroupTypeHandler(NMaster::TBootstrap* bootstrap)
        : TSubjectTypeHandlerBase(bootstrap, EObjectType::Group)
    { }

    virtual void Initialize() override
    {
        TSubjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->SetAttribute(TGroup::SpecSchema);

        StatusAttributeSchema_
            ->SetComposite();
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TGroup>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &GroupsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &GroupsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TGroup(id, this, session));
    }

protected:
    virtual std::vector<EAccessControlPermission> GetDefaultPermissions() override
    {
        return {};
    }

    virtual bool IsObjectNameSupported() const override
    {
        return true;
    }
};

std::unique_ptr<IObjectTypeHandler> CreateGroupTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TGroupTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

