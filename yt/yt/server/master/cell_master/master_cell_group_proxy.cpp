#include "master_cell_group_proxy.h"

#include "bootstrap.h"
#include "master_cell_group.h"
#include "master_cell_group_manager.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellMaster {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMasterCellGroupProxy
    : public TNonversionedObjectProxyBase<TMasterCellGroup>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TMasterCellGroup>;

    void ValidateRemoval() override
    { }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellTags)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* group = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(group->GetName());
                return true;

            case EInternedAttributeKey::CellTags:
                BuildYsonFluently(consumer)
                    .Value(group->CellTags());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        auto* group = GetThisImpl();
        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();

        switch (key) {
            case EInternedAttributeKey::Name: {
                groupManager->RenameMasterCellGroup(group, ConvertTo<std::string>(value));
                return true;
            }

            case EInternedAttributeKey::CellTags: {
                groupManager->SetMasterCellGroupCellTags(group, ConvertTo<TCellTagSet>(value));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateMasterCellGroupProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TMasterCellGroup* group)
{
    return New<TMasterCellGroupProxy>(bootstrap, metadata, group);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
