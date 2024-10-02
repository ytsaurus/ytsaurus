#ifndef GROUP_TYPE_HANDLER_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include group_type_handler_detail.h"
// For the sake of sane code completion.
#include "group_type_handler_detail.h"
#endif

#include "config.h"
#include "transaction.h"
#include "subject_type_handler_detail.h"

#include <yt/yt/orm/server/access_control/public.h>
#include <yt/yt/orm/server/master/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TGroup, std::derived_from<IObjectTypeHandler> TGeneratedGroupTypeHandler>
class TGroupTypeHandlerBase
    : public TSubjectTypeHandlerBase<TGeneratedGroupTypeHandler>
{
    using TBase = TSubjectTypeHandlerBase<TGeneratedGroupTypeHandler>;

public:
    TGroupTypeHandlerBase(
        NMaster::IBootstrap* bootstrap,
        TObjectManagerConfigPtr config)
        : TBase(bootstrap, config)
        , Config_(config->GroupTypeHandler)
    { }

    virtual void Initialize() override
    {
        TBase::Initialize();

        TBase::SpecAttributeSchema_
            ->template AddUpdateHandler<TGroup>(std::bind_front(&TGroupTypeHandlerBase::AfterSpecUpdated, this))
            ->template AddValidator<TGroup>(std::bind_front(&TGroupTypeHandlerBase::ValidateSpec, this));
    }

    virtual bool IsBuiltin(const TObject* object) const override
    {
        return object->template As<TGroup>()->GetId() == SuperusersGroupId;
    }

protected:
    virtual std::vector<NAccessControl::TAccessControlEntry> GetDefaultAcl() override
    {
        return {};
    }

private:
    std::vector<TObjectId> GetNewMembers(const TGroup* group)
    {
        const auto& groupSpec = group->Spec().Etc().Load();
        const auto& groupSpecOld = group->Spec().Etc().LoadOld();

        THashSet<TObjectId> oldMembers;
        for (const auto& member : groupSpecOld.members()) {
            oldMembers.insert(member);
        }

        std::vector<TObjectId> newMembers;
        for (const auto& member : groupSpec.members()) {
            if (oldMembers.find(member) == oldMembers.end()) {
                newMembers.push_back(member);
            }
        }
        return newMembers;
    }

    void AfterSpecUpdated(TTransaction* transaction, TGroup* group)
    {
        if (!Config_->EnableMembersValidation) {
            return;
        }
        auto newMembers = GetNewMembers(group);
        for (const auto& id : newMembers) {
            // GetObject schedules existence checking.
            transaction->GetObject(NClient::NObjects::TObjectTypeValues::User, TObjectKey(id));
            transaction->GetObject(NClient::NObjects::TObjectTypeValues::Group, TObjectKey(id));
        }
    }

    void ValidateSpec(TTransaction* transaction, const TGroup* group)
    {
        if (!Config_->EnableMembersValidation) {
            return;
        }
        auto newMembers = GetNewMembers(group);
        ValidateNewMembers(transaction, group, newMembers);
    }

    static void ValidateNewMembers(
        TTransaction* transaction,
        const TGroup* group,
        const std::vector<TObjectId>& newMembers)
    {
        for (const auto& id : newMembers) {
            if (id.Empty()) {
                THROW_ERROR_EXCEPTION("%v contains empty user id",
                    group->GetDisplayName());
            }

            if (id == group->GetId()) {
                THROW_ERROR_EXCEPTION("%v contains itself in members",
                    group->GetDisplayName());
            }

            if (!transaction->GetObject(NClient::NObjects::TObjectTypeValues::User, TObjectKey(id))->DoesExist() &&
                !transaction->GetObject(NClient::NObjects::TObjectTypeValues::Group, TObjectKey(id))->DoesExist())
            {
                THROW_ERROR_EXCEPTION("Member %v in %v does not exist",
                    id,
                    group->GetDisplayName());
            }
        }
    }

private:
    const TGroupTypeHandlerConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TGroup, std::derived_from<IObjectTypeHandler> TGeneratedGroupTypeHandler>
std::unique_ptr<IObjectTypeHandler> CreateGroupTypeHandler(
    NMaster::IBootstrap* bootstrap,
    TObjectManagerConfigPtr config)
{
    return std::make_unique<TGroupTypeHandlerBase<TGroup, TGeneratedGroupTypeHandler>>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
