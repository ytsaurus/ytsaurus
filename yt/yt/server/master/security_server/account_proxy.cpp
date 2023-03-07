#include "account_proxy.h"
#include "account.h"
#include "security_manager.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/map_object_proxy.h>
#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/ytlib/security_client/proto/account_ypath.pb.h>

#include <yt/core/yson/async_writer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/virtual.h>

namespace NYT::NSecurityServer {

using namespace NChunkServer;
using namespace NObjectServer;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

class TAccountProxy
    : public TNonversionedMapObjectProxyBase<TAccount>
{
private:
    using TBase = TNonversionedMapObjectProxyBase<TAccount>;
    using TBasePtr = TIntrusivePtr<TBase>;

public:
    TAccountProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TAccount* account)
        : TBase(bootstrap, metadata, account)
    { }

protected:
    virtual std::unique_ptr<TNonversionedMapObjectFactoryBase<TAccount>> CreateObjectFactory() const override
    {
        return std::make_unique<TAccountFactory>(Bootstrap_);
    }

    virtual TBasePtr ResolveNameOrThrow(const TString& name) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccountByNameOrThrow(name);
        return GetProxy(account);
    }

    virtual void ValidateBeforeAttachChild(
        const TString& key,
        const TIntrusivePtr<TNonversionedMapObjectProxyBase<TAccount>>& child) override
    {
        TBase::ValidateBeforeAttachChild(key, child);

        auto* childAccount = child->GetObject()->As<TAccount>();
        auto* impl = GetThisImpl();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidateAttachChildAccount(impl, childAccount);
    }

private:
    class TAccountFactory
        : public TNonversionedMapObjectFactoryBase<TAccount>
    {
    public:
        explicit TAccountFactory(NCellMaster::TBootstrap* bootstrap)
            : TNonversionedMapObjectFactoryBase<TAccount>(bootstrap)
        { }

    protected:
        virtual TAccount* DoCreateObject(IAttributeDictionary* attributes) override
        {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto hintId = attributes->GetAndRemove("hint_id", NullObjectId);
            return securityManager->CreateAccount(hintId);
        }
    };

    virtual void ValidateRemoval() override
    {
        const auto* account = GetThisImpl();
        if (account->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in account %Qv",
                account->GetName());
        }

        TBase::ValidateRemoval();
    }

    virtual void ValidateChildNameAvailability(const TString& childName) override
    {
        TNonversionedMapObjectProxyBase::ValidateChildNameAvailability(childName);

        if (Bootstrap_->GetSecurityManager()->FindAccountByName(childName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %Qv already exists",
                childName);
        }
    }

    bool IsRootAccount() const
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return GetThisImpl() == securityManager->GetRootAccount();
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        auto isRootAccount = IsRootAccount();
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceUsage)
            .SetPresent(!isRootAccount));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CommittedResourceUsage)
            .SetPresent(!isRootAccount));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellStatistics)
            .SetOpaque(true)
            .SetPresent(!isRootAccount));

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AllowChildrenLimitOvercommit)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true)
            .SetPresent(!isRootAccount));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceLimits)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true)
            .SetPresent(!isRootAccount));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ViolatedResourceLimits)
            .SetPresent(!isRootAccount));

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveResourceUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveCommittedResourceUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveViolatedResourceLimits)
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* account = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ResourceUsage: {
                if (IsRootAccount()) {
                    break;
                }
                auto resourceUsage =
                    account->ClusterStatistics().ResourceUsage - account->ComputeTotalChildrenResourceUsage();
                SerializeClusterResources(resourceUsage, account, true, consumer);
                return true;
            }

            case EInternedAttributeKey::RecursiveResourceUsage:
                SerializeClusterResources(account->ClusterStatistics().ResourceUsage, account, true, consumer);
                return true;

            case EInternedAttributeKey::CommittedResourceUsage: {
                if (IsRootAccount()) {
                    break;
                }
                auto resourceUsage =
                    account->ClusterStatistics().CommittedResourceUsage - account->ComputeTotalChildrenCommittedResourceUsage();
                SerializeClusterResources(resourceUsage, account, true, consumer);
                return true;
            }

            case EInternedAttributeKey::RecursiveCommittedResourceUsage:
                SerializeClusterResources(account->ClusterStatistics().CommittedResourceUsage, account, true, consumer);
                return true;

            case EInternedAttributeKey::MulticellStatistics: {
                if (IsRootAccount()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();

                BuildYsonFluently(consumer)
                    .DoMapFor(account->MulticellStatistics(), [&] (TFluentMap fluent, const std::pair<TCellTag, const TAccountStatistics&>& pair) {
                        fluent.Item(ToString(pair.first));
                        Serialize(pair.second, fluent.GetConsumer(), chunkManager);
                    });
                return true;
            }

            case EInternedAttributeKey::AllowChildrenLimitOvercommit:
                if (IsRootAccount()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(account->GetAllowChildrenLimitOvercommit());
                return true;

            case EInternedAttributeKey::ResourceLimits:
                if (IsRootAccount()) {
                    break;
                }
                SerializeClusterResources(account->ClusterResourceLimits(), account, true, consumer);
                return true;

            case EInternedAttributeKey::ViolatedResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();

                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("disk_space").Value(account->IsDiskSpaceLimitViolated())
                        .Item("disk_space_per_medium").DoMapFor(chunkManager->Media(),
                            [&] (TFluentMap fluent, const std::pair<TMediumId, TMedium*>& pair) {
                                const auto* medium = pair.second;
                                fluent
                                    .Item(medium->GetName()).Value(account->IsDiskSpaceLimitViolated(medium->GetIndex()));
                            })
                        .Item("node_count").Value(account->IsNodeCountLimitViolated())
                        .Item("chunk_count").Value(account->IsChunkCountLimitViolated())
                        .Item("tablet_count").Value(account->IsTabletCountLimitViolated())
                        .Item("tablet_static_memory").Value(account->IsTabletStaticMemoryLimitViolated())
                        .Item("master_memory").Value(account->IsMasterMemoryLimitViolated())
                    .EndMap();
                return true;
            }

            case EInternedAttributeKey::RecursiveViolatedResourceLimits: {
                const auto& securityManager = Bootstrap_->GetSecurityManager();

                auto violatedLimits = securityManager->GetAccountRecursiveViolatedResourceLimits(account);
                SerializeClusterResources(violatedLimits, nullptr, false, consumer);
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value) override
    {
        auto* account = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        switch (key) {
            case EInternedAttributeKey::AllowChildrenLimitOvercommit: {
                if (IsRootAccount()) {
                    break;
                }

                auto overcommitAllowed = ConvertTo<bool>(value);
                securityManager->SetAccountAllowChildrenLimitOvercommit(account, overcommitAllowed);
                return true;
            }

            case EInternedAttributeKey::ResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }

                auto limits = ConvertTo<TSerializableClusterResourcesPtr>(value);
                securityManager->TrySetResourceLimits(account, limits->ToClusterResources(chunkManager));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    void SerializeClusterResources(
        const TClusterResources& clusterResources,
        const TAccount* account,
        bool serializeDiskSpace,
        NYson::IYsonConsumer* consumer)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto resourceSerializer = New<TSerializableClusterResources>(
            chunkManager,
            clusterResources,
            serializeDiskSpace);

        if (account) {
            // Make sure medium disk space usage is serialized even if it's zero - for media with limits set.
            for (auto [mediumIndex, _] : account->ClusterResourceLimits().DiskSpace()) {
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (!medium || medium->GetCache()) {
                    continue;
                }
                resourceSerializer->AddToMediumDiskSpace(medium->GetName(), 0);
            }
        }

        BuildYsonFluently(consumer)
            .Value(resourceSerializer);
    }
};

TIntrusivePtr<TNonversionedMapObjectProxyBase<TAccount>> CreateAccountProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TAccount* account)
{
    return New<TAccountProxy>(bootstrap, metadata, account);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

