#include "account_proxy.h"
#include "account.h"
#include "helpers.h"
#include "security_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/map_object_proxy.h>
#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/ytlib/security_client/proto/account_ypath.pb.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/virtual.h>

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
        auto* account = securityManager->GetAccountByNameOrThrow(name, true /*activeLifeStageOnly*/);
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

        if (Bootstrap_->GetSecurityManager()->FindAccountByName(childName, false /*activeLifeStageOnly*/)) {
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
        const auto* account = GetThisImpl();
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
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalChildrenResourceLimits)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MergeJobRateLimit)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Abc)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(account->GetAbcConfig().operator bool()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FolderId)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(account->GetFolderId().has_value()));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* account = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ResourceUsage: {
                if (IsRootAccount()) {
                    break;
                }
                auto resourceUsage =
                    account->ClusterStatistics().ResourceUsage - account->ComputeTotalChildrenResourceUsage();
                SerializeClusterResources(chunkManager, resourceUsage, account, consumer);
                return true;
            }

            case EInternedAttributeKey::RecursiveResourceUsage:
                SerializeClusterResources(
                    chunkManager,
                    account->ClusterStatistics().ResourceUsage,
                    account,
                    consumer);
                return true;

            case EInternedAttributeKey::CommittedResourceUsage: {
                if (IsRootAccount()) {
                    break;
                }
                auto resourceUsage =
                    account->ClusterStatistics().CommittedResourceUsage - account->ComputeTotalChildrenCommittedResourceUsage();
                SerializeClusterResources(chunkManager, resourceUsage, account, consumer);
                return true;
            }

            case EInternedAttributeKey::RecursiveCommittedResourceUsage:
                SerializeClusterResources(
                    chunkManager,
                    account->ClusterStatistics().CommittedResourceUsage,
                    account,
                    consumer);
                return true;

            case EInternedAttributeKey::MulticellStatistics: {
                if (IsRootAccount()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& multicellManager = Bootstrap_->GetMulticellManager();

                BuildYsonFluently(consumer)
                    .DoMapFor(account->MulticellStatistics(), [&] (TFluentMap fluent, const std::pair<TCellTag, const TAccountStatistics&>& pair) {
                        fluent.Item(multicellManager->GetMasterCellName(pair.first));
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
                SerializeClusterResourceLimits(account->ClusterResourceLimits(), true, consumer);
                return true;

            case EInternedAttributeKey::ViolatedResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& multicellManager = Bootstrap_->GetMulticellManager();

                auto cellTags = multicellManager->GetSecondaryCellTags();
                cellTags.push_back(multicellManager->GetPrimaryCellTag());

                // TODO(shakurov): introduce TAccount::GetViolatedResourceLimits
                // and make use of it here.
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
                        .Item("master_memory")
                            .BeginMap()
                                .Item("total").Value(account->IsMasterMemoryLimitViolated())
                                .Item("chunk_host").Value(account->IsChunkHostMasterMemoryLimitViolated(multicellManager))
                                .Item("per_cell").DoMapFor(cellTags, [&] (TFluentMap fluent, TCellTag cellTag) {
                                    fluent
                                        .Item(multicellManager->GetMasterCellName(cellTag)).Value(account->IsMasterMemoryLimitViolated(cellTag));
                                })
                            .EndMap()
                    .EndMap();
                return true;
            }

            case EInternedAttributeKey::RecursiveViolatedResourceLimits: {
                const auto& securityManager = Bootstrap_->GetSecurityManager();

                auto violatedLimits = securityManager->GetAccountRecursiveViolatedResourceLimits(account);
                SerializeClusterResourceLimits(violatedLimits, false, consumer);
                return true;
            }

            case EInternedAttributeKey::TotalChildrenResourceLimits: {
                auto resourceLimits = account->ComputeTotalChildrenLimits();
                SerializeClusterResourceLimits(resourceLimits, true, consumer);
                return true;
            }

            case EInternedAttributeKey::MergeJobRateLimit: {
                BuildYsonFluently(consumer)
                    .Value(account->GetMergeJobRateLimit());
                return true;
            }

            case EInternedAttributeKey::Abc: {
                if (account->GetAbcConfig()) {
                    BuildYsonFluently(consumer)
                        .Value(*account->GetAbcConfig());
                    return true;
                } else {
                    return false;
                }
            }

            case EInternedAttributeKey::FolderId: {
                if (account->GetFolderId()) {
                    BuildYsonFluently(consumer)
                        .Value(account->GetFolderId().value());
                    return true;
                } else {
                    return false;
                }
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
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        switch (key) {
            case EInternedAttributeKey::AllowChildrenLimitOvercommit: {
                if (IsRootAccount()) {
                    break;
                }

                auto overcommitAllowed = ConvertTo<bool>(value);
                securityManager->SetAccountAllowChildrenLimitOvercommit(account, overcommitAllowed);
                return true;
            }

            case EInternedAttributeKey::MergeJobRateLimit: {
                auto* user = securityManager->GetAuthenticatedUser();
                if (!securityManager->IsSuperuser(user)) {
                    THROW_ERROR_EXCEPTION(
                        NSecurityClient::EErrorCode::AuthorizationError,
                        "Access denied: only superusers can change merge job rate limit");
                }

                auto mergeJobRateLimit = ConvertTo<int>(value);
                account->SetMergeJobRateLimit(mergeJobRateLimit);
                return true;
            }

            case EInternedAttributeKey::ResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }

                auto limits = ConvertTo<TSerializableClusterResourceLimitsPtr>(value);
                securityManager->TrySetResourceLimits(account, limits->ToClusterResourceLimits(chunkManager, multicellManager));
                return true;
            }

            case EInternedAttributeKey::Abc: {
                account->SetAbcConfig(ConvertTo<NObjectClient::TAbcConfigPtr>(value));
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                TString newFolderId = ConvertTo<TString>(value);
                ValidateFolderId(newFolderId);
                account->SetFolderId(std::move(newFolderId));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    virtual bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* account = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Abc: {
                account->SetAbcConfig(nullptr);
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                account->SetFolderId(std::nullopt);
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }

    void SerializeClusterResourceLimits(
        const TClusterResourceLimits& clusterResourceLimits,
        bool serializeDiskSpace,
        NYson::IYsonConsumer* consumer)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto resourceSerializer = New<TSerializableClusterResourceLimits>(
            chunkManager,
            multicellManager,
            clusterResourceLimits,
            serializeDiskSpace);
        BuildYsonFluently(consumer)
            .Value(resourceSerializer);
    }

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(TransferAccountResources);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, TransferAccountResources)
    {
        Y_UNUSED(response);

        DeclareMutating();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        auto* impl = GetThisImpl();
        auto* srcAccount = securityManager->GetAccountByNameOrThrow(request->src_account(), true /*activeLifeStageOnly*/);
        auto serializableResourceDelta = ConvertTo<TSerializableClusterResourceLimitsPtr>(
            TYsonString(request->resource_delta()));
        auto resourceDelta = serializableResourceDelta->ToClusterResourceLimits(chunkManager, multicellManager);

        context->SetRequestInfo("SrcAccount: %v, DstAccount: %v",
            srcAccount->GetName(),
            impl->GetName());

        securityManager->TransferAccountResources(srcAccount, impl, resourceDelta);

        context->Reply();

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(context);
        }
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

