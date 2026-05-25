#include "account_proxy.h"

#include "account.h"
#include "config.h"
#include "helpers.h"
#include "security_manager.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/map_object_proxy.h>
#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

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
using namespace NServer;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

struct TChunkMergerUsage
{
    int NodeTraversals;
    i64 MergeJobThrottlerAvailability;
    std::optional<double> MergeJobThrottlerLimit;
};

void Deserialize(TChunkMergerUsage& usage, INodePtr node)
{
    auto mapNode = node->AsMap();
    Deserialize(usage.NodeTraversals, mapNode->GetChildOrThrow("node_traversals"));
    Deserialize(usage.MergeJobThrottlerAvailability, mapNode->GetChildOrThrow("merge_job_throttler_availability"));
}

void Deserialize(TChunkMergerUsage& usage, TYsonPullParserCursor* cursor)
{
    Deserialize(usage, ExtractTo<INodePtr>(cursor));
}

void Serialize(const TChunkMergerUsage& usage, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("node_traversals").Value(usage.NodeTraversals)
            .Item("merge_job_throttler_availability").Value(usage.MergeJobThrottlerAvailability)
        .EndMap();
}

TChunkMergerUsage GetChunkMergerUsageFromAccount(const TAccount* account)
{
    const auto& mergeJobThrottler = account->MergeJobThrottler();
    return TChunkMergerUsage{
        .NodeTraversals = account->GetChunkMergerNodeTraversals(),
        .MergeJobThrottlerAvailability = mergeJobThrottler->GetAvailable(),
        .MergeJobThrottlerLimit = mergeJobThrottler->GetLimit(),
    };
}

////////////////////////////////////////////////////////////////////////////////

class TAccountProxy
    : public TNonversionedMapObjectProxyBase<TAccount>
{
private:
    using TBase = TNonversionedMapObjectProxyBase<TAccount>;
    using TBasePtr = TIntrusivePtr<TBase>;

public:
    using TNonversionedMapObjectProxyBase::TNonversionedMapObjectProxyBase;

protected:
    std::unique_ptr<TNonversionedMapObjectFactoryBase<TAccount>> CreateObjectFactory() const override
    {
        return std::make_unique<TAccountFactory>(Bootstrap_);
    }

    TBasePtr ResolveNameOrThrow(const std::string& name) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccountByNameOrThrow(name, true /*activeLifeStageOnly*/);
        return GetProxy(account);
    }

    void ValidateBeforeAttachChild(
        const std::string& key,
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
        TAccount* DoCreateObject(IAttributeDictionary* attributes) override
        {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto hintId = attributes->GetAndRemove("hint_id", NullObjectId);
            return securityManager->CreateAccount(hintId);
        }
    };

    void ValidateRemoval() override
    {
        const auto* account = GetThisImpl();
        if (account->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in account %Qv",
                account->GetName());
        }

        if (account->ClusterStatistics() != TAccountStatistics::Empty) {
            THROW_ERROR_EXCEPTION("Cannot remove an account %Qv because its usage is not zero (AccountUsage: %v)",
                account->GetName(),
                account->ClusterStatistics());
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->ValidateAccountRemoval(account);

        TBase::ValidateRemoval();
    }

    void ValidateChildNameAvailability(const std::string& childName) override
    {
        TNonversionedMapObjectProxyBase::ValidateChildNameAvailability(childName);

        if (Bootstrap_->GetSecurityManager()->FindAccountByName(childName, /*activeLifeStageOnly*/ false)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Account %Qv already exists",
                childName);
        }
    }

    bool IsSuperuser()
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->GetAuthenticatedUser();
        return securityManager->IsSuperuser(user);
    }

    void ValidateSuperuser(TInternedAttributeKey key)
    {
        if (!IsSuperuser()) {
            THROW_ERROR_EXCEPTION(
                NSecurityClient::EErrorCode::AuthorizationError,
                "Access denied: only superusers can change %Qv",
                key.Unintern());
        }
    }

    bool IsRootAccount() const
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return GetThisImpl() == securityManager->GetRootAccount();
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        const auto* account = GetThisImpl();
        TBase::ListSystemAttributes(descriptors);

        auto isRootAccount = IsRootAccount();
        auto isSuperuser = IsSuperuser();

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
        descriptors->emplace_back(EInternedAttributeKey::ResourceLimits)
            .SetPresent(!isRootAccount)
            .SetWritable(!isRootAccount)
            .SetReplicated(true)
            .SetMandatory(true);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ViolatedResourceLimits)
            .SetOpaque(true)
            .SetPresent(!isRootAccount));

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveResourceUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveCommittedResourceUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RecursiveViolatedResourceLimits)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalChildrenResourceLimits)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MergeJobRateLimit)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerNodeTraversalConcurrency)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerCriteria)
            .SetWritable(true)
            .SetWritePermission(EPermission::Administer)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(!account->ChunkMergerCriteria().IsEmpty()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AllowUsingChunkMerger)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkMergerUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MulticellChunkMergerUsage)
            .SetOpaque(true));
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

        descriptors->emplace_back(EInternedAttributeKey::EnableChunkReincarnation)
            .SetWritable(true)
            .SetReplicated(true);

        descriptors->emplace_back(EInternedAttributeKey::BackupConfig)
            .SetWritable(isSuperuser)
            .SetReplicated(true)
            .SetOpaque(true)
            .SetRemovable(true);
        descriptors->emplace_back(EInternedAttributeKey::BackupSourceAccounts)
            .SetOpaque(true);

        if (Bootstrap_->GetConfig()->ExposeTestingFacilities) {
            descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TransientMasterMemoryUsage));
        }
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto* account = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::ResourceUsage:
            case EInternedAttributeKey::CommittedResourceUsage:
            case EInternedAttributeKey::RecursiveResourceUsage:
            case EInternedAttributeKey::RecursiveCommittedResourceUsage: {
                auto committed =
                    key == EInternedAttributeKey::CommittedResourceUsage ||
                    key == EInternedAttributeKey::RecursiveCommittedResourceUsage;
                auto recursive =
                    key == EInternedAttributeKey::RecursiveResourceUsage ||
                    key == EInternedAttributeKey::RecursiveCommittedResourceUsage;

                if (!recursive && IsRootAccount()) {
                    break;
                }

                SerializeAccountClusterResourceUsage(
                    account,
                    committed,
                    recursive,
                    consumer,
                    Bootstrap_);

                return true;
            }

            case EInternedAttributeKey::MulticellStatistics: {
                if (IsRootAccount()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .DoMapFor(account->MulticellStatistics(), [&] (TFluentMap fluent, const std::pair<TCellTag, const TAccountStatistics&>& pair) {
                        fluent.Item(multicellManager->GetMasterCellName(pair.first));
                        Serialize(pair.second, fluent.GetConsumer(), Bootstrap_);
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
                SerializeClusterResourceLimits(
                    account->ClusterResourceLimits(),
                    consumer,
                    Bootstrap_,
                    /*serializeDiskSpace*/ true);
                return true;

            case EInternedAttributeKey::ViolatedResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }

                auto violatedLimits = account->GetViolatedResourceLimits(
                    Bootstrap_,
                    Bootstrap_->GetConfigManager()->GetConfig()->SecurityManager->EnableTabletResourceValidation);
                SerializeViolatedClusterResourceLimitsInBooleanFormat(
                    violatedLimits,
                    consumer,
                    Bootstrap_,
                    /*serializeDiskSpace*/ true);

                return true;
            }

            case EInternedAttributeKey::RecursiveViolatedResourceLimits: {
                auto violatedLimits = account->GetRecursiveViolatedResourceLimits(
                    Bootstrap_,
                    Bootstrap_->GetConfigManager()->GetConfig()->SecurityManager->EnableTabletResourceValidation);
                SerializeViolatedClusterResourceLimits(
                    violatedLimits,
                    consumer,
                    Bootstrap_);

                return true;
            }

            case EInternedAttributeKey::TotalChildrenResourceLimits: {
                auto resourceLimits = account->ComputeTotalChildrenLimits();
                SerializeClusterResourceLimits(resourceLimits, consumer, Bootstrap_, /*serializeDiskSpace*/ true);
                return true;
            }

            case EInternedAttributeKey::MergeJobRateLimit: {
                BuildYsonFluently(consumer)
                    .Value(account->GetMergeJobRateLimit());
                return true;
            }

            case EInternedAttributeKey::ChunkMergerNodeTraversalConcurrency: {
                BuildYsonFluently(consumer)
                    .Value(account->GetChunkMergerNodeTraversalConcurrency());
                return true;
            }

            case EInternedAttributeKey::ChunkMergerCriteria: {
                BuildYsonFluently(consumer)
                    .Value(account->ChunkMergerCriteria());
                return true;
            }

            case EInternedAttributeKey::AllowUsingChunkMerger: {
                BuildYsonFluently(consumer)
                    .Value(account->GetAllowUsingChunkMerger());
                return true;
            }

            case EInternedAttributeKey::ChunkMergerUsage: {
                BuildYsonFluently(consumer)
                    .Value(GetChunkMergerUsageFromAccount(account));
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

            case EInternedAttributeKey::EnableChunkReincarnation:
                BuildYsonFluently(consumer)
                    .Value(account->GetEnableChunkReincarnation());
                return true;

            case EInternedAttributeKey::TransientMasterMemoryUsage:
                if (!Bootstrap_->GetConfig()->ExposeTestingFacilities) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(account->DetailedMasterMemoryUsage());
                return true;

            case EInternedAttributeKey::BackupConfig:
                if (account->GetBackupConfig()) {
                    SerializeAccountBackupConfig(
                        *account->GetBackupConfig(),
                        consumer,
                        Bootstrap_);

                    return true;
                } else {
                    return false;
                }

            case EInternedAttributeKey::BackupSourceAccounts:
                BuildYsonFluently(consumer)
                    .Value(securityManager->GetBackupSourceAccountNames(account));
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        auto* account = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::MulticellChunkMergerUsage: {
                if (!Bootstrap_->IsPrimaryMaster()) {
                    break;
                }

                std::vector<TFuture<std::pair<TCellTag, TChunkMergerUsage>>> asyncResults;
                asyncResults.push_back(MakeFuture(std::make_pair(Bootstrap_->GetPrimaryCellTag(), GetChunkMergerUsageFromAccount(account))));
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto secondaryCellTags = multicellManager->GetSecondaryCellTags();

                for (auto cellTag : secondaryCellTags) {
                    asyncResults.push_back(GetRemoteChunkMergerUsage(cellTag, account));
                }

                return AllSucceeded(asyncResults).Apply(
                    BIND([] (const std::vector<std::pair<TCellTag, TChunkMergerUsage>>& results) {
                        return BuildYsonStringFluently()
                            .DoMapFor(results, [] (TFluentMap map, const std::pair<TCellTag, TChunkMergerUsage>& response) {
                                map.Item(Format("%v", response.first))
                                    .Value(response.second);
                            });
                    }));
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttributeAsync(key);
    }

    TFuture<std::pair<TCellTag, TChunkMergerUsage>> GetRemoteChunkMergerUsage(TCellTag cellTag, const TAccount* account)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto proxy = NObjectClient::TObjectServiceProxy::FromDirectMasterChannel(
            multicellManager->GetMasterChannelOrThrow(cellTag, NHydra::EPeerKind::Follower));
        auto batchReq = proxy.ExecuteBatch();

        auto accountId = GetId();

        auto req = TYPathProxy::Get(account->GetObjectPath() + "/@chunk_merger_usage");
        batchReq->AddRequest(req);

        return batchReq->Invoke()
            .Apply(BIND([=] (const NObjectClient::TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                auto cumulativeError = GetCumulativeError(batchRspOrError);

                THROW_ERROR_EXCEPTION_IF_FAILED(cumulativeError, "Error fetching chunk merger usage of account %v from cell %v",
                    accountId,
                    cellTag);

                const auto& batchRsp = batchRspOrError.Value();
                auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(0);
                const auto& rsp = rspOrError.Value();
                return std::pair(cellTag, ConvertTo<TChunkMergerUsage>(TYsonString(rsp->value())));
            }).AsyncVia(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override
    {
        auto* account = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

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
                ValidateSuperuser(key);

                auto mergeJobRateLimit = ConvertTo<int>(value);
                if (mergeJobRateLimit < 0) {
                    THROW_ERROR_EXCEPTION("%Qv cannot be negative", key.Unintern());
                }
                account->SetMergeJobRateLimit(mergeJobRateLimit);
                return true;
            }

            case EInternedAttributeKey::ChunkMergerNodeTraversalConcurrency: {
                ValidateSuperuser(key);

                auto chunkMergerNodeTraversalConcurrency = ConvertTo<int>(value);
                if (chunkMergerNodeTraversalConcurrency < 0) {
                    THROW_ERROR_EXCEPTION("%Qv cannot be negative", key.Unintern());
                }
                account->SetChunkMergerNodeTraversalConcurrency(chunkMergerNodeTraversalConcurrency);
                return true;
            }

            case EInternedAttributeKey::ChunkMergerCriteria: {
                ValidateSuperuser(key);

                auto criteria = ConvertTo<TChunkMergerCriteria>(value);
                criteria.Validate();
                account->ChunkMergerCriteria() = criteria;
                return true;
            }

            case EInternedAttributeKey::AllowUsingChunkMerger: {
                ValidateSuperuser(key);

                auto allowUsingChunkMerger = ConvertTo<bool>(value);
                account->SetAllowUsingChunkMerger(allowUsingChunkMerger);
                return true;
            }

            case EInternedAttributeKey::ResourceLimits: {
                if (IsRootAccount()) {
                    break;
                }

                TClusterResourceLimits limits;
                DeserializeClusterResourceLimits(
                    limits,
                    ConvertToNode(value),
                    Bootstrap_,
                    /*zeroByDefault*/ false);
                securityManager->TrySetResourceLimits(account, limits);
                return true;
            }

            case EInternedAttributeKey::Abc: {
                account->SetAbcConfig(ConvertTo<NObjectClient::TAbcConfigPtr>(value));
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                auto newFolderId = ConvertTo<std::string>(value);
                ValidateFolderId(newFolderId);
                account->SetFolderId(std::move(newFolderId));
                return true;
            }

            case EInternedAttributeKey::EnableChunkReincarnation: {
                account->SetEnableChunkReincarnation(ConvertTo<bool>(value));
                return true;
            }

            case EInternedAttributeKey::BackupConfig: {
                ValidateSuperuser(key);

                auto backupConfig = DeserializeAccountBackupConfig(ConvertToNode(value), Bootstrap_);
                backupConfig.Validate(Bootstrap_);

                securityManager->UpdateBackupConfigForAccount(account, std::move(backupConfig));
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* account = GetThisImpl();
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::ChunkMergerCriteria: {
                account->ChunkMergerCriteria() = {};
                return true;
            }

            case EInternedAttributeKey::Abc: {
                account->SetAbcConfig(nullptr);
                return true;
            }

            case EInternedAttributeKey::FolderId: {
                account->SetFolderId(std::nullopt);
                return true;
            }

            case EInternedAttributeKey::BackupConfig: {
                ValidateSuperuser(key);
                securityManager->RemoveBackupConfigForAccount(account);
                return true;
            }

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(TransferAccountResources);
        return TBase::DoInvoke(context);
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, TransferAccountResources)
    {
        Y_UNUSED(response);

        DeclareMutating();

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto* impl = GetThisImpl();
        auto* srcAccount = securityManager->GetAccountByNameOrThrow(
            request->src_account(),
            /*activeLifeStageOnly*/ true);

        TClusterResourceLimits resourceDelta;
        DeserializeClusterResourceLimits(
            resourceDelta,
            ConvertToNode(TYsonString(request->resource_delta())),
            Bootstrap_,
            /*zeroByDefault*/ true);

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

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<TNonversionedMapObjectProxyBase<TAccount>> CreateAccountProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TAccount* account)
{
    return New<TAccountProxy>(bootstrap, metadata, account);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
