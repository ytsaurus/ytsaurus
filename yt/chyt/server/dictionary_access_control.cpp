#include "dictionary_access_control.h"

#include "dictionary_source.h"
#include "host.h"

#include <library/cpp/iterator/zip.h>
#include <library/cpp/iterator/enumerate.h>

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/permission.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>

#include <Access/AccessControl.h>
#include <Access/AccessRights.h>
#include <Access/User.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NThreading;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDictionaryAccessControl
    : public IDictionaryAccessControl, public TAsyncExpiringCache<std::string, void>
{
public:
    TDictionaryAccessControl(TPermissionCachePtr permissionCache, TDictionaryAccessControlConfigPtr config, IInvokerPtr invoker)
        : TAsyncExpiringCache(config->CacheConfig, invoker)
        , Invoker_(std::move(invoker))
        , ProcessSourcesExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TDictionaryAccessControl::CollectStorageIds, MakeWeak(this)),
            config->CollectLoadedDictionariesPeriod))
        , PermissionCache_(std::move(permissionCache))
    { }

    void SyncUserAccessRights(const std::string& user) override
    {
        YT_VERIFY(ServerContext_);
        Y_UNUSED(WaitFor(Get(user)));
    }

    void Start(DB::ContextMutablePtr serverContext) override
    {
        ServerContext_ = serverContext;
        ProcessSourcesExecutor_->Start();
    }

private:
    void CollectStorageIds()
    {
        WriterGuard(SourceToStorageIdsLock_);

        SourceToStorageIds_.clear();
        FullAccessStorageIds_.clear();

        auto results = ServerContext_->getExternalDictionariesLoader().getLoadResults();
        for (const auto& result : results) {
            auto dictionary = std::dynamic_pointer_cast<const DB::IDictionary>(result.object);
            if (!dictionary) {
                continue;
            }

            if (auto path = GetTableDictionarySourcePath(dictionary->getSource())) {
                auto it = SourceToStorageIds_.emplace(*path, std::vector<DB::StorageID>()).first;
                it->second.push_back(dictionary->getDictionaryID());
            } else {
                FullAccessStorageIds_.push_back(dictionary->getDictionaryID());
            }
        }
    }

    TFuture<void> DoGet(const std::string& user, bool isPeriodicUpdate) noexcept override
    {
        return DoGetMany({user}, isPeriodicUpdate).AsVoid();
    }

    TFuture<std::vector<TError>> DoGetMany(
        const std::vector<std::string>& users,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        std::vector<TPermissionKey> permissionCacheKeys;
        THashMap<std::string, size_t> userToIndex;
        {
            auto guard = ReaderGuard(SourceToStorageIdsLock_);
            permissionCacheKeys.reserve(users.size() * SourceToStorageIds_.size());
            for (const auto& [index, user] : Enumerate(users)) {
                userToIndex[user] = index;
                for (const auto& source : SourceToStorageIds_) {
                    permissionCacheKeys.push_back(TPermissionKey{
                        .Path = source.first,
                        .User = user,
                        .Permission = EPermission::Read,
                        .CallerIsRlsAware = true,
                    });
                }
            }
        }

        auto permissionResult = WaitFor(PermissionCache_->GetMany(permissionCacheKeys))
            .ValueOrDefault(std::vector<TErrorOr<TPermissionValue>>(permissionCacheKeys.size(), TError("", TError::DisableFormat)));


        std::vector<DB::AccessRights> userGrantedRights(users.size());
        std::vector<DB::AccessRights> userRevokedRights(users.size());
        {
            auto guard = ReaderGuard(SourceToStorageIdsLock_);

            for (size_t userIndex = 0; userIndex < users.size(); ++userIndex) {
                for (const auto& storageId : FullAccessStorageIds_) {
                    userGrantedRights[userIndex].grant(DB::AccessType::dictGet, storageId.database_name, storageId.table_name);
                }
            }

            for (const auto& [permissionKey, result] : Zip(permissionCacheKeys, permissionResult)) {
                auto rightsIndex = userToIndex[permissionKey.User];
                for (const auto& storageId : GetOrDefault(SourceToStorageIds_, permissionKey.Path)) {
                    auto* rights = &userGrantedRights[rightsIndex];
                    if (!result.IsOK() || result.Value().RowLevelAcl) {
                        rights = &userRevokedRights[rightsIndex];
                    }
                    rights->grant(DB::AccessType::dictGet, storageId.database_name, storageId.table_name);
                }
            }
        }

        auto& accessControl = ServerContext_->getAccessControl();
        for (size_t userIndex = 0; userIndex < users.size(); ++userIndex) {
            auto userId = accessControl.find(DB::AccessEntityType::USER, users[userIndex]);
            YT_VERIFY(userId);
            accessControl.tryUpdate(*userId, [&] (const DB::AccessEntityPtr& entity, const DB::UUID&) {
                auto user = std::static_pointer_cast<DB::User>(entity->clone());
                user->access.makeUnion(userGrantedRights[userIndex]);
                user->access.makeDifference(userRevokedRights[userIndex]);
                return user;
            });
        }

        return MakeFuture(std::vector<TError>(users.size()));
    }

private:
    IInvokerPtr Invoker_;
    DB::ContextMutablePtr ServerContext_;
    TPeriodicExecutorPtr ProcessSourcesExecutor_;

    NSecurityClient::TPermissionCachePtr PermissionCache_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SourceToStorageIdsLock_);
    THashMap<TYPath, std::vector<DB::StorageID>> SourceToStorageIds_;
    std::vector<DB::StorageID> FullAccessStorageIds_;
};

////////////////////////////////////////////////////////////////////////////////

IDictionaryAccessControlPtr CreateDictionaryAccessControl(
    TPermissionCachePtr permissionCache,
    TDictionaryAccessControlConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TDictionaryAccessControl>(std::move(permissionCache), std::move(config), std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
