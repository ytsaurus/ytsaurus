#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/library/query/base/public.h>

#include <source_location>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IIndexSession
{
    virtual ~IIndexSession() = default;

    virtual void TryAddLoadedKey(const TObjectKey& key, bool exists) = 0;
    virtual void AddUniqueKeyOrThrow(const TObjectKey& key, const TString& indexName) = 0;
    virtual void RemoveUniqueKey(const TObjectKey& key) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class ISession
{
public:
    virtual ~ISession() = default;

    virtual IObjectTypeHandler* GetTypeHandlerOrCrash(TObjectTypeValue type) const = 0;
    virtual IObjectTypeHandler* GetTypeHandlerOrThrow(TObjectTypeValue type) const = 0;

    virtual TObject* GetObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey = {}) = 0;

    virtual void RemoveObject(TObject* object) = 0;
    virtual void RemoveObjects(std::vector<TObject*> objects) = 0;

    // For batch creations with |allowExisting|.
    virtual void ScheduleExistenceCheck(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey) = 0;
    virtual bool ExistenceCheckSuccessful(TObjectTypeValue type, TObjectKey key) = 0;

    using TLoadCallback = std::function<void(ILoadContext*)>;
    // Order of callbacks execution matter even within one priority.
    virtual void ScheduleLoad(
        TLoadCallback callback,
        ELoadPriority priority = ELoadPriority::Default) = 0;

    using TFinalizeCallback = std::function<void()>;
    virtual void ScheduleFinalize(TFinalizeCallback callback) = 0;

    using TStoreCallback = std::function<void(IStoreContext*)>;
    virtual void ScheduleStore(TStoreCallback callback) = 0;

    virtual void FlushTransaction(const TMutatingTransactionOptions& options) = 0;
    virtual void FlushLoads(std::source_location location = std::source_location::current()) = 0;

    virtual const TTransactionConfigsSnapshot& GetConfigsSnapshot() const = 0;

    virtual IIndexSession* GetOrCreateIndexSession(const TString& indexName) = 0;

    virtual TTransaction* GetOwner() const = 0;

    // For testing purposes only.
    virtual void SetTestingStorageOptions(TTestingStorageOptions options) = 0;

    virtual void ProfileObjectsAttributes() = 0;

    virtual NQueryClient::TColumnEvaluatorPtr GetObjectTableEvaluator(TObjectTypeValue type) = 0;

protected:
    friend class TTransaction;

    // This returns an incomplete object that must be finalized by the transaction.
    virtual TObject* CreateObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey,
        bool allowExisting) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISession> CreateSession(
    NMaster::IBootstrap* bootstrap,
    TTransaction* owner,
    TTransactionConfigsSnapshot configsSnapshot,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
