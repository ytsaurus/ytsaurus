#pragma once

#include "public.h"

#include "object_detail.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/journal_client/journal_hunk_chunk_writer.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct THunkStorageSettings
{
    THunkStorageMountConfigPtr MountConfig;
    THunkStoreWriterConfigPtr StoreWriterConfig;
    THunkStoreWriterOptionsPtr StoreWriterOptions;
};

////////////////////////////////////////////////////////////////////////////////

class THunkTablet
    : public TObjectBase
    , public TRefTracked<THunkTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);

    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, MountRevision);

    DEFINE_BYVAL_RW_PROPERTY(NHiveServer::TAvenueEndpointId, MasterAvenueEndpointId);

    DEFINE_BYREF_RW_PROPERTY(THunkStorageMountConfigPtr, MountConfig, New<THunkStorageMountConfig>());
    DEFINE_BYREF_RW_PROPERTY(THunkStoreWriterConfigPtr, StoreWriterConfig, New<THunkStoreWriterConfig>());
    DEFINE_BYREF_RW_PROPERTY(THunkStoreWriterOptionsPtr, StoreWriterOptions, New<THunkStoreWriterOptions>());

    DEFINE_BYVAL_RW_PROPERTY(THunkStorePtr, ActiveStore);
    DEFINE_BYREF_RW_PROPERTY(THashSet<THunkStorePtr>, AllocatedStores);
    DEFINE_BYREF_RW_PROPERTY(THashSet<THunkStorePtr>, PassiveStores);

public:
    THunkTablet(
        IHunkTabletHostPtr host,
        TTabletId tabletId);

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    TFuture<std::vector<NJournalClient::TJournalHunkDescriptor>> WriteHunks(
        std::vector<TSharedRef> payloads);

    void Reconfigure(const THunkStorageSettings& settings);

    THunkStorePtr FindStore(TStoreId storeId);
    THunkStorePtr GetStore(TStoreId storeId);
    THunkStorePtr GetStoreOrThrow(TStoreId storeId);

    void AddStore(THunkStorePtr store);
    void RemoveStore(const THunkStorePtr& store);

    int GetAllocatedStoreCount() const;

    bool IsReadyToUnmount(bool force = false) const;
    bool IsFullyUnlocked(bool forceUnmount = false) const;

    void OnStopLeading();
    void OnUnmount();

    void RotateActiveStore();
    void OnStoreAllocationFailed(const TError& error);

    void LockTransaction(TTransactionId transactionId);
    bool TryUnlockTransaction(TTransactionId transactionId);
    TTransactionId GetLockTransactionId() const;

    bool TryLockScan();
    void UnlockScan();

    void ValidateMountRevision(NHydra::TRevision mountRevision) const;
    void ValidateMounted(NHydra::TRevision mountRevision) const;

    void BuildOrchidYson(NYson::IYsonConsumer* consumer) const;

    const NLogging::TLogger& GetLogger() const;

private:
    const IHunkTabletHostPtr Host_;

    const NLogging::TLogger Logger;

    THashMap<TStoreId, THunkStorePtr> IdToStore_;

    TTransactionId LockTransactionId_;

    bool LockedByScan_ = false;

    //! Number of active writes. Tablet cannot be unmounted when
    //! write is in progress.
    int WriteLockCount_ = 0;

    TPromise<THunkStorePtr> ActiveStorePromise_;
    TFuture<THunkStorePtr> ActiveStoreFuture_;

    void MakeAllStoresPassive();

    void RenewPromise();
    void RenewPromise(TError error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
