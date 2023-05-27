#pragma once

#include "tablet_statistics.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletServant
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State, ETabletState::Unmounted);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, MountRevision);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, MountTime);

public:
    operator bool() const;

    void Clear();

    void Swap(TTabletServant* other);

    void Persist(const NCellMaster::TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

class TTabletBase
    : public NObjectServer::TObject
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Index, -1);

    //! Only makes sense for mounted tablets.
    DEFINE_BYVAL_RW_PROPERTY(NTabletClient::EInMemoryMode, InMemoryMode);

    DEFINE_BYREF_RW_PROPERTY(TTabletServant, Servant);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, SettingsRevision);

    //! Only makes sense for unmounted tablets.
    DEFINE_BYVAL_RW_PROPERTY(bool, WasForcefullyUnmounted);

    DEFINE_BYVAL_RW_PROPERTY(TTabletAction*, Action);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, StoresUpdatePreparedTransaction);

public:
    using TObject::TObject;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

    ETabletState GetState() const;
    void SetState(ETabletState state);
    // COMPAT(gritukan)
    void SetStateCompat(ETabletState state);

    ETabletState GetExpectedState() const;
    void SetExpectedState(ETabletState state);
    // COMPAT(gritukan)
    void SetExpectedStateCompat(ETabletState state);

    TTabletOwnerBase* GetOwner() const;
    virtual void SetOwner(TTabletOwnerBase* owner);
    // COMPAT(gritukan)
    void SetOwnerCompat(TTabletOwnerBase* owner);

    virtual void CopyFrom(const TTabletBase& other);

    void ValidateMountRevision(NHydra::TRevision mountRevision);

    bool IsActive() const;

    NChunkServer::TChunkList* GetChunkList();
    const NChunkServer::TChunkList* GetChunkList() const;

    NChunkServer::TChunkList* GetHunkChunkList();
    const NChunkServer::TChunkList* GetHunkChunkList() const;

    NChunkServer::TChunkList* GetChunkList(NChunkServer::EChunkListContentType type);
    const NChunkServer::TChunkList* GetChunkList(NChunkServer::EChunkListContentType type) const;

    TTabletServant* FindServant(TTabletCellId cellId);
    const TTabletServant* FindServant(TTabletCellId cellId) const;

    TTabletServant* FindServant(NHydra::TRevision mountRevision);
    const TTabletServant* FindServant(NHydra::TRevision mountRevision) const;

    TTabletCell* GetCell() const;

    i64 GetTabletStaticMemorySize(NTabletClient::EInMemoryMode inMemoryMode) const;
    i64 GetTabletStaticMemorySize() const;

    virtual i64 GetTabletMasterMemoryUsage() const;

    virtual TTabletStatistics GetTabletStatistics() const = 0;

    virtual void ValidateMount(bool freeze);
    virtual void ValidateUnmount();

    virtual void ValidateFreeze() const;
    virtual void ValidateUnfreeze() const;

    virtual void ValidateReshard() const;
    virtual void ValidateReshardRemove() const;

    int GetTabletErrorCount() const;
    void SetTabletErrorCount(int tabletErrorCount);

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const override;

private:
    TTabletOwnerBase* Owner_ = nullptr;

    ETabletState State_ = ETabletState::Unmounted;
    ETabletState ExpectedState_ = ETabletState::Unmounted;

    int TabletErrorCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
