#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletAction
    : public NObjectServer::TObject
    , public TRefTracked<TTabletAction>
{
public:
    //! Action kind: move, reshard.
    DEFINE_BYVAL_RW_PROPERTY(ETabletActionKind, Kind);

    //! Current state.
    DEFINE_BYVAL_RW_PROPERTY(ETabletActionState, State);

    //! Participating tablets.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTabletBase*>, Tablets);

    //! Tablet cells to mount tablet into (if present).
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTabletCell*>, TabletCells);

    //! Pivot keys for reshard (if present).
    DEFINE_BYREF_RW_PROPERTY(std::vector<NTableClient::TLegacyOwningKey>, PivotKeys);

    //! Desired number of tablets (for reshard).
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, TabletCount);

    //! Skip initial freezing pass while performing tablet action.
    DEFINE_BYVAL_RW_PROPERTY(bool, SkipFreezing);

    //! Freeze tablets when action is completed.
    DEFINE_BYVAL_RW_PROPERTY(bool, Freeze);

    //! Contains error if tablet action failed.
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

    //! Random guid to help connecting tablet balancer logs with tablet actions.
    DEFINE_BYVAL_RW_PROPERTY(TGuid, CorrelationId);

    //! When finished, action will not be destroyed until this time.
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ExpirationTime);

    //! Tablet cell bundle of the participating tablets.
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellBundle*, TabletCellBundle);

    //! When finished, action will not be destroyed for the specified amount of time.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, ExpirationTimeout);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    //! Save ids of the participating tablets for future retrieval.
    //! Subsequent calls have no effect.
    void SaveTabletIds();
    std::vector<TTabletId> GetTabletIds() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    bool IsFinished() const;

private:
    //! Ids of tablets for a finished action. For a completed action
    //! these are always resulting tablets. For a failed one these might be
    //! either resulting or initial ones.
    std::optional<std::vector<TTabletId>> SavedTabletIds_;
};

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TTabletAction& action);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
