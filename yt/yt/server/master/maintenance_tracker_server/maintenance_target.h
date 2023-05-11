#pragma once

#include "maintenance_request.h"

#include <yt/yt/server/master/object_server/object.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateMaintenanceTargetBase
{
public:
    using TMaintenanceRequests = THashMap<TMaintenanceId, TMaintenanceRequest>;
    DEFINE_BYREF_RO_PROPERTY(TMaintenanceRequests, MaintenanceRequests);

    // COMPAT(kvk1920): Make it protected.
    bool GetMaintenanceFlag(EMaintenanceType type) const;
    //! Returns |true| if maintenance flag is changed.
    [[nodiscard]] bool ClearMaintenanceFlag(EMaintenanceType type);
    //! Returns |true| if maintenance flag is changed.
    [[nodiscard]] bool SetMaintenanceFlag(EMaintenanceType type, TString userName, TInstant timestamp);

    //! Returns |true| if maintenance flag is changed.
    // Precondition: this target has not maintenance request with such id.
    [[nodiscard]] bool AddMaintenance(TMaintenanceId id, TMaintenanceRequest request);
    //! Returns maintenance type if maintenance flag is changed.
    // Precondition: this target has maintenance request with such id.
    [[nodiscard]] std::optional<EMaintenanceType> RemoveMaintenance(TMaintenanceId id);

protected:
    // NB: These methods do not persist TObject.
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // To avoid virtual inheritance.
    friend class TMaintenanceTracker;
    virtual NObjectServer::TObject* AsObject() = 0;

private:
    TMaintenanceCounts MaintenanceCounts_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl, EMaintenanceType Type>
class TMaintenanceFlagGetter;

#define DEFINE_MAINTENANCE_FLAG_GETTER(Method, Type) \
    template <class TImpl> \
    class TMaintenanceFlagGetter<TImpl, EMaintenanceType::Type> \
    { \
    public: \
        bool Method() const \
        { \
            return static_cast<const TImpl*>(this)->GetMaintenanceFlag(EMaintenanceType::Type); \
        } \
    }

DEFINE_MAINTENANCE_FLAG_GETTER(IsBanned, Ban);
DEFINE_MAINTENANCE_FLAG_GETTER(IsDecommissioned, Decommission);
DEFINE_MAINTENANCE_FLAG_GETTER(AreWriteSessionsDisabled, DisableWriteSessions);
DEFINE_MAINTENANCE_FLAG_GETTER(AreSchedulerJobsDisabled, DisableSchedulerJobs);
DEFINE_MAINTENANCE_FLAG_GETTER(AreTabletCellsDisabled, DisableTabletCells);

#undef DEFINE_MAINTENANCE_FLAG_GETTER

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class TImpl, EMaintenanceType... Types>
class TMaintenanceTarget
    : public TNontemplateMaintenanceTargetBase
    , public NDetail::TMaintenanceFlagGetter<TMaintenanceTarget<TImpl, Types...>, Types>...
{
private:
    NObjectServer::TObject* AsObject() final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

#define MAINTENANCE_TARGET_INL_H_
#include "maintenance_target-inl.h"
#undef MAINTENANCE_TARGET_INL_H_
