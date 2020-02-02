#include "config.h"

#include <yt/ytlib/api/native/config.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

TCellDirectoryConfig::TCellDirectoryConfig()
{
    RegisterParameter("primary_master", PrimaryMaster);
    RegisterParameter("secondary_masters", SecondaryMasters)
        .Default();
    RegisterParameter("master_cache", MasterCache)
        .Default();

    if (PrimaryMaster) {
        auto cellId = PrimaryMaster->CellId;
        auto primaryCellTag = CellTagFromId(cellId);
        THashSet<TCellTag> cellTags = {primaryCellTag};
        for (const auto& cellConfig : SecondaryMasters) {
            if (ReplaceCellTagInId(cellConfig->CellId, primaryCellTag) != cellId) {
                THROW_ERROR_EXCEPTION("Invalid cell id %v specified for secondary master in connection configuration",
                    cellConfig->CellId);
            }
            auto cellTag = CellTagFromId(cellConfig->CellId);
            if (!cellTags.insert(cellTag).second) {
                THROW_ERROR_EXCEPTION("Duplicate cell tag %v in connection configuration",
                    cellTag);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellDirectorySynchronizerConfig::TCellDirectorySynchronizerConfig()
{
    RegisterParameter("sync_period", SyncPeriod)
        .Default(TDuration::Minutes(60));
    RegisterParameter("retry_period", RetryPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("success_expiration_time", SuccessExpirationTime)
        .Default(TDuration::Minutes(20));
    RegisterParameter("failure_expiration_time", FailureExpirationTime)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
