#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NObjectClient;

///////////////////////////////////////////////////////////////////////////////

void TCellDirectoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("primary_master", &TThis::PrimaryMaster)
        .Default();
    registrar.Parameter("secondary_masters", &TThis::SecondaryMasters)
        .Default();
    registrar.Parameter("master_cache", &TThis::MasterCache)
        .Default();

    registrar.Parameter("caching_object_service", &TThis::CachingObjectService)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->CachingObjectService->RateLimit = 1000000; // effective infinity
    });
    registrar.Postprocessor([] (TThis* config) {
        if (config->PrimaryMaster) {
            auto cellId = config->PrimaryMaster->CellId;
            auto primaryCellTag = CellTagFromId(cellId);
            THashSet<TCellTag> cellTags = {primaryCellTag};
            for (const auto& cellConfig : config->SecondaryMasters) {
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
    });
}

////////////////////////////////////////////////////////////////////////////////

void TCellDirectorySynchronizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sync_period", &TThis::SyncPeriod)
        .Default(TDuration::Minutes(60));
    registrar.Parameter("retry_period", &TThis::RetryPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("expire_after_successful_update_time", &TThis::ExpireAfterSuccessfulUpdateTime)
        .Alias("success_expiraton_time")
        .Default(TDuration::Minutes(20));
    registrar.Parameter("expire_after_failed_update_time", &TThis::ExpireAfterFailedUpdateTime)
        .Alias("failure_expiration_time")
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
