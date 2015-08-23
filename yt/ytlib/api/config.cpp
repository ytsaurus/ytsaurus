#include "stdafx.h"
#include "config.h"

#include <ytlib/transaction_client/config.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/table_client/config.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NApi {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TMasterConnectionConfig::TMasterConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("network_name", NetworkName)
        .Default(NNodeTrackerClient::InterconnectNetworkName);
    RegisterParameter("primary_master", PrimaryMaster);
    RegisterParameter("secondary_masters", SecondaryMasters)
        .Default();
    RegisterParameter("master_cache", MasterCache)
        .Default();
    RegisterParameter("enable_read_from_followers", EnableReadFromFollowers)
        .Default(false);
    RegisterParameter("timestamp_provider", TimestampProvider)
        .Default();
    RegisterParameter("cell_directory", CellDirectory)
        .DefaultNew();
    RegisterParameter("scheduler", Scheduler)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();
    RegisterParameter("table_mount_cache", TableMountCache)
        .DefaultNew();

    RegisterParameter("query_evaluator", QueryEvaluator)
        .DefaultNew();
    RegisterParameter("query_timeout", QueryTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("query_response_codec", QueryResponseCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("default_input_row_limit", DefaultInputRowLimit)
        .GreaterThan(0)
        .Default(100000000);
    RegisterParameter("default_output_row_limit", DefaultOutputRowLimit)
        .GreaterThan(0)
        .Default(1000000);

    RegisterParameter("column_evaluator_cache", ColumnEvaluatorCache)
        .DefaultNew();

    RegisterParameter("write_timeout", WriteTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("write_request_codec", WriteRequestCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_write_request", MaxRowsPerWriteRequest)
        .GreaterThan(0)
        .Default(1000);
    RegisterParameter("max_rows_per_transaction", MaxRowsPerTransaction)
        .GreaterThan(0)
        .Default(100000);

    RegisterParameter("lookup_timeout", LookupTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("lookup_request_codec", LookupRequestCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("lookup_response_codec", LookupResponseCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_read_request", MaxRowsPerReadRequest)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("enable_udf", EnableUdf)
        .Default(false);
    RegisterParameter("udf_registry_path", UdfRegistryPath)
        .Default("//tmp/udfs");

    RegisterValidator([&] () {
        const auto& cellId = PrimaryMaster->CellId;
        auto primaryCellTag = CellTagFromId(PrimaryMaster->CellId);
        yhash_set<TCellTag> cellTags = {primaryCellTag};
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
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

