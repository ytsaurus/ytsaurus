#include "stdafx.h"
#include "config.h"

#include <ytlib/hydra/config.h>

#include <ytlib/transaction_client/config.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/hive/config.h>

#include <ytlib/tablet_client/config.h>

#include <ytlib/new_table_client/config.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("masters", Masters);
    RegisterParameter("timestamp_provider", TimestampProvider);
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

