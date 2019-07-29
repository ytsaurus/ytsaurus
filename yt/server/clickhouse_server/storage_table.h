#pragma once

#include "private.h"

#include <Interpreters/Cluster.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageTable(TClickHouseTablePtr table);

void RegisterStorageTable();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
