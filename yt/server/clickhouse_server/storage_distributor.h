#pragma once

#include "private.h"

#include "table_schema.h"
#include "subquery_spec.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(std::vector<TClickHouseTablePtr> tables);

void RegisterStorageDistributor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
