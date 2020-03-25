#pragma once

#include "private.h"

#include "schema.h"
#include "subquery_spec.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>

#include <yt/client/ypath/rich.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// We expose small part of storage functionality using this interface.
struct IStorageDistributor
{
public:
    virtual std::vector<TTablePtr> GetTables() const = 0;

    virtual NTableClient::TTableSchema GetSchema() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(
    TQueryContext* queryContext,
    std::vector<TTablePtr> tables);

void RegisterStorageDistributor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
