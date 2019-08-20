#pragma once

#include "private.h"

#include "table_schema.h"
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
    virtual std::vector<NYPath::TRichYPath> GetTablePaths() const = 0;

    virtual TClickHouseTableSchema GetClickHouseSchema() const = 0;

    virtual NTableClient::TTableSchema GetSchema() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(std::vector<TClickHouseTablePtr> tables);

void RegisterStorageDistributor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
