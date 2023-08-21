#pragma once

#include "private.h"

#include "conversion.h"
#include "subquery_spec.h"

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/client/ypath/rich.h>

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

    virtual NTableClient::TTableSchemaPtr GetSchema() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageDistributor(
    DB::ContextPtr context,
    std::vector<TTablePtr> tables);

void RegisterStorageDistributor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
