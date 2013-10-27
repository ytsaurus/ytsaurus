#pragma once

#include "public.h"

#include <core/misc/common.h>
#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schema.h>

#include <ytlib/object_client/public.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
// In use.

using NVersionedTableClient::EColumnType;
using NVersionedTableClient::NProto::TColumnSchema;
typedef NVersionedTableClient::NProto::TTableSchemaExt TTableSchema;

////////////////////////////////////////////////////////////////////////////////
// Hooks for other means.


struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const NYPath::TYPath& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

