#pragma once

#include "public.h"

#include <core/logging/log.h>

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NVersionedTableClient::TRowBuffer;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger QueryClientLogger;

NLogging::TLogger BuildLogger(const TConstQueryPtr& query);

extern size_t LogThreshold;

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

