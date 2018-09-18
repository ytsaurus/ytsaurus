#pragma once

#include "public.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IDocumentPtr CreateDocument(NYTree::INodePtr documentNode);

} // namespace NClickHouse
} // namespace NYT
