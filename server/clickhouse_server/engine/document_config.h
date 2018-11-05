#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Util::AbstractConfiguration> CreateDocumentConfig(NNative::IDocumentPtr document);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
