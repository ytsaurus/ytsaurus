#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

Poco::AutoPtr<Poco::Util::AbstractConfiguration> CreateDocumentConfig(
    NInterop::IDocumentPtr document);

} // namespace NClickHouse
} // namespace NYT
