#pragma once

#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/AutoPtr.h>

#include <string>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

using IConfigPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

////////////////////////////////////////////////////////////////////////////////

IConfigPtr LoadConfigFromLocalFile(const std::string& path);

}   // namespace NClickHouse
}   // namespace NYT
