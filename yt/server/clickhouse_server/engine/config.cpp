#include "config.h"

#include <Poco/Util/XMLConfiguration.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

IConfigPtr LoadConfigFromLocalFile(const std::string& path)
{
    if (!path.empty()) {
        return new Poco::Util::XMLConfiguration(path);
    }
    return nullptr;
}

}   // namespace NClickHouse
}   // namespace NYT
