#pragma once

#include <atomic>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_set>
#include <base/defines.h>


namespace DBPoco { class URI; }
namespace DBPoco { namespace Util { class AbstractConfiguration; } }

namespace DB
{
class RemoteHostFilter
{
/**
 * This class checks if URL is allowed.
 * If primary_hosts and regexp_hosts are empty all urls are allowed.
 */
public:
    void checkURL(const DBPoco::URI & uri) const; /// If URL not allowed in config.xml throw UNACCEPTABLE_URL Exception

    void setValuesFromConfig(const DBPoco::Util::AbstractConfiguration & config);

    void checkHostAndPort(const std::string & host, const std::string & port) const; /// Does the same as checkURL, but for host and port.

private:
    std::atomic_bool is_initialized = false;

    mutable std::mutex hosts_mutex;
    std::unordered_set<std::string> primary_hosts TSA_GUARDED_BY(hosts_mutex);  /// Allowed primary (<host>) URL from config.xml
    std::vector<std::string> regexp_hosts TSA_GUARDED_BY(hosts_mutex);          /// Allowed regexp (<hots_regexp>) URL from config.xml

    /// Checks if the primary_hosts and regexp_hosts contain str. If primary_hosts and regexp_hosts are empty return true.
    bool checkForDirectEntry(const std::string & str) const;
};
}
