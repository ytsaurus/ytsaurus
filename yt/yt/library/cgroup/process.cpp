#include "process.h"

#include <util/stream/file.h>

#include <util/string/split.h>

namespace NYT::NContainers {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

THashMap<std::string, std::string> ParseProcessCGroups(const TString& str)
{
    THashMap<std::string, std::string> result;

    std::vector<std::string> values;
    StringSplitter(str.data()).SplitBySet(":\n").SkipEmpty().Collect(&values);
    for (int index = 0; index + 2 < std::ssize(values); index += 3) {
        // Check format.
        FromString<int>(values[index]);

        const auto& subsystemsSet = values[index + 1];
        const auto& name = values[index + 2];

        std::vector<std::string> subsystems;
        StringSplitter(subsystemsSet.data()).Split(',').SkipEmpty().Collect(&subsystems);
        for (const auto& subsystem : subsystems) {
            if (!subsystem.starts_with("name=")) {
                int start = 0;
                if (name.starts_with("/")) {
                    start = 1;
                }
                result[subsystem] = name.substr(start);
            }
        }
    }

    return result;
}

THashMap<std::string, std::string> GetProcessCGroups(pid_t pid)
{
    auto cgroupsPath = Format("/proc/%v/cgroup", pid);
    auto rawCgroups = TFileInput{cgroupsPath}.ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

THashMap<std::string, std::string> GetSelfProcessCGroups()
{
    auto rawCgroups = TFileInput{"/proc/self/cgroup"}.ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
