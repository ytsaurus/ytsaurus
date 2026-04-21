#include "process.h"

#include <util/stream/file.h>

#include <util/string/split.h>

namespace NYT::NContainers {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TString> ParseProcessCGroups(const TString& str)
{
    THashMap<TString, TString> result;

    TVector<TString> values;
    StringSplitter(str.data()).SplitBySet(":\n").SkipEmpty().Collect(&values);
    for (size_t i = 0; i + 2 < values.size(); i += 3) {
        // Check format.
        FromString<int>(values[i]);

        const auto& subsystemsSet = values[i + 1];
        const auto& name = values[i + 2];

        TVector<TString> subsystems;
        StringSplitter(subsystemsSet.data()).Split(',').SkipEmpty().Collect(&subsystems);
        for (const auto& subsystem : subsystems) {
            if (!subsystem.StartsWith("name=")) {
                int start = 0;
                if (name.StartsWith("/")) {
                    start = 1;
                }
                result[subsystem] = name.substr(start);
            }
        }
    }

    return result;
}

THashMap<TString, TString> GetProcessCGroups(pid_t pid)
{
    auto cgroupsPath = Format("/proc/%v/cgroup", pid);
    auto rawCgroups = TFileInput{cgroupsPath}.ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

THashMap<TString, TString> GetSelfProcessCGroups()
{
    auto rawCgroups = TFileInput{"/proc/self/cgroup"}.ReadAll();
    return ParseProcessCGroups(rawCgroups);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers

