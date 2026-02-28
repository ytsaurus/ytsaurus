#include <yt/microservices/lib/cpp/get_snapshots/get_snapshots.h>
#include <yt/microservices/resource_usage_roren/remove_excessive.h>

#include <yt/microservices/resource_usage_roren/lib/misc.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/string/format.h>
#include <util/system/env.h>

// snapshotTimes: timestamps of snapshots
// lowerBorderOffset: first step size
// allowSnapshotEvery: first step max allowed frequency
// upperBorder: timestamp from which to start working
// stepMagnitude: step increase factor
// denominator: denominator of the geometric progression factor (total sum converges if greater than 1)
// Returns timestamps selected to be removed
std::vector<i64> ExponentialRemoval(
    std::vector<i64> snapshotTimes,
    double lowerBorderOffset,
    double allowSnapshotEvery,
    double upperBorder,
    double stepMagnitude,
    double denominator)
{
    if (snapshotTimes.size() < 2) {
        return {};
    }
    std::sort(snapshotTimes.rbegin(), snapshotTimes.rend());

    std::vector<i64> snapshotsToRemove;

    size_t idx = 0;
    while (snapshotTimes[idx] > upperBorder) {
        ++idx;
    }

    while (!snapshotTimes.empty() && snapshotTimes.back() < upperBorder) {
        auto lowerBorder = upperBorder - lowerBorderOffset;

        std::vector<double> group;
        while (idx < snapshotTimes.size() && snapshotTimes[idx] > lowerBorder) {
            group.push_back(snapshotTimes[idx]);
            ++idx;
        }

        std::optional<double> previousTime;
        for (auto it = group.crbegin(); it != group.crend(); ++it) {
            const auto& snapshotTime = *it;
            if (previousTime.has_value() && abs(previousTime.value() - snapshotTime) < allowSnapshotEvery) {
                snapshotsToRemove.push_back(snapshotTime);
            } else {
                previousTime = snapshotTime;
            }
        }

        upperBorder = lowerBorder;
        lowerBorderOffset *= stepMagnitude;
        allowSnapshotEvery *= denominator * stepMagnitude;
    }

    return snapshotsToRemove;
}

void RemoveExcessive(TString cluster,
    TString destination,
    double denominator,
    double stepSizeIncrease,
    double ignoreWindowSize,
    double firstStepWindowSize,
    double firstStepAllowedFrequency)
{
    const auto ytClient = NYT::CreateClient(cluster);
    THashMap<i64, THashMap<TString, i64>> snapshots;
    std::vector<i64> snapshotTimes;
    auto attr = NYT::TAttributeFilter();
    attr.AddAttribute("resource_usage");
    auto listOptions = NYT::TListOptions().AttributeFilter(attr);
    auto [snapshotAttrs, errors] = GetSnapshots(ytClient, destination, listOptions);
    i64 totalDiskSpace = 0;
    for (const auto& error : errors) {
        Cout << error << Endl;
    }

    for (const auto& [timestampAndSnapshotId, extensionsMap] : snapshotAttrs) {
        const auto& [timestamp, snapshotId] = timestampAndSnapshotId;
        snapshotTimes.push_back(timestamp);
        snapshots[snapshotTimes.back()] = {};
        for (const auto& [extension, attrs] : extensionsMap) {
            const auto& attrsMap = attrs.AsMap();
            auto attrsMapResourceUsageIt = attrsMap.find("resource_usage");
            Y_ABORT_IF(attrsMapResourceUsageIt == attrsMap.end());
            Y_ABORT_IF(!attrsMapResourceUsageIt->second.IsMap());
            const auto& resourceUsageMap = attrsMapResourceUsageIt->second.AsMap();
            auto resourceUsageMapDiskSpace = resourceUsageMap.find("disk_space");
            Y_ABORT_IF(resourceUsageMapDiskSpace == resourceUsageMap.end());
            Y_ABORT_IF(!resourceUsageMapDiskSpace->second.IsInt64());
            i64 diskSpace = resourceUsageMapDiskSpace->second.AsInt64();
            snapshots[snapshotTimes.back()][NYT::Format("%v/%v.%v", destination, snapshotId, extension)] = diskSpace;
            totalDiskSpace += diskSpace;
        }
    }

    auto timesToRemove = ExponentialRemoval(
        snapshotTimes,
        firstStepWindowSize,
        firstStepAllowedFrequency,
        Now().TimeT() - ignoreWindowSize,
        stepSizeIncrease,
        denominator);

    for (const auto& snapshotTime : timesToRemove) {
        auto tx = ytClient->StartTransaction();
        for (const auto& [table, diskSize] : snapshots[snapshotTime]) {
            Cout << NYT::Format("Removing table [%v] with disk_size of %v", table, diskSize) << Endl;
            tx->Remove(table);
        }
        tx->Commit();
    }

    Cout << NYT::Format("Done, %v snapshots left of total size %v.", snapshotTimes.size() - timesToRemove.size(), totalDiskSpace) << Endl;
}

const THashMap<TString, double> TIME_MAPPING = {
    {"weeks", 7 * 24 * 3600.0},
    {"days", 24 * 3600.0},
    {"hours", 3600.0},
    {"minutes", 60.0},
    {"seconds", 1.0},
    {"milliseconds", 1e-3},
    {"microseconds", 1e-6},
};

double GetTimestampFromJson(TString jsonTime)
{
    auto nodeTime = NYT::NodeFromJsonString(jsonTime);
    if (!nodeTime.IsMap()) {
        THROW_ERROR_EXCEPTION("JSON %v is not map", jsonTime);
    }
    auto& mapTime = nodeTime.AsMap();
    double ans = 0;
    for (const auto& [unit, number] : mapTime) {
        if (!number.IsDouble() && !number.IsInt64()) {
            THROW_ERROR_EXCEPTION("Value by key %v is not one of double and int64", unit);
        }
        double doubleNumber = number.IsDouble() ? number.AsDouble() : number.AsInt64();
        auto timeMappingIt = TIME_MAPPING.find(unit);
        if (timeMappingIt == TIME_MAPPING.end()) {
            THROW_ERROR_EXCEPTION("Unit %v is not known", unit);
        }
        ans += doubleNumber * timeMappingIt->second;
    }
    return ans;
}

void RemoveExcessiveMain(int argc, const char** argv)
{
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    TString cluster;
    opts.AddFreeArgBinding("cluster", cluster, "Name of cluster with source table. If is not provided, is taken from env var YT_PROXY (in this case it must be set)");
    opts.AddLongOption("destination").DefaultValue("//sys/admin/yt-microservices/resource_usage");
    opts.AddLongOption("denominator").DefaultValue("1.4");
    opts.AddLongOption("step-size-increase").DefaultValue("2.86");
    opts.AddLongOption("ignore-window-size").DefaultValue("{\"days\": 3}");
    opts.AddLongOption("first-step-window-size").DefaultValue("{\"weeks\": 1}");
    opts.AddLongOption("first-step-allowed-frequency").DefaultValue("{\"hours\": 6}");

    NLastGetopt::TOptsParseResult r(&opts, argc, argv);

    if (cluster.empty()) {
        cluster = GetEnv("YT_PROXY", "");
        if (cluster.empty()) {
            opts.PrintUsage(argv[0], Cerr);
            exit(1);
        }
    }
    TString destination = r.Get("destination");
    auto denominator = FromString<double>(r.Get("denominator"));
    auto stepSizeIncrease = FromString<double>(r.Get("step-size-increase"));
    double ignoreWindowSize = GetTimestampFromJson(r.Get("ignore-window-size"));
    double firstStepWindowSize = GetTimestampFromJson(r.Get("first-step-window-size"));
    double firstStepAllowedFrequency = GetTimestampFromJson(r.Get("first-step-allowed-frequency"));

    NYT::TConfig::Get()->Token = LoadResourceUsageToken();

    RemoveExcessive(cluster, destination, denominator, stepSizeIncrease, ignoreWindowSize, firstStepWindowSize, firstStepAllowedFrequency);
}
