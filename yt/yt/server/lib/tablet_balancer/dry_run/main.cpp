#include <yt/yt/server/lib/tablet_balancer/dry_run/lib/executor.h>
#include <yt/yt/server/lib/tablet_balancer/dry_run/lib/holders.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/yt/logging/logger.h>

static const NYT::NLogging::TLogger Logger("TabletBalancer");

int main(int argc, const char** argv)
{
    try {
        auto opts = NLastGetopt::TOpts::Default();
        opts.AddHelpOption();

        TString modeName;
        opts.AddLongOption("mode", "choose balancing type from [parameterized"
                           " | reshard | in_memory_move | ordinary_move]")
            .RequiredArgument("mode")
            .StoreResult(&modeName);

        TString loadFromFile;
        opts.AddLongOption("snapshot-path", "path to bundle snapshot in special format")
            .RequiredArgument("filename")
            .StoreResult(&loadFromFile);

        TString groupName;
        opts.AddLongOption("group-name", "used only for parameterized balancing")
            .Optional()
            .RequiredArgument("group-name")
            .StoreResult(&groupName);

        TString parameterizedConfig;
        opts.AddLongOption("parameterized-config", "used only for parameterized balancing")
            .Optional()
            .RequiredArgument("parameterized-config")
            .StoreResult(&parameterizedConfig);

        NLastGetopt::TOptsParseResult results(&opts, argc, argv);

        auto mode = NYT::NYTree::ConvertTo<NYT::NTabletBalancer::NDryRun::EBalancingMode>(modeName);

        TIFStream dataStream(loadFromFile);
        auto bundleHolder = NYT::NYTree::ConvertTo<NYT::NTabletBalancer::NDryRun::TBundleHolderPtr>(
            NYT::NYson::TYsonString(dataStream.ReadAll()));
        auto bundle = bundleHolder->CreateBundle();

        NYT::NTabletBalancer::NDryRun::BalanceAndPrintDescriptors(
            mode,
            bundle,
            groupName,
            parameterizedConfig);

    } catch (const std::exception& ex) {
        YT_LOG_ERROR("Dry run failed with an error: %v", ex.what());
    }
}
