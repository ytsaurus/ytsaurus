#include <library/cpp/yt/logging/logger.h>

#include <util/system/env.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/microservices/resource_usage_roren/import_snapshot.h>
#include <yt/microservices/resource_usage_roren/remove_excessive.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <yt/yt/library/auth/auth.h>

static NYT::NLogging::TLogger Logger("resource_usage_roren");

const THashSet<TString> COMMANDS = {"remove-excessive", "import-snapshot"};

int main(int argc, const char** argv)
try {
    NYT::TConfig::Get()->LogLevel = "info";
    auto resolverConfig = NYT::New<NYT::NNet::TAddressResolverConfig>();
    resolverConfig->EnableIPv4 = true;
    NYT::NNet::TAddressResolver::Get()->Configure(resolverConfig);
    NYT::Initialize(argc, argv);

    if (argc < 2 || !COMMANDS.contains(argv[1])) {
        Cerr << "Usage: resource_usage_roren COMMAND [ARGS]...\n\nCommands:\n  import-snapshot\n  remove-excessive" << Endl;
        exit(1);
    }

    if (argv[1] == TString("import-snapshot")) {
        ImportSnapshotMain(argc - 1, argv + 1);
    } else {
        RemoveExcessiveMain(argc - 1, argv + 1);
    }

    return 0;
} catch (const std::exception& ex) {
    Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
    Cerr << ex.what() << Endl;
    exit(EXIT_FAILURE);
} catch (...) {
    Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
    Cerr << "Unknown error" << Endl;
    exit(EXIT_FAILURE);
}
