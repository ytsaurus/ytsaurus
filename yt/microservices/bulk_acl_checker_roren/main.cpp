#include <yt/microservices/bulk_acl_checker_roren/import_snapshot.h>
#include <yt/microservices/bulk_acl_checker_roren/remove_excessive.h>
#include <yt/microservices/bulk_acl_checker_roren/full.h>

#include <yt/cpp/mapreduce/interface/init.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/library/auth/auth.h>
#include <yt/yt/library/named_value/named_value.h>

#include <library/cpp/getopt/small/modchooser.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/yt/logging/logger.h>

#include <util/system/env.h>

const std::vector<TString> ALLOWED_COMMANDS = {"import-snapshot", "remove-excessive", "full", "-h", "--help"};

TString LoadBulkAclCheckerToken()
{
    auto ytResourceUsageToken = GetEnv("YT_BULK_ACL_CHECKER_TOKEN");
    NYT::NAuth::ValidateToken(ytResourceUsageToken);
    return ytResourceUsageToken;
}

TString cluster; // This variable is only used to pass it to opts.AddFreeArgBinding(), after that it is not used.

void AddCommonOpts(NLastGetopt::TOpts& opts)
{
    opts.AddHelpOption('h');
    opts.AddFreeArgBinding("cluster", cluster, "Name of cluster with source table. If is not provided, is taken from env var YT_PROXY");
    opts.SetFreeArgsMax(1);
    opts.AddLongOption("destination").DefaultValue("//sys/admin/yt-microservices/bulk_acl_checker");
}

void AddFullSpecificOpts(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("snapshot-id").DefaultValue("latest");
    opts.AddLongOption("snapshot-limit").DefaultValue("15");
    opts.AddLongOption("days-to-leave").DefaultValue("7"); // one week
    opts.AddLongOption("pool");
    opts.AddLongOption("force").NoArgument();
    opts.AddLongOption("memory-limit").DefaultValue(10_GB);
    opts.AddLongOption("enable-ipv4").NoArgument();
}

void AddImportSpecificOpts(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("snapshot-id").DefaultValue("all");
    opts.AddLongOption("snapshot-limit").DefaultValue("0");
    opts.AddLongOption("pool");
    opts.AddLongOption("force").NoArgument();
    opts.AddLongOption("memory-limit").DefaultValue(10_GB);
    opts.AddLongOption("enable-ipv4").NoArgument();
}

void AddRemoveSpecificOpts(NLastGetopt::TOpts& opts)
{
    opts.AddLongOption("days-to-leave").DefaultValue("7"); // one week
}

NLastGetopt::TOpts CreateImportSnapshotOpts()
{
    NLastGetopt::TOpts opts;
    AddCommonOpts(opts);
    AddImportSpecificOpts(opts);
    return opts;
}

NLastGetopt::TOpts CreateRemoveExcessiveOpts()
{
    NLastGetopt::TOpts opts;
    AddCommonOpts(opts);
    AddRemoveSpecificOpts(opts);
    return opts;
}

NLastGetopt::TOpts CreateFullOpts()
{
    NLastGetopt::TOpts opts;
    AddCommonOpts(opts);
    AddFullSpecificOpts(opts);
    return opts;
}

int main(int argc, const char** argv)
try {
    NYT::TConfig::Get()->LogLevel = "info";
    NYT::Initialize(argc, argv);

    NYT::TConfig::Get()->Token = LoadBulkAclCheckerToken();

    auto importSnapshotOpts = CreateImportSnapshotOpts();
    auto removeExcessiveOpts = CreateRemoveExcessiveOpts();
    auto fullOpts = CreateFullOpts();

    TImportSnapshotsMain importSnapshotsCommand(importSnapshotOpts);
    TRemoveExcessiveMain removeExcessiveCommand(removeExcessiveOpts);
    TFullCommand fullCommand(fullOpts);

    TModChooser modChooser;
    modChooser.AddMode("import-snapshot", &importSnapshotsCommand, "Import snapshots from specified directory");
    modChooser.AddMode("remove-excessive", &removeExcessiveCommand, "Remove excessive imported snapshots from specified directory");
    modChooser.AddMode("full", &fullCommand, "Import snapshots and remove excessive");

    return modChooser.Run(argc, argv);
} catch (const std::exception& ex) {
    Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
    Cerr << ex.what() << Endl;
    exit(EXIT_FAILURE);
} catch (...) {
    Cerr << TBackTrace::FromCurrentException().PrintToString() << Endl;
    Cerr << "Unknown error" << Endl;
    exit(EXIT_FAILURE);
}
