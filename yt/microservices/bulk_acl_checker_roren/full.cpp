#include <yt/microservices/bulk_acl_checker_roren/full.h>
#include <yt/microservices/bulk_acl_checker_roren/import_snapshot.h>
#include <yt/microservices/bulk_acl_checker_roren/remove_excessive.h>

TFullCommand::TFullCommand(const NLastGetopt::TOpts& opts)
    : Opts_(opts)
{
}

int TFullCommand::operator()(int argc, const char** argv)
{
    TImportSnapshotsMain importSnapshots(Opts_);
    auto importResult = importSnapshots(argc, argv);
    if (importResult) {
        return importResult;
    }

    TRemoveExcessiveMain removeExcessive(Opts_);
    auto removeResult = removeExcessive(argc, argv);
    return removeResult;
}
