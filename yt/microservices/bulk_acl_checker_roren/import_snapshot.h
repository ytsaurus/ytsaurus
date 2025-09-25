#pragma once

#include <yt/microservices/bulk_acl_checker_roren/options.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

class TImportSnapshotsMain : public TMainClass
{
public:
    TImportSnapshotsMain(const NLastGetopt::TOpts& opts);

    int operator()(int argc, const char** argv);

private:
    void ParseArgs(int argc, const char** argv);

    int ImportSnapshots();

private:
    TImportSnapshotOptions Options_;
    const NLastGetopt::TOpts& Opts_;
};
