#pragma once

#include <yt/microservices/bulk_acl_checker_roren/options.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

class TRemoveExcessiveMain : public TMainClass
{
public:
    TRemoveExcessiveMain(const NLastGetopt::TOpts& opts);

    int operator()(int argc, const char** argv);

private:
    void ParseArgs(int argc, const char** argv);

    int RemoveExcessive();

private:
    TRemoveExcessiveOptions Options_;
    const NLastGetopt::TOpts& Opts_;
};
