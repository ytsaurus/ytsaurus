#pragma once

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/getopt/small/modchooser.h>

class TFullCommand : public TMainClass
{
public:
    TFullCommand(const NLastGetopt::TOpts& opts);

    int operator()(int argc, const char** argv);
private:
    const NLastGetopt::TOpts& Opts_;
};
