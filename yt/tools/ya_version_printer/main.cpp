#include <library/getopt/last_getopt.h>
#include <library/getopt/modchooser.h>

#include <yt/build/build.h>
#include <yp/build/build.h>
#include <yt/core/ya_version/ya_version.h>

int main(int argc, const char** argv)
{
    int patchNumber;
    TString branch;
    TString project;

    using namespace NLastGetopt;
    TOpts opts = NLastGetopt::TOpts::Default();
    opts.AddHelpOption();
    opts.AddLongOption("branch")
        .StoreResult(&branch)
        .DefaultValue("local")
        .RequiredArgument("BRANCH")
        .Help("override branch part of the version string");

    opts.AddLongOption("patch-number")
        .StoreResult(&patchNumber)
        .DefaultValue(0)
        .RequiredArgument("PATCH_NUMBER")
        .Help("override patch number part of the version string");

    opts.AddLongOption("project")
        .StoreResult(&project)
        .RequiredArgument("PROJECT")
        .Required()
        .Help(R"(project we want print version for, possible values: "yt", "yp")");

    TOptsParseResult res(&opts, argc, argv);

    int majorVersion;
    int minorVersion;
    if (project == "yt") {
        majorVersion = NYT::GetVersionMajor();
        minorVersion = NYT::GetVersionMinor();
    } else if (project == "yp") {
        majorVersion = NYP::GetVersionMajor();
        minorVersion = NYP::GetVersionMinor();
    } else {
        Cerr << "Unknown project \"" << project << '"'<< Endl;
        return 1;
    }
    Cout << NYT::CreateYtVersion(majorVersion, minorVersion, patchNumber, branch) << Endl;

    return 0;
}
