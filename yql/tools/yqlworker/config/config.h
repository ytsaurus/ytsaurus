#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/cfg/proto/worker_config.pb.h>

#include <library/cpp/getopt/last_getopt.h>
#include <util/system/file.h>


namespace NYql {

enum class EQtWorkerRole {
    ROLE_MASTER     /* "master" */,
    ROLE_CORE       /* "core" */,
    ROLE_FORKER     /* "forker" */,
    ROLE_DQ         /* "dq" */,
};

struct TMainConfig: private NLastGetopt::TOpts
{
    NProto::TWorkerConfig Worker;
    TGatewaysConfig Gateways;
    TFileStorageConfig FileStorage;
    TString StdErrFile;
    bool Daemonize = false;
    TMaybe<EQtWorkerRole> QtRole;

    explicit TMainConfig(bool forQt = false);
    void ParseFromCommandLineArgs(int argc, const char* argv[]);
    void ParseAgain();
    void ReopenStdErr();

private:
    void CleanUsernames();

private:
    int SavedArgc_;
    TVector<char*> SavedArgv_;
    TString SavedArgvBuf_;
    bool ForQt_;
};

void ParseFreeArgsConfig(const TVector<TString>& freeArgs, TMainConfig* config);

} // namespace NYql
