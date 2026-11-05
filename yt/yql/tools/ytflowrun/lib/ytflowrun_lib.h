#pragma once

#include <yt/yql/providers/ytflow/provider/yql_ytflow_gateway.h>

#include <yt/yql/tools/ytrun/lib/ytrun_lib.h>

namespace NYql {

class TYtflowRunTool: public TYtRunTool {
public:
    TYtflowRunTool(TString name = "ytflowrun");
    ~TYtflowRunTool() = default;

protected:
    int DoMain(int argc, const char *argv[]) override;
    TProgram::TStatus DoRunProgram(TProgramPtr program) override;

    virtual IYtflowGateway::TPtr CreateYtflowGateway();
};

} // NYql
