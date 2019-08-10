#include <yt/server/exec/program.h>

int main(int argc, const char** argv)
{
    return NYT::NJobProxy::TExecProgram().Run(argc, argv);
}
