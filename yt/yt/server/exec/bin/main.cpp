#include <yt/server/exec/program.h>

int main(int argc, const char** argv)
{
    return NYT::NExec::TExecProgram().Run(argc, argv);
}
