#include <yt/yt/server/tools/program.h>

int main(int argc, const char** argv)
{
    return NYT::NTools::TToolsProgram().Run(argc, argv);
}
