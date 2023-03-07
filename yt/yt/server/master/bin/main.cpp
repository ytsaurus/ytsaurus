#include <yt/server/master/cell_master/program.h>

int main(int argc, const char** argv)
{
    return NYT::TCellMasterProgram().Run(argc, argv);
}
