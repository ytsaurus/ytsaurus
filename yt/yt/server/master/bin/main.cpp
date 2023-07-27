#include <yt/yt/server/master/cell_master/program.h>

int main(int argc, const char** argv)
{
    return NYT::NCellMaster::TCellMasterProgram().Run(argc, argv);
}
