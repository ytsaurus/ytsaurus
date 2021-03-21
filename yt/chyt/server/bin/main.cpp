#include <yt/chyt/server/program.h>

int main(int argc, const char** argv)
{
    return NYT::NClickHouseServer::TClickHouseServerProgram().Run(argc, argv);
}
