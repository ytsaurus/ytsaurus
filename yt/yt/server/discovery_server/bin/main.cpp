#include <yt/yt/server/discovery_server/program.h>

int main(int argc, const char** argv)
{
    return NYT::NClusterDiscoveryServer::TClusterDiscoveryServerProgram().Run(argc, argv);
}
