#include <yt/server/node/cluster_node/program.h>

int main(int argc, const char** argv)
{
    return NYT::NClusterNode::TClusterNodeProgram().Run(argc, argv);
}
