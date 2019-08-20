#include <yt/server/node/cell_node/program.h>

int main(int argc, const char** argv)
{
    return NYT::NCellNode::TCellNodeProgram().Run(argc, argv);
}
