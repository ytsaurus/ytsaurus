#include <yt/yt/server/tcp_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::NTcpProxy::TTcpProxyProgram().Run(argc, argv);
}
