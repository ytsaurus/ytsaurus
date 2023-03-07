#include <yt/server/rpc_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::NRpcProxy::TRpcProxyProgram().Run(argc, argv);
}
