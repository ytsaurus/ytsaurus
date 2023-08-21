#include <yt/yt/server/cypress_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::NCypressProxy::TCypressProxyProgram().Run(argc, argv);
}
