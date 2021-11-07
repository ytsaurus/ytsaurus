#include <yt/yt/server/http_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::NHttpProxy::THttpProxyProgram().Run(argc, argv);
}
