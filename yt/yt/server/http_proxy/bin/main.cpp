#include <yt/server/http_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::THttpProxyProgram().Run(argc, argv);
}
