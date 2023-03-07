#include <yt/server/job_proxy/program.h>

int main(int argc, const char** argv)
{
    return NYT::NJobProxy::TJobProxyProgram().Run(argc, argv);
}
