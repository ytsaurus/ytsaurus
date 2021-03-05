#include <yt/yt/server/timestamp_provider/program.h>

int main(int argc, const char** argv)
{
    return NYT::NTimestampProvider::TTimestampProviderProgram().Run(argc, argv);
}
