#include <yt/yt/server/master_cache/program.h>

int main(int argc, const char** argv)
{
    return NYT::NMasterCache::TMasterCacheProgram().Run(argc, argv);
}
