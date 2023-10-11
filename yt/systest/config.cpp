
#include <yt/systest/config.h>

namespace NYT::NTest {

TConfig::TConfig() {
    Ipv6 = false;
}

void TConfig::ParseCommandline(int argc, char* argv[])
{
    int opt;
    while ((opt = getopt(argc, argv, "b:c:h:n:vd")) != -1) {
        switch (opt) {
            case 'n':
                RunnerConfig.NumOperations = atoi(optarg);
                break;
            case 'b':
                RunnerConfig.NumBootstrapRecords = atoi(optarg);
                break;
            case 'h':
                RunnerConfig.HomeDirectory = optarg;
                break;
            case 's':
                RunnerConfig.Seed = atoi(optarg);
                break;
            case 'd':
                RunnerConfig.EnableRenamesDeletes = true;
                break;
            case 'v':
                Ipv6 = true;
                break;
            default:
                printf("Usage: systest <... options ... >\n");
                exit(EXIT_FAILURE);
        }
    }
}

}  // namespace NYT::NTest
