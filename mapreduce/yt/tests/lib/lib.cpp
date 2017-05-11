#include "lib.h"

namespace NYT {
namespace NTest {

////////////////////////////////////////////////////////////////////////////////

int Main(int argc, const char* argv[])
{
    if (argc < 2) {
        Cerr << "error: no parameters" << Endl;
        return 1;
    }

    TString mode(argv[1]);

    if (mode == "--list") {
        for (auto t : *Singleton<TTestMap>()) {
            Cout << t.first << Endl;
        }
        return 0;

    }

    if (mode == "--run") {
        if (argc < 4) {
            Cerr << "error: missing parameters for --run" << Endl;
            return 1;
        }

        TTestConfig::Get()->ServerName = argv[2];
        TString testName(argv[3]);

        auto test = TTestMap::Get()->find(testName);
        if (test == TTestMap::Get()->end()) {
            Cerr << "error: test " << testName << " is not registered" << Endl;
            return 1;
        }
        NUnitTest::TTestContext context;
        test->second(context);
        return 0;
    }

    Cerr << "error: invalid mode" << Endl;
    return 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

