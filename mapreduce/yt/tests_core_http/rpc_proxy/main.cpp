#include <library/cpp/testing/unittest/utmain.h>

#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/finally.h>

using namespace NYT;

int main(int argc, const char** argv)
{
    auto finally = Finally([] {
        NYT::Shutdown();
    });

    return NUnitTest::RunMain(argc, const_cast<char**>(argv));
}
