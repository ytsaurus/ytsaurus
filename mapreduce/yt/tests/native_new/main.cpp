#include <library/unittest/utmain.h>

#include <mapreduce/yt/interface/client.h>

int main(int argc, const char** argv)
{
    NYT::Initialize(argc, argv);
    return NUnitTest::RunMain(argc, const_cast<char**>(argv));
}
