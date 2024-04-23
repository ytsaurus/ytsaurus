#include <library/cpp/testing/gtest/main.h>
#include <yt/cpp/mapreduce/interface/init.h>

int main(int argc, char **argv)
{
    NYT::Initialize();

    return NGTest::Main(argc, argv);
}
