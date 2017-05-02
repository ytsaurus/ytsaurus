#pragma once

#include <util/generic/stroka.h>
#include <util/generic/hash.h>
#include <util/generic/singleton.h>

#include <library/unittest/gtest.h>

namespace NYT {
namespace NTest {

////////////////////////////////////////////////////////////////////////////////

struct TTestMap
    : public yhash<Stroka, std::function<void(NUnitTest::TTestContext&)>>
{
    static TTestMap* Get()
    {
        return Singleton<TTestMap>();
    }
};

struct TTestRegistrator
{
    inline TTestRegistrator(const Stroka& name, std::function<void(NUnitTest::TTestContext&)> func) {
        TTestMap::Get()->insert({name, func});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestConfig
{
    static TTestConfig* Get()
    {
        return Singleton<TTestConfig>();
    }

    Stroka ServerName;
};

////////////////////////////////////////////////////////////////////////////////

class TTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        ServerName_ = TTestConfig::Get()->ServerName;
    }

    void TearDown() override
    { }

    const Stroka& ServerName() { return ServerName_; }

private:
    Stroka ServerName_;
};

////////////////////////////////////////////////////////////////////////////////

int Main(int argc, const char* argv[]);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTest
} // namespace NYT

#define YT_TEST(N, NN) \
void Test##N##NN(NUnitTest::TTestContext&); \
static NYT::NTest::TTestRegistrator RegisterTest##N##NN(#N#NN, Test##N##NN); \
TEST_F(N, NN)

