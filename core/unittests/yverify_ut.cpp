#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/assert.h>

#include <util/generic/yexception.h>

namespace NYT {
namespace {

using ::testing::_;
using ::testing::A;
using ::testing::NiceMock;
using ::testing::ReturnArg;
using ::testing::Throw;

////////////////////////////////////////////////////////////////////////////////

class TCallee
{
public:
    virtual ~TCallee() { }
    virtual bool F(bool passThrough, const char* comment) = 0;
};

class TMockCallee
    : public TCallee
{
public:
    TMockCallee()
    {
        ON_CALL(*this, F(A<bool>(), _))
            .WillByDefault(ReturnArg<0>());
    }

    MOCK_METHOD2(F, bool(bool passThrough, const char* comment));
};

TEST(TVerifyDeathTest, DISABLED_NoCrashForTruthExpression)
{
    TMockCallee callee;
    EXPECT_CALL(callee, F(true, _)).Times(1);

    YCHECK(callee.F(true, "This should be okay."));
    SUCCEED();
}

TEST(TVerifyDeathTest, DISABLED_CrashForFalseExpression)
{
    NiceMock<TMockCallee> callee;

    ASSERT_DEATH(
        { YCHECK(callee.F(false, "Cheshire Cat")); },
        ".*Cheshire Cat.*"
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
