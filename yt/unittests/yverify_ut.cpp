#include "stdafx.h"

#include <ytlib/misc/assert.h>

#include <util/generic/yexception.h>

#include <contrib/testing/framework.h>

using ::testing::_;
using ::testing::A;
using ::testing::NiceMock;
using ::testing::ReturnArg;
using ::testing::Throw;

namespace NYT {

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

TEST(TVerifyDeathTest, NoCrashForTruthExpression)
{
    TMockCallee callee;
    EXPECT_CALL(callee, F(true, _)).Times(1);

    YCHECK(callee.F(true, "This should be okay."));
    SUCCEED();
}

TEST(TVerifyDeathTest, CrashForFalseExpression)
{
    NiceMock<TMockCallee> callee;

    ASSERT_DEATH(
        { YCHECK(callee.F(false, "Cheshire Cat")); },
        ".*Cheshire Cat.*"
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

