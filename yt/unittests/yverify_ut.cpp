#include "../ytlib/misc/assert.h"

#include <util/generic/yexception.h>

#include "framework/framework.h"

using ::testing::_;
using ::testing::A;
using ::testing::NiceMock;
using ::testing::ReturnArg;
using ::testing::Throw;

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

class TCallee {
public:
    virtual bool F(bool passThrough, const char* comment) = 0;
};

class TMockCallee : public TCallee {
public:
    MOCK_METHOD2(F, bool(bool passThrough, const char* comment));
};

TEST(TVerifyDeathTest, NoCrushForTruthExpression)
{
    TMockCallee callee;
    EXPECT_CALL(callee, F(true, _))
        .Times(1)
        .WillOnce(ReturnArg<0>());

    YVERIFY(callee.F(true, "This should be okay."));
    SUCCEED();
}

TEST(TVerifyDeathTest, CrushForFalseExpression)
{
    NiceMock<TMockCallee> callee;
    ON_CALL(callee, F(A<bool>(), _))
        .WillByDefault(ReturnArg<0>());

    ASSERT_DEATH(
        { YVERIFY(callee.F(false, "Cheshire Cat")); },
        "Assertion failed.*Cheshire Cat"
    );
}

TEST(TVerifyDeathTest, CrushForException)
{
    NiceMock<TMockCallee> callee;
    ON_CALL(callee, F(A<bool>(), _))
        .WillByDefault(Throw(yexception() << "Dumb Exception"));

    ASSERT_DEATH(
        { YVERIFY(callee.F(true, "Cheshire Cat")); },
        "Assertion failed.*Exception during verification"
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT

