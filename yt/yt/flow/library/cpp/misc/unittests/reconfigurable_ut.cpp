#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/test_framework/framework.h>

#include <stdexcept>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TFailureRecordingReconfigurable
    : public TReconfigurable<>
{
public:
    TError Error;
    int FailureCount = 0;

protected:
    void OnReconfigureFail(const TError& error) override
    {
        Error = error;
        ++FailureCount;
    }
};

TEST(TReconfigurableTest, DefaultHandlerPreservesException)
{
    TReconfigurable<> reconfigurable;
    reconfigurable.SubscribeReconfigured(BIND([] {
        throw std::invalid_argument("Invalid configuration");
    }));
    reconfigurable.SubscribeReconfigured(BIND([] {
        ADD_FAILURE() << "Reconfiguration continued after failure";
    }));

    try {
        reconfigurable.Reconfigure();
        ADD_FAILURE() << "Exception was swallowed";
    } catch (const std::invalid_argument& ex) {
        EXPECT_STREQ(ex.what(), "Invalid configuration");
    }
}

TEST(TReconfigurableTest, CustomHandlerReceivesEveryFailure)
{
    TFailureRecordingReconfigurable reconfigurable;
    reconfigurable.SubscribeReconfigured(BIND([] {
        THROW_ERROR_EXCEPTION(TErrorCode(12345), "Invalid configuration");
    }));
    reconfigurable.SubscribeReconfigured(BIND([] {
        ADD_FAILURE() << "Reconfiguration continued after failure";
    }));

    EXPECT_NO_THROW(reconfigurable.Reconfigure());
    EXPECT_NO_THROW(reconfigurable.Reconfigure());
    EXPECT_EQ(reconfigurable.FailureCount, 2);
    EXPECT_TRUE(reconfigurable.Error.FindMatching(TErrorCode(12345)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
