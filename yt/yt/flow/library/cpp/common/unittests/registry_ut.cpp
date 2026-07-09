#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TTestElephantYsonStruct
    : public virtual TYsonMessage
{
    REGISTER_YSON_STRUCT(TTestElephantYsonStruct);

    static void Register(TRegistrar /*registrar*/)
    { }
};

YT_FLOW_DEFINE_YSON_MESSAGE(TTestElephantYsonStruct);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRegistryTest, Create)
{
    auto message = TRegistry::Get()->CreateYsonMessage("NYT::NFlow::TTestElephantYsonStruct");
    auto& messageRef = *message;
    auto expectedMessage = New<TTestElephantYsonStruct>();
    auto& expectedMessageRef = *expectedMessage;
    ASSERT_EQ(typeid(messageRef), typeid(expectedMessageRef));

    auto createWithMistake = [] {
        try {
            auto message = TRegistry::Get()->CreateYsonMessage("TTestElephantYsonStruct"); // No namespace.
        } catch (const TErrorException& ex) {
            ASSERT_TRUE(ToString(ex).Contains("[\"NYT::NFlow::TTestElephantYsonStruct")) << "Full exception: " << ToString(ex);
            throw;
        }
    };
    EXPECT_THROW(createWithMistake(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
