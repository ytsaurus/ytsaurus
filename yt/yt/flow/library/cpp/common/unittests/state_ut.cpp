#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <vector>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TStateMock, TMutableStateClient)
{
    auto manager = New<TStateManagerMock>();
    EXPECT_FALSE(manager->Contains("/state"));
    {
        TMutableStateClient<std::string> state;
        manager->CreateContext()->InitClient<std::string>(state, "state");
        EXPECT_EQ(*state, "");
        EXPECT_TRUE(state.IsEmpty());
        *state = "5";
        EXPECT_FALSE(state.IsEmpty());
        manager->Sync();
    }
    EXPECT_TRUE(manager->Contains("/state"));
    {
        TMutableStateClient<std::string> state;
        manager->CreateContext()->InitClient<std::string>(state, "state");
        EXPECT_EQ(*state, "5");
        EXPECT_FALSE(state.IsEmpty());
        *state = "";
        manager->Sync();
    }
    EXPECT_FALSE(manager->Contains("/state"));
    {
        TMutableStateClient<std::string> state;
        manager->CreateContext()->InitClient<std::string>(state, "state");
        EXPECT_EQ(*state, "");
        EXPECT_TRUE(state.IsEmpty());
        *state = "12";
        state->push_back('5');
        manager->Sync();
    }
    EXPECT_TRUE(manager->Contains("/state"));
    {
        TMutableStateClient<std::string> state;
        manager->CreateContext()->InitClient<std::string>(state, "state");
        EXPECT_EQ(*state, "125");
        EXPECT_FALSE(state.IsEmpty());
        state.Clear();
        EXPECT_TRUE(state.IsEmpty());
        manager->Sync();
    }
    EXPECT_FALSE(manager->Contains("/state"));
};

////////////////////////////////////////////////////////////////////////////////

struct TTestState
    : public NYTree::TYsonStruct
{
    int Value{};

    REGISTER_YSON_STRUCT(TTestState);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("value", &TThis::Value)
            .Default();
    }
};

using TTestStatePtr = TIntrusivePtr<TTestState>;

TEST(TStateMock, TMutableStateClientYsonStruct)
{
    auto manager = New<TStateManagerMock>();
    EXPECT_FALSE(manager->Contains("/state"));
    {
        TMutableStateClient<TTestState> state;
        manager->CreateContext()->InitClient<TTestState>(state, "state");
        EXPECT_EQ(state->Value, 0);
        EXPECT_TRUE(state.IsEmpty());
        state->Value = 5;
        EXPECT_FALSE(state.IsEmpty());
        manager->Sync();
    }
    EXPECT_TRUE(manager->Contains("/state"));
    {
        TMutableStateClient<TTestState> state;
        manager->CreateContext()->InitClient<TTestState>(state, "state");
        EXPECT_EQ(state->Value, 5);
        EXPECT_FALSE(state.IsEmpty());
        state->Value = 0;
        manager->Sync();
    }
    EXPECT_FALSE(manager->Contains("/state"));
    {
        TMutableStateClient<TTestState> state;
        manager->CreateContext()->InitClient<TTestState>(state, "state");
        EXPECT_EQ(state->Value, 0);
        EXPECT_TRUE(state.IsEmpty());
        state->Value = 125;
        manager->Sync();
    }
    EXPECT_TRUE(manager->Contains("/state"));
    {
        TMutableStateClient<TTestState> state;
        manager->CreateContext()->InitClient<TTestState>(state, "state");
        EXPECT_EQ(state->Value, 125);
        EXPECT_FALSE(state.IsEmpty());
        state.Clear();
        EXPECT_TRUE(state.IsEmpty());
        manager->Sync();
    }
    EXPECT_FALSE(manager->Contains("/state"));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
