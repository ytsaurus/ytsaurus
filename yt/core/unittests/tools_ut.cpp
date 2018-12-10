#include <yt/core/test_framework/framework.h>

#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>

#include <yt/core/ytree/serialize.h>

namespace NYT::NTools {
namespace {

////////////////////////////////////////////////////////////////////////////////

#ifndef _win_

template <
    typename ToolType,
    typename TArg = typename TFunctionTraits<ToolType>::TArg,
    typename TResult = typename TFunctionTraits<ToolType>::TResult>
TResult RunToolInProcess(const TArg& arg)
{
    return RunTool<ToolType, TArg, TResult>(arg, DoRunToolInProcess);
}

struct TMultiplyByTwo
{
    int operator()(int arg) const
    {
        return 2 * arg;
    }
};

REGISTER_TOOL(TMultiplyByTwo)

TEST(TTools, MultiplyByTwo)
{
    auto result = RunToolInProcess<TMultiplyByTwo>(2);
    EXPECT_EQ(4, result);
}

struct TStringToVoid
{
    void operator()(const TString& arg) const
    { }
};

REGISTER_TOOL(TStringToVoid)

TEST(TTools, ToVoid)
{
    RunToolInProcess<TStringToVoid>("Hello world");
}

struct TFaulty
{
    int operator()(int arg) const
    {
        THROW_ERROR_EXCEPTION("Fail");
    }
};

REGISTER_TOOL(TFaulty)

TEST(TTools, Faulty)
{
    try {
        RunToolInProcess<TFaulty>(0);
    } catch (const TErrorException& ex) {
        auto errorMessage = ex.Error().InnerErrors()[0].GetMessage();
        EXPECT_TRUE(errorMessage == "Fail");
    }
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTools
