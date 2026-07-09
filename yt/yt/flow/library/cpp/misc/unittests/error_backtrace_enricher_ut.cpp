#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/error_backtrace_enricher.h>

#include <library/cpp/testing/hook/hook.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace NErrorBacktraceEnricherTest {

////////////////////////////////////////////////////////////////////////////////

Y_NO_INLINE void F4()
{
    throw std::runtime_error("from f4");
}

Y_NO_INLINE void F3()
{
    F4();
}

Y_NO_INLINE void F2(IInvokerPtr invoker)
{
    WaitFor(BIND(&F3).AsyncVia(invoker).Run()).ThrowOnError();
}

Y_NO_INLINE void F1(IInvokerPtr invoker)
{
    F2(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErrorBacktraceEnricherTest

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NErrorBacktraceEnricherTest;

////////////////////////////////////////////////////////////////////////////////

Y_TEST_HOOK_BEFORE_RUN(GTEST_YTFLOW_ERROR_BACKTRACE_ENRICHER_SETUP)
{
    InitBacktraceEnricher();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TErrorBacktraceEnricherTest, Configure)
{
    EXPECT_EQ(TBacktraceEnricherState::Get()->Level.load(), EBacktraceEnricherLevel::Disabled);
    auto finally = Finally([] {
        TBacktraceEnricherState::Get()->Level.store(EBacktraceEnricherLevel::Disabled);
    });

    auto spec = New<TBacktraceEnricherSpec>();
    auto dynamicSpec = New<TBacktraceEnricherDynamicSpec>();

    for (auto level : {EBacktraceEnricherLevel::EnabledForAll, EBacktraceEnricherLevel::Disabled}) {
        auto spec = New<TBacktraceEnricherSpec>();
        spec->Level = level;
        ConfigureSingleton(spec);
        EXPECT_EQ(TBacktraceEnricherState::Get()->Level.load(), level);
    }

    for (auto level : {EBacktraceEnricherLevel::EnabledForAll, EBacktraceEnricherLevel::Disabled}) {
        auto spec = New<TBacktraceEnricherSpec>();
        spec->Level = level;
        auto dynamicSpec = New<TBacktraceEnricherDynamicSpec>();
        ReconfigureSingleton(spec, dynamicSpec);
        EXPECT_EQ(TBacktraceEnricherState::Get()->Level.load(), level);
    }

    for (auto level : {EBacktraceEnricherLevel::EnabledForAll, EBacktraceEnricherLevel::Disabled}) {
        for (auto levelDynamic : {EBacktraceEnricherLevel::EnabledForAll, EBacktraceEnricherLevel::Disabled}) {
            auto spec = New<TBacktraceEnricherSpec>();
            spec->Level = level;
            auto dynamicSpec = New<TBacktraceEnricherDynamicSpec>();
            dynamicSpec->Level = levelDynamic;
            ReconfigureSingleton(spec, dynamicSpec);
            EXPECT_EQ(TBacktraceEnricherState::Get()->Level.load(), levelDynamic);
        }
    }
}

TEST(TErrorBacktraceEnricherTest, Enabling)
{
    EXPECT_EQ(TBacktraceEnricherState::Get()->Level.load(), EBacktraceEnricherLevel::Disabled);
    auto finally = Finally([] {
        TBacktraceEnricherState::Get()->Level.store(EBacktraceEnricherLevel::Disabled);
    });

    auto aq = NYT::New<NYT::NConcurrency::TActionQueue>("aq1");
    auto aq2 = NYT::New<NYT::NConcurrency::TActionQueue>("aq2");

    auto message = Format("%v", WaitFor(BIND(&F1, aq2->GetInvoker()).AsyncVia(aq->GetInvoker()).Run()));

    for (auto level : {EBacktraceEnricherLevel::EnabledForAll, EBacktraceEnricherLevel::Disabled}) {
        TBacktraceEnricherState::Get()->Level.store(level);

        bool enabled = (level == EBacktraceEnricherLevel::EnabledForAll);
        auto message = Format("%v", WaitFor(BIND(&F1, aq2->GetInvoker()).AsyncVia(aq->GetInvoker()).Run()));
        EXPECT_TRUE(message.Contains("from f4")) << "message='" << message << "'";
        EXPECT_EQ(message.Contains("F4"), enabled) << "message='" << message << "', enabled=" << enabled;
        EXPECT_EQ(message.Contains("F3"), enabled) << "message='" << message << "', enabled=" << enabled;
        EXPECT_EQ(message.Contains("F2"), enabled) << "message='" << message << "', enabled=" << enabled;
        EXPECT_EQ(message.Contains("F1"), enabled) << "message='" << message << "', enabled=" << enabled;
    }
}

TEST(TErrorBacktraceEnricherTest, CheckLevel)
{
    TError genericError = TError("generic");
    TError canceledError = TError(NYT::EErrorCode::Canceled, "Canceled");
    TError canceledNotTrivialError = TError(NYT::EErrorCode::Canceled, "Canceled because this and this");
    TError stdExceptionError = TError(std::runtime_error("std::runtime_error"));
    TError goodError = TError(NYT::EErrorCode::Timeout, "request time out") << TErrorAttribute("table_path", "path");

    // EnabledForAll, Disabled.
    for (const auto& error : {genericError, canceledError, canceledNotTrivialError, stdExceptionError, goodError}) {
        EXPECT_TRUE(CheckLevel(error, EBacktraceEnricherLevel::EnabledForAll)) << "error=" << ToString(error);
        EXPECT_FALSE(CheckLevel(error, EBacktraceEnricherLevel::Disabled)) << "error=" << ToString(error);
    }

    // EnabledForNotNativeErrors.
    EXPECT_TRUE(CheckLevel(stdExceptionError, EBacktraceEnricherLevel::EnabledForNotNativeErrors)) << "error=" << ToString(stdExceptionError);
    for (const auto& error : {genericError, canceledError, canceledNotTrivialError, goodError}) {
        EXPECT_FALSE(CheckLevel(error, EBacktraceEnricherLevel::EnabledForNotNativeErrors)) << "error=" << ToString(error);
    }

    // EnabledForTrivialErrors. Also check wrapped errors here.
    for (const auto& error : {genericError, canceledError, stdExceptionError}) {
        EXPECT_TRUE(CheckLevel(error, EBacktraceEnricherLevel::EnabledForTrivialErrors)) << "error=" << ToString(error);
        EXPECT_TRUE(CheckLevel(TError("wrapper") << error, EBacktraceEnricherLevel::EnabledForTrivialErrors)) << "error=" << ToString(error);
    }
    for (const auto& error : {canceledNotTrivialError, goodError}) {
        EXPECT_FALSE(CheckLevel(error, EBacktraceEnricherLevel::EnabledForTrivialErrors)) << "error=" << ToString(error);
        EXPECT_FALSE(CheckLevel(TError("wrapper") << error, EBacktraceEnricherLevel::EnabledForTrivialErrors)) << "error=" << ToString(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
