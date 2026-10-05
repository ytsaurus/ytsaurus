#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/signature/common/test_helpers/mock_keystore.h>

#include <yt/yt/library/signature/generation/config.h>
#include <yt/yt/library/signature/generation/key_rotator.h>
#include <yt/yt/library/signature/generation/signature_generator.h>

#include <util/system/event.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

using testing::_;
using testing::AtLeast;
using testing::Between;
using testing::Ne;
using testing::NotNull;
using testing::Pointee;
using testing::Return;
using testing::SizeIs;

////////////////////////////////////////////////////////////////////////////////


struct TKeyRotatorTest
    : public ::testing::Test
{
    const TOwnerId OwnerId = TOwnerId("test-generator");
    TKeyRotatorConfigPtr Config = New<TKeyRotatorConfig>();
    TIntrusivePtr<TStrictMockKeyStoreWriter> Store = New<TStrictMockKeyStoreWriter>();
    TSignatureGeneratorPtr Generator = New<TSignatureGenerator>(New<TSignatureGeneratorConfig>());
    TKeyRotatorPtr Rotator;

    TKeyRotatorTest()
    {
        WaitFor(InitializeCryptography(GetCurrentInvoker()))
            .ThrowOnError();
        EXPECT_CALL(*Store, GetOwner())
            .WillRepeatedly(Return(OwnerId));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateOutOfBand)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(2)
        .WillRepeatedly(Return(OKFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    WaitFor(Rotator->Start())
        .ThrowOnError();
    auto firstKey = Generator->KeyInfo();
    EXPECT_THAT(firstKey, NotNull());

    WaitFor(Rotator->Rotate())
        .ThrowOnError();
    auto secondKey = Generator->KeyInfo();
    EXPECT_THAT(secondKey, Pointee(Ne(*firstKey)));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateOnStart)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .WillOnce(Return(OKFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, MethodsFailWhileStopped)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .WillOnce(Return(OKFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();

    EXPECT_THROW_WITH_ERROR_CODE(
        WaitFor(Rotator->Rotate()).ThrowOnError(),
        NYT::EErrorCode::Canceled);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, StopAllowsReentrantFutureSubscriber)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(0);

    Rotator = New<TKeyRotator>(Config, GetNullInvoker(), Store, Generator);
    auto startFuture = Rotator->Start();
    ASSERT_FALSE(startFuture.IsSet());

    auto reentryQueue = New<TActionQueue>("RotatorReentry");
    TManualEvent reentryStarted;
    TManualEvent reentryFinished;
    TFuture<void> reentryFuture;

    startFuture.Subscribe(BIND([&] (const TError& /*error*/) {
        reentryFuture = BIND([&] {
            reentryStarted.Signal();
            YT_UNUSED_FUTURE(Rotator->Stop());
            reentryFinished.Signal();
        })
            .AsyncVia(reentryQueue->GetInvoker())
            .Run();

        ASSERT_TRUE(reentryStarted.WaitT(TDuration::Seconds(5)));
        EXPECT_TRUE(reentryFinished.WaitT(TDuration::Seconds(5)));
    }));

    WaitFor(Rotator->Stop())
        .ThrowOnError();
    WaitFor(reentryFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    reentryQueue->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, DisabledByNullPeriod)
{
    Config->KeyRotationOptions.Period.reset();
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    auto startFuture = Rotator->Start();
    EXPECT_TRUE(startFuture.IsSet());
    WaitFor(startFuture)
        .ThrowOnError();

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureFromNullPeriod)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(3, 50))
        .WillRepeatedly(Return(OKFuture));

    Config->KeyRotationOptions.Period.reset();
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();

    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationOptions.Period = TDuration::MilliSeconds(10);
    Rotator->Reconfigure(newConfig);
    Sleep(*newConfig->KeyRotationOptions.Period * 20);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureToNullPeriod)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(1)
        .WillOnce(Return(OKFuture));

    Config->KeyRotationOptions.Period = TDuration::MilliSeconds(200);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();

    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationOptions.Period.reset();
    Rotator->Reconfigure(newConfig);
    Sleep(*Config->KeyRotationOptions.Period * 5);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureWhileRotating)
{
    auto rotationStarted = NewPromise<void>();
    auto finishRotation = NewPromise<void>();

    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(1)
        .WillOnce([&] (const TKeyInfoPtr&) -> TFuture<void> {
            rotationStarted.Set();
            return finishRotation.ToFuture();
        });

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    auto startFuture = Rotator->Start();
    WaitFor(rotationStarted.ToFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    auto disabledConfig = New<TKeyRotatorConfig>();
    disabledConfig->KeyRotationOptions.Period.reset();
    Rotator->Reconfigure(disabledConfig);
    Rotator->Reconfigure(New<TKeyRotatorConfig>());

    EXPECT_FALSE(startFuture.IsSet());
    finishRotation.Set();

    WaitFor(startFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_THAT(Generator->KeyInfo(), NotNull());
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, PeriodicRotate)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(3, 50))
        .WillRepeatedly(Return(OKFuture));

    Config->KeyRotationOptions.Period = TDuration::MilliSeconds(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    Sleep(*Config->KeyRotationOptions.Period * 20);
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateRetry)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(AtLeast(2))
        .WillOnce(Return(MakeFuture<void>(TError("error"))))
        .WillRepeatedly(Return(OKFuture));

    Config->KeyRotationOptions.Period = TDuration::Hours(10);
    Config->KeyRotationOptions.MinBackoff = TDuration::MilliSeconds(1);
    Config->KeyRotationOptions.MaxBackoff = TDuration::MilliSeconds(300);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    // Rotation errors should not propagate.
    EXPECT_NO_THROW(WaitFor(Rotator->Start())
        .ThrowOnError());
    Sleep(Config->KeyRotationOptions.Splay + Config->KeyRotationOptions.MaxBackoff * 5);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureChangesRotationInterval)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(5, 30))
        .WillRepeatedly(Return(OKFuture));

    Config->KeyRotationOptions.Period = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    // Shouldn't do any rotations.
    YT_UNUSED_FUTURE(Rotator->Start());

    // Reconfigure with a shorter interval.
    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationOptions.Period = TDuration::MilliSeconds(20);
    Rotator->Reconfigure(newConfig);
    Sleep(*newConfig->KeyRotationOptions.Period * 15);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureWhileStopped)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(5, 30))
        .WillRepeatedly(Return(OKFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();

    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationOptions.Period = TDuration::MilliSeconds(10);
    Rotator->Reconfigure(newConfig);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    Sleep(*newConfig->KeyRotationOptions.Period * 15);
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, MultipleReconfigures)
{
    // This test verifies that multiple reconfigurations on a running rotator work correctly
    // by starting with a long interval and ending with a very short one.
    // The final short interval should dominate the rotation count.

    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(50, 500))
        .WillRepeatedly(Return(OKFuture));

    Config->KeyRotationOptions.Period = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    WaitFor(Rotator->Start())
        .ThrowOnError();

    for (int i = 0; i < 5; ++i) {
        auto newConfig = New<TKeyRotatorConfig>();
        // Gradually decreases: 5s -> 2s -> 1s -> 500ms -> 200ms.
        newConfig->KeyRotationOptions.Period = TDuration::Seconds(5) / (1 << i);
        Rotator->Reconfigure(newConfig);
    }

    // Final reconfiguration with a very short interval to overshadow everything that has been before.
    auto finalConfig = New<TKeyRotatorConfig>();
    finalConfig->KeyRotationOptions.Period = TDuration::MilliSeconds(10);
    Rotator->Reconfigure(finalConfig);
    Sleep(*finalConfig->KeyRotationOptions.Period * 300);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, Splay)
{
    // No expectations: we test that it doesn't start rotation immediately.
    Config->KeyRotationOptions.Splay = TDuration::Hours(1);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    YT_UNUSED_FUTURE(Rotator->Start());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
