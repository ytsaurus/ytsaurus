#include <yt/yt/core/test_framework/framework.h>

#include "mock_keystore.h"

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/key_rotator.h>
#include <yt/yt/server/lib/signature/signature_generator.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

using testing::_;
using testing::Between;
using testing::Ne;
using testing::NotNull;
using testing::Pointee;
using testing::Return;
using testing::ReturnRef;
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
        EXPECT_CALL(*Store, GetOwner())
            .WillRepeatedly(Return(OwnerId));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateOutOfBand)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(2)
        .WillRepeatedly(Return(VoidFuture));

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
        .WillOnce(Return(VoidFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, PeriodicRotate)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(3, 50))
        .WillRepeatedly(Return(VoidFuture));

    Config->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    Sleep(Config->KeyRotationInterval * 20);
    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateNoErrorPropagation)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .WillRepeatedly(Return(MakeFuture<void>(TError("error"))));

    Config->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    // Rotation errors should not propagate.
    EXPECT_NO_THROW(WaitFor(Rotator->Start())
        .ThrowOnError());
    EXPECT_NO_THROW(Sleep(Config->KeyRotationInterval * 10));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureChangesRotationInterval)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(5, 30))
        .WillRepeatedly(Return(VoidFuture));

    Config->KeyRotationInterval = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);
    // Shouldn't do any rotations.
    YT_UNUSED_FUTURE(Rotator->Start());

    // Reconfigure with a shorter interval.
    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationInterval = TDuration::MilliSeconds(20);
    Rotator->Reconfigure(newConfig);
    Sleep(newConfig->KeyRotationInterval * 15);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, ReconfigureWhileStopped)
{
    EXPECT_CALL(*Store, RegisterKey(_))
        .Times(Between(5, 30))
        .WillRepeatedly(Return(VoidFuture));

    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();

    auto newConfig = New<TKeyRotatorConfig>();
    newConfig->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator->Reconfigure(newConfig);
    WaitFor(Rotator->Start())
        .ThrowOnError();
    Sleep(newConfig->KeyRotationInterval * 15);
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
        .WillRepeatedly(Return(VoidFuture));

    Config->KeyRotationInterval = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Store, Generator);

    WaitFor(Rotator->Start())
        .ThrowOnError();

    for (int i = 0; i < 5; ++i) {
        auto newConfig = New<TKeyRotatorConfig>();
        // Gradually decreases: 5s -> 2s -> 1s -> 500ms -> 200ms.
        newConfig->KeyRotationInterval = TDuration::Seconds(5) / (1 << i);
        Rotator->Reconfigure(newConfig);
    }

    // Final reconfiguration with a very short interval to overshadow everything that has been before.
    auto finalConfig = New<TKeyRotatorConfig>();
    finalConfig->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator->Reconfigure(finalConfig);
    Sleep(finalConfig->KeyRotationInterval * 300);

    WaitFor(Rotator->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
