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
            .WillRepeatedly(ReturnRef(OwnerId));
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

} // namespace
} // namespace NYT::NSignature
