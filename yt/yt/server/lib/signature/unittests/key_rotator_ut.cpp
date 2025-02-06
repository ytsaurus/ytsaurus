#include <yt/yt/core/test_framework/framework.h>

#include "stub_keystore.h"

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/key_rotator.h>
#include <yt/yt/server/lib/signature/signature_generator.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NSignature {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

using testing::AllOf;
using testing::Gt;
using testing::IsEmpty;
using testing::Lt;
using testing::Not;
using testing::SizeIs;

////////////////////////////////////////////////////////////////////////////////

struct TKeyRotatorTest
    : public ::testing::Test
{
    TKeyRotatorConfigPtr Config = New<TKeyRotatorConfig>();
    TStubKeyStorePtr Store = New<TStubKeyStore>();
    TSignatureGeneratorPtr Generator = New<TSignatureGenerator>(
        New<TSignatureGeneratorConfig>(),
        Store);
    TKeyRotatorPtr Rotator;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateOnStart)
{
    Config->KeyRotationInterval = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Generator);
    EXPECT_THAT(Store->Data, IsEmpty());
    WaitFor(Rotator->Start())
        .ThrowOnError();
    WaitFor(Rotator->Stop())
        .ThrowOnError();
    EXPECT_THAT(Store->Data[Store->GetOwner()], Not(IsEmpty()));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, PeriodicRotate)
{
    Config->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator = New<TKeyRotator>(Config, GetCurrentInvoker(), Generator);
    YT_UNUSED_FUTURE(Rotator->Start());
    Sleep(Config->KeyRotationInterval * 20);
    WaitFor(Rotator->Stop())
        .ThrowOnError();
    EXPECT_THAT(Store->Data[Store->GetOwner()], SizeIs(AllOf(Gt(3), Lt(50))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
