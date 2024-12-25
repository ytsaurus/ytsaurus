#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/key_rotator.h>
#include <yt/yt/server/lib/signature/signature_generator.h>

#include <yt/yt/server/lib/signature/key_stores/stub.h>

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
    TActionQueuePtr ActionQueue = New<TActionQueue>();
    TKeyRotatorConfigPtr Config = New<TKeyRotatorConfig>();
    TStubKeyStorePtr Store = New<TStubKeyStore>();
    TSignatureGeneratorPtr Generator = New<TSignatureGenerator>(
        New<TSignatureGeneratorConfig>(),
        Store);
    ISuspendableInvokerPtr Invoker = CreateSuspendableInvoker(ActionQueue->GetInvoker());
    TKeyRotatorPtr Rotator;

    TKeyRotatorTest()
    {
        Config->KeyRotationInterval = TDuration::MilliSeconds(10);
        Rotator = New<TKeyRotator>(Config, Invoker, Generator);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, RotateOnStart)
{
    Config->KeyRotationInterval = TDuration::Hours(10);
    Rotator = New<TKeyRotator>(Config, Invoker, Generator);
    EXPECT_THAT(Store->Data, IsEmpty());
    Rotator->Start();
    Sleep(TDuration::MilliSeconds(300));
    WaitFor(Invoker->Suspend()).ThrowOnError();
    EXPECT_THAT(Store->Data[Store->GetOwner()], Not(IsEmpty()));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TKeyRotatorTest, PeriodicRotate)
{
    Config->KeyRotationInterval = TDuration::MilliSeconds(10);
    Rotator = New<TKeyRotator>(Config, Invoker, Generator);
    Rotator->Start();
    Sleep(Config->KeyRotationInterval * 20);
    WaitFor(Invoker->Suspend()).ThrowOnError();
    EXPECT_THAT(Store->Data[Store->GetOwner()], SizeIs(AllOf(Gt(3), Lt(50))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NSignature
