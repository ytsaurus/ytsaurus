#include <gtest/gtest.h>

#include <yt/yt/ytlib/api/native/transaction_helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <barrier>

namespace NYT::NApi::NNative {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TransactionSignatureGeneratorTest, Simple)
{
    TTransactionSignatureGenerator generator(/*targetSignature*/ 0xfe);
    generator.RegisterRequest();
    generator.RegisterRequests(/*count*/ 2);

    EXPECT_EQ(0xfcu, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TUniformSignatureGeneratorTest, Simple)
{
    TUniformSignatureGenerator generator;
    generator.RegisterRequest();
    generator.RegisterRequests(/*count*/ 2);

    EXPECT_EQ(0x1u, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
    EXPECT_EQ(0x3u, generator.GetFinalSignature());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TUniformSignatureGeneratorTest, ConcurrentUnregisterKeepsGenerationConsistent)
{
    constexpr int IterationCount = 100'000;

    TUniformSignatureGenerator generator;
    generator.RegisterRequests(/*count*/ 1);
    generator.GenerateSignature();

    std::barrier barrier(2, [&] () noexcept {
        generator.RegisterRequests(/*count*/ 1);
    });

    auto actionQueue = New<TActionQueue>("Unregisterer");
    auto unregistered = BIND([&] {
        for (int index = 0; index < IterationCount; ++index) {
            barrier.arrive_and_wait();
            generator.UnregisterRequests(/*count*/ 1);
        }
    }).AsyncVia(actionQueue->GetInvoker()).Run();

    for (int index = 0; index < IterationCount; ++index) {
        barrier.arrive_and_wait();
        EXPECT_EQ(0x1u, generator.GenerateSignature());
    }

    WaitFor(unregistered).ThrowOnError();

    EXPECT_EQ(0x1u, generator.GetFinalSignature());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi::NNative
