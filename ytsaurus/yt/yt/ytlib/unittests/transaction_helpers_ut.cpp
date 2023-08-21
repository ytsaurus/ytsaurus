#include <gtest/gtest.h>

#include <yt/yt/ytlib/api/native/transaction_helpers.h>

namespace NYT::NApi::NNative {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TransactionSignatureGenerator, Simple)
{
    TTransactionSignatureGenerator generator(/*targetSignature*/ 0xfe);
    generator.RegisterRequest();
    generator.RegisterRequests(/*count*/ 2);

    EXPECT_EQ(0xfcu, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
    EXPECT_EQ(0x1u, generator.GenerateSignature());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi::NNative
