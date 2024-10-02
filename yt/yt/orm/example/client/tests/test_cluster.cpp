#include "common.h"

namespace NYT::NOrm::NExample::NClient::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNativeClientTransactionTestSuite
    : public TNativeClientTestSuite
{
public:
    void SetUp() override
    {
        TNativeClientTestSuite::SetUp();
        WaitAllMastersAlive();
    }

    TTransactionId StartTransaction()
    {
        return WaitFor(Client_->StartTransaction())
            .ValueOrThrow().TransactionId;
    }

    void CommitTransaction(TTransactionId transactionId)
    {
        return WaitFor(Client_->CommitTransaction(transactionId))
            .ThrowOnError();
    }

    void AbortTransaction(TTransactionId transactionId)
    {
        return WaitFor(Client_->AbortTransaction(transactionId))
            .ThrowOnError();
    }

    IClientPtr CreateAdditionalClient()
    {
        return CreateClient();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNativeClientTransactionTestSuite, TestCommitTransaction)
{
    ASSERT_NO_THROW(CommitTransaction(StartTransaction()));
}

TEST_F(TNativeClientTransactionTestSuite, TestAbortTransaction)
{
    ASSERT_NO_THROW(AbortTransaction(StartTransaction()));
}

TEST_F(TNativeClientTransactionTestSuite, TestAttachTransaction)
{
    auto transactionId = WaitFor(CreateAdditionalClient()->StartTransaction())
        .ValueOrThrow().TransactionId;
    ASSERT_NO_THROW(CommitTransaction(transactionId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NExample::NClient::NTests
