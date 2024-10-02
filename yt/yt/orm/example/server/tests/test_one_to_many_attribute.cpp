#include "common.h"

#include <yt/yt/orm/example/server/library/autogen/objects.h>

#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NExample::NServer::NTests {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

class TOneToManyAttributeTestSuiteBase
    : public ::testing::Test
{
protected:
    TObjectKey Publisher_;
    TObjectKey PublisherGroup_;

    void SetUp() override
    {
        Publisher_ = CreatePublisher();
        PublisherGroup_ = CreatePublisher();
    }

    void TearDown() override
    {
        auto transaction = StartTransaction();
        auto [publisher, publisherGroup] = GetObjects<NLibrary::TPublisher>(transaction, Publisher_, PublisherGroup_);

        publisherGroup->Status().Publishers().Clear();
        transaction->RemoveObjects({publisherGroup, publisher});

        CommitTransaction(std::move(transaction));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOneToManyAttributeTestSuite
    : public TOneToManyAttributeTestSuiteBase
{ };

TEST_F(TOneToManyAttributeTestSuite, TestLoadOld)
{
    {
        auto transaction = StartTransaction();
        auto [publisher, publisherGroup] = GetObjects<NLibrary::TPublisher>(transaction, Publisher_, PublisherGroup_);
        auto& publishers = publisherGroup->Status().Publishers();

        publishers.Add(publisher);

        EXPECT_EQ(publishers.Load(), std::vector{publisher});
        EXPECT_TRUE(publishers.LoadOld().empty());

        CommitTransaction(std::move(transaction));
    }

    {
        auto transaction = StartTransaction();
        auto [publisher, publisherGroup] = GetObjects<NLibrary::TPublisher>(transaction, Publisher_, PublisherGroup_);
        auto& publishers = publisherGroup->Status().Publishers();

        publishers.Remove(publisher);

        EXPECT_EQ(publishers.LoadOld(), std::vector{publisher});
        EXPECT_TRUE(publishers.Load().empty());

        CommitTransaction(std::move(transaction));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NTests
