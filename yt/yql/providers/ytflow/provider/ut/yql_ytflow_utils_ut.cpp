#include "yql_ytflow_physical_finalizing_setup.h"

#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>

#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NTest {
namespace {

using namespace NNodes;

class TYtflowConsumers : public testing::Test, public TPhysicalFinalizingSetup {
protected:
    TYtflowConsumers()
        : Operation_(MakeSourceMap(MakeReadWrap(), NewWorld(), MakeSettings()))
        , Output_(MakeOutput(Operation_.Ptr()))
    {}

    TExprNode::TPtr MakeExtractMembers() {
        return Build<TCoExtractMembers>(Ctx_, Position_)
            .Input(TExprBase(Output_))
            .Members().Build()
            .Done().Ptr();
    }

    TExprNode::TPtr MakeRight() {
        return Build<TCoRight>(Ctx_, Position_)
            .Input(Operation_)
            .Done().Ptr();
    }

    TParentsMap MakeParents(const TExprNode::TPtr& root) {
        TParentsMap parents;
        GatherParents(*root, parents);
        return parents;
    }

    TYtflowOpBase Operation_;
    TExprNode::TPtr Output_;
};

TEST_F(TYtflowConsumers, UnknownConsumerReturnsNothing)
{
    auto consumer = MakeRight();
    auto parents = MakeParents(consumer);

    const auto consumers = GetConsumersUnordered(Operation_, parents);
    ASSERT_FALSE(consumers);
    EXPECT_FALSE(GetConsumersOrdered(Operation_, parents));
}

TEST_F(TYtflowConsumers, StrictLookupRejectsUnexpectedOperationParent)
{
    auto consumer = MakeRight();
    auto parents = MakeParents(consumer);

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        GetConsumersUnorderedStrict(Operation_, parents),
        TYqlPanic,
        "Unexpected consumer Right");
}

TEST_F(TYtflowConsumers, IgnoresLeftNodesWhenCollectingConsumers)
{
    auto sink = MakePersistentSink(NewWorld(), Output_);
    auto left = MakeLeft(Operation_.Ptr());
    auto root = Ctx_.NewList(Position_, {sink, left});
    auto parents = MakeParents(root);

    const auto consumers = GetConsumersUnorderedStrict(Operation_, parents);

    ASSERT_EQ(consumers.size(), 1);
    EXPECT_EQ(consumers.front().first, sink.Get());
    EXPECT_EQ(consumers.front().second.Raw(), Output_.Get());
}

TEST_F(TYtflowConsumers, AcceptsPersistentSink)
{
    auto sink = MakePersistentSink(NewWorld(), Output_);
    auto parents = MakeParents(sink);

    const auto consumers = GetConsumersUnordered(Operation_, parents);

    ASSERT_TRUE(consumers);
    ASSERT_EQ(consumers->size(), 1);
    EXPECT_EQ(consumers->front().first, sink.Get());
    EXPECT_EQ(consumers->front().second.Raw(), Output_.Get());
}

TEST_F(TYtflowConsumers, RejectsUnexpectedOutputParent)
{
    auto consumer = MakeExtractMembers();
    auto parents = MakeParents(consumer);

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        GetConsumersUnordered(Operation_, parents),
        TYqlPanic,
        "Unexpected consumer ExtractMembers");
}

TEST_F(TYtflowConsumers, RejectsUnexpectedSourcesListParent)
{
    auto root = Ctx_.NewCallable(Position_, "Unexpected", {Ctx_.NewList(Position_, {Output_})});
    auto parents = MakeParents(root);

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        GetConsumersUnordered(Operation_, parents),
        TYqlPanic,
        "Unexpected consumer Unexpected");
}

TEST_F(TYtflowConsumers, CollectsOperationsSharingSourcesList)
{
    auto first = MakeMap(Output_, NewWorld());
    auto second = MakeMap(Output_, NewWorld());
    second = Ctx_.ChangeChild(
        *second,
        TYtflowMapBase::idx_Sources,
        first->ChildPtr(TYtflowMapBase::idx_Sources));
    auto root = MakeRoot({MakeOutput(first), MakeOutput(second)});
    auto parents = MakeParents(root);
    const auto parentsIt = parents.find(Output_.Get());
    ASSERT_NE(parentsIt, parents.end());
    ASSERT_EQ(parentsIt->second.size(), 1);

    const auto consumers = GetConsumersUnordered(Operation_, parents);

    ASSERT_TRUE(consumers);
    ASSERT_EQ(consumers->size(), 2);
    THashSet<const TExprNode*> operations;
    for (const auto& [operation, output] : *consumers) {
        operations.insert(operation);
        EXPECT_EQ(output.Raw(), Output_.Get());
    }
    EXPECT_TRUE(operations.contains(first.Get()));
    EXPECT_TRUE(operations.contains(second.Get()));
}

} // anonymous namespace
} // namespace NYql::NYtflow::NTest
