#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/ordered_memory.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TOrderedMemoryTest, Integer)
{
    using TIntegerMemory = TOrderedMemory<i64, i64>;
    auto memory = New<TIntegerMemory>();
    EXPECT_FALSE(memory->IsRegistered(1));
    memory->Register(5, 12);
    EXPECT_FALSE(memory->IsRegistered(8));
    memory->Register(6, 15);
    memory->Register(5, 200);
    memory->Register(6, 201);
    memory->Register(8, 11);
    memory->Register(7, 202);
    EXPECT_EQ(memory->size(), 3);
    EXPECT_EQ(memory->Extract(4), 12);
    EXPECT_EQ(memory->Extract(5), 12);
    EXPECT_EQ(memory->Extract(6), 15);
    EXPECT_EQ(memory->Extract(7), 11);
    EXPECT_EQ(memory->Extract(8), 11);
    EXPECT_TRUE(memory->IsRegistered(3));
    EXPECT_TRUE(memory->IsRegistered(8));
    EXPECT_FALSE(memory->IsRegistered(9));
    memory->AdvanceExclusive(6);
    EXPECT_TRUE(memory->IsRegistered(3));
    EXPECT_EQ(memory->size(), 2);
    EXPECT_EQ(memory->Extract(4), 15);
    EXPECT_EQ(memory->Extract(5), 15);
    EXPECT_EQ(memory->Extract(6), 15);
    EXPECT_EQ(memory->Extract(7), 11);
    EXPECT_EQ(memory->Extract(8), 11);
    memory->AdvanceExclusive(7);
    EXPECT_EQ(memory->size(), 1);
    EXPECT_EQ(memory->Extract(4), 11);
    EXPECT_EQ(memory->Extract(5), 11);
    EXPECT_EQ(memory->Extract(6), 11);
    EXPECT_EQ(memory->Extract(7), 11);
    EXPECT_EQ(memory->Extract(8), 11);
    memory->AdvanceExclusive(8);
    EXPECT_EQ(memory->size(), 1);
    EXPECT_TRUE(memory->IsRegistered(7)); // Glass box test, is not a contract. Moreover it is a misuse of class.
    EXPECT_EQ(memory->Extract(4), 11);
    EXPECT_EQ(memory->Extract(5), 11);
    EXPECT_EQ(memory->Extract(6), 11);
    EXPECT_EQ(memory->Extract(7), 11);
    EXPECT_EQ(memory->Extract(8), 11);
    memory->AdvanceExclusive(9);
    EXPECT_TRUE(memory->empty());
    EXPECT_FALSE(memory->IsRegistered(8)); // Glass box test, is not a contract. Moreover it is a misuse of class.
    EXPECT_FALSE(memory->IsRegistered(9));

    memory->Register(10, 300);
    EXPECT_FALSE(memory->empty());
    memory->clear();
    EXPECT_TRUE(memory->empty());
}

TEST(TOrderedMemoryTest, Serialize)
{
    using TIntegerMemory = TOrderedMemory<i64, i64>;
    auto memory = New<TIntegerMemory>();
    memory->Register(5, 12);
    memory->Register(6, 15);
    memory->Register(8, 11);
    memory->Register(7, 202);
    EXPECT_EQ(memory->size(), 3);
    EXPECT_EQ(memory->Extract(4), 12);
    EXPECT_EQ(memory->Extract(5), 12);
    EXPECT_EQ(memory->Extract(6), 15);
    EXPECT_EQ(memory->Extract(7), 11);
    EXPECT_EQ(memory->Extract(8), 11);
    auto other = ConvertTo<TIntrusivePtr<TIntegerMemory>>(ConvertToYsonString(memory));
    EXPECT_EQ(other->size(), 3);
    EXPECT_EQ(other->Extract(4), 12);
    EXPECT_EQ(other->Extract(5), 12);
    EXPECT_EQ(other->Extract(6), 15);
    EXPECT_EQ(other->Extract(7), 11);
    EXPECT_EQ(other->Extract(8), 11);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
