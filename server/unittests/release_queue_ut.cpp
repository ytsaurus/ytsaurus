#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/guid.h>
#include <yt/core/misc/optional.h>

#include <yt/server/misc/release_queue.h>

namespace NYT {
namespace {

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TReleaseQueueTest
    : public Test
{
public:
    void Reset()
    {
        Queue_.emplace();
    }

    TGuid Push()
    {
        auto guid = TGuid::Create();
        YCHECK(InQueue_.insert(guid).second);
        Queue_->Push(guid);
        return guid;
    }

    TReleaseQueue<TGuid>::TCookie Checkpoint()
    {
        return Queue_->Checkpoint();
    }

    std::vector<TGuid> Release(TReleaseQueue<TGuid>::TCookie limit = std::numeric_limits<TReleaseQueue<TGuid>::TCookie>::max())
    {
        auto released = Queue_->Release(limit);
        for (const auto& value : released) {
            EXPECT_TRUE(InQueue_.erase(value));
        }
        return released;
    }

    virtual void SetUp() override
    {
        Reset();
    }

protected:
    std::optional<TReleaseQueue<TGuid>> Queue_;

private:
    THashSet<TGuid> InQueue_;
};

TEST_F(TReleaseQueueTest, Correctness1)
{
    auto v0 = Push();
    auto v1 = Push();
    auto c2 = Checkpoint();
    auto v2 = Push();
    auto v3 = Push();
    auto c4 = Checkpoint();
    auto v4 = Push();

    EXPECT_EQ(0, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(c2), ElementsAre(v0, v1));
    EXPECT_EQ(2, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(c4), ElementsAre(v2, v3));
    EXPECT_EQ(4, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(), ElementsAre(v4));
    EXPECT_EQ(5, Queue_->GetHeadCookie());
}

TEST_F(TReleaseQueueTest, Correctness2)
{
    auto v0 = Push();
    auto v1 = Push();
    auto c2 = Checkpoint();
    auto v2 = Push();
    auto v3 = Push();
    auto c4 = Checkpoint();
    auto v4 = Push();

    EXPECT_EQ(0, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(c4), ElementsAre(v0, v1, v2, v3));
    EXPECT_EQ(4, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(c2), ElementsAre());
    EXPECT_EQ(4, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(c4), ElementsAre());
    EXPECT_EQ(4, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(), ElementsAre(v4));
    EXPECT_EQ(5, Queue_->GetHeadCookie());
    EXPECT_THAT(Release(), ElementsAre());
    EXPECT_EQ(5, Queue_->GetHeadCookie());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
