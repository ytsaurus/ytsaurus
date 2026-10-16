#include <yt/yt/core/test_framework/framework.h>

#include "chunk_helpers.h"

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

namespace NYT::NChunkServer {
namespace {

using namespace NTesting;

using NChunkClient::EChunkListKind;

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeTest
    : public TChunkGeneratorTestBase
{ };

TEST_F(TChunkTreeTest, DeriveScratchDetachPolicy)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    EXPECT_EQ(EChunkDetachPolicy::Scratch, DeriveChunkTreeDetachPolicy(chunkList));
}

TEST_F(TChunkTreeTest, DeriveDetachPolicyForUnsupportedKinds)
{
    for (auto kind : TEnumTraits<EChunkListKind>::GetDomainValues()) {
        if (kind == EChunkListKind::Scratch) {
            continue;
        }
        EXPECT_THROW_WITH_SUBSTRING(
            DeriveChunkTreeDetachPolicy(CreateChunkList(kind)),
            "Cannot derive detach policy");
    }
}

TEST_F(TChunkTreeTest, CannotAttachAlreadyAttachedChild)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    auto* chunk = CreateChunk(1, 1, 1, 1);

    AttachToChunkList(chunkList, {chunk});

    EXPECT_THROW_WITH_SUBSTRING(
        AttachToChunkList(chunkList, {chunk}),
        "Cannot append a duplicate child");

    EXPECT_EQ(1, std::ssize(chunkList->Children()));
    EXPECT_EQ(1, GetParentCount(chunk));
}

TEST_F(TChunkTreeTest, CannotAttachDuplicateChildrenInOneBatch)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    auto* chunk = CreateChunk(1, 1, 1, 1);
    auto* otherChunk = CreateChunk(1, 1, 1, 1);

    EXPECT_THROW_WITH_SUBSTRING(
        AttachToChunkList(chunkList, {chunk, otherChunk, chunk}),
        "Cannot append a duplicate child");

    // The check runs before anything is appended, so the batch is rejected whole.
    EXPECT_TRUE(chunkList->Children().empty());
    EXPECT_EQ(0, GetParentCount(chunk));
    EXPECT_EQ(0, GetParentCount(otherChunk));
}

TEST_F(TChunkTreeTest, CannotAttachSealedAfterUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(true, false)}),
        TErrorException);
}

TEST_F(TChunkTreeTest, CannotAttachSealedAfterUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false), CreateJournalChunk(true, false)}),
        TErrorException);
}

TEST_F(TChunkTreeTest, CanAttachUnsealedAfterSealed)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(true, false)});
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
}

TEST_F(TChunkTreeTest, CannotHaveMultipleNonoverlayedUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkTreeTest, CannotHaveMultipleNonoverlayedUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false), CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkTreeTest, CannotHaveNonoverlayedAfterOverlayed)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkTreeTest, CanHaveMultipleOverlayedUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
}

TEST_F(TChunkTreeTest, CanHaveMultipleOverlayedUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true), CreateJournalChunk(false, true)});
}

TEST_F(TChunkTreeTest, DetachFromScratchChunkList)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    std::vector<TChunkTreeRawPtr> chunks;
    for (int index = 0; index < 4; ++index) {
        chunks.push_back(CreateChunk(1, 1, 1, 1));
    }
    AttachToChunkList(chunkList, chunks);

    DetachFromChunkList(chunkList, {chunks[1]}, EChunkDetachPolicy::Scratch);

    EXPECT_EQ(3, std::ssize(chunkList->Children()));
    EXPECT_FALSE(chunkList->HasChild(chunks[1]));
    for (int index : {0, 2, 3}) {
        EXPECT_TRUE(chunkList->HasChild(chunks[index]));
    }
}

TEST_F(TChunkTreeTest, CannotDetachForeignChild)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    auto* chunk = CreateChunk(1, 1, 1, 1);
    auto* foreignChunk = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(chunkList, {chunk});

    EXPECT_THROW_WITH_SUBSTRING(
        DetachFromChunkList(chunkList, {foreignChunk}, EChunkDetachPolicy::Scratch),
        "has no child");

    EXPECT_EQ(1, std::ssize(chunkList->Children()));
}

TEST_F(TChunkTreeTest, CannotDetachDuplicateChildren)
{
    auto* chunkList = CreateChunkList(EChunkListKind::Scratch);
    auto* chunk = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(chunkList, {chunk});

    EXPECT_THROW_WITH_SUBSTRING(
        DetachFromChunkList(chunkList, {chunk, chunk}, EChunkDetachPolicy::Scratch),
        "Cannot detach a duplicate child");

    // Validation runs before anything is mutated, so the child is still attached.
    EXPECT_EQ(1, std::ssize(chunkList->Children()));
    EXPECT_EQ(1, GetParentCount(chunk));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
