#include "stdafx.h"
#include "framework.h"

#include <ytlib/meta_state/private.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/meta_state/snapshot.h>
#include <ytlib/meta_state/snapshot_store.h>

#include <core/ytree/convert.h>

#include <util/random/random.h>
#include <util/system/tempfile.h>

namespace NYT {
namespace NMetaState {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotTest
    : public ::testing::Test
{
protected:
    std::unique_ptr<TTempFile> TemporaryFile;

    virtual void SetUp()
    {
        TemporaryFile.reset(new TTempFile(GenerateRandomFileName("Snapshot")));
    }

    virtual void TearDown()
    {
        NFs::Remove(TemporaryFile->Name());
        TemporaryFile.reset();
    }
};

TEST_F(TSnapshotTest, EmptySnapshot)
{
    // TODO: Add checksums.
    ASSERT_NO_THROW({
        TSnapshotWriterPtr writer = New<TSnapshotWriter>(
            TemporaryFile->Name(),
            0,
            true);
        writer->Open(NonexistingPrevRecordCount, TEpochId());
        writer->Close();
    });

    ASSERT_NO_THROW({
        TSnapshotReaderPtr reader = New<TSnapshotReader>(
            TemporaryFile->Name(),
            0,
            true);
        reader->Open();
    });
}

TEST_F(TSnapshotTest, WriteAndThenRead)
{
    // TODO: Add checksums.
    const i32 recordCount = 1024;
    const TEpochId epoch(1, 2);

    TSnapshotWriterPtr writer = New<TSnapshotWriter>(
        TemporaryFile->Name(),
        0,
        true);
    writer->Open(NonexistingPrevRecordCount, epoch);
    auto* outputStream = writer->GetStream();

    for (i32 i = 0; i < recordCount; ++i) {
        outputStream->Write(&i, sizeof(i32));
    }

    writer->Close();
    writer.Reset();

    TSnapshotReaderPtr reader = New<TSnapshotReader>(
        TemporaryFile->Name(),
        0,
        true);
    reader->Open();

    EXPECT_EQ(reader->GetPrevRecordCount(), NonexistingPrevRecordCount);
    EXPECT_EQ(reader->GetEpoch(), epoch);

    auto* inputStream = reader->GetStream();

    for (i32 i = 0; i < recordCount; ++i) {
        i32 data;
        i32 bytesRead = inputStream->Load(&data, sizeof(i32));

        EXPECT_EQ(static_cast<i32>(sizeof(i32)), bytesRead);
        EXPECT_EQ(i, data);
    }

    reader.Reset();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSnapshotTest, SnapshotStore)
{
    // TODO(sandello): Cleanup created files afterwards.
    TSnapshotStoreConfigPtr config = New<TSnapshotStoreConfig>();
    config->Path = ".";

    TSnapshotStorePtr store = New<TSnapshotStore>(config);
    store->Start();

    EXPECT_FALSE(store->GetReader(1).IsOK());

    { // Add snapshot file to store.
        i32 id = 2;
        auto writer = store->GetWriter(id);
        writer->Open(1, TEpochId());
        TOutputStream* output = writer->GetStream();
        std::vector<char> data(10, 42);
        output->Write(&*data.begin(), data.size());
        writer->Close();
        store->OnSnapshotAdded(id);
    }

    auto readerResult = store->GetReader(2);
    ASSERT_TRUE(readerResult.IsOK());
    auto reader = readerResult.Value();
    reader->Open();
    EXPECT_EQ(1, reader->GetPrevRecordCount());
    EXPECT_EQ(TEpochId(), reader->GetEpoch());

    EXPECT_FALSE(store->GetReader(3).IsOK());

    EXPECT_EQ(NonexistingSnapshotId, store->GetLatestSnapshotId(1));
    EXPECT_EQ(2, store->GetLatestSnapshotId(2));
    EXPECT_EQ(2, store->GetLatestSnapshotId(10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NMetaState
} // namespace NYT
