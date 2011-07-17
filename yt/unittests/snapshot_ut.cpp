#include "../ytlib/master/snapshot.h"

#include <util/random/random.h>
#include <util/system/tempfile.h>

#include "framework/framework.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotTest : public ::testing::Test {
protected:
    THolder<TTempFile> TemporaryFile;
    
    virtual void SetUp()
    {
        TemporaryFile.Reset(new TTempFile(
            Sprintf("Snapshot-%08" PRIx64 "-%08" PRIx64,
                MicroSeconds(),
                RandomNumber<ui64>())));
    }
};

TEST_F(TSnapshotTest, EmptySnapshot)
{
    // TODO: Add checksums.
    ASSERT_NO_THROW({
        TSnapshotWriter writer(TemporaryFile->Name(), 0);
        writer.Open(-1);
        writer.Close();
    });

    ASSERT_NO_THROW({
        TSnapshotReader reader(TemporaryFile->Name(), 0);
        reader.Open(-1);
        reader.Close();
    });
}

TEST_F(TSnapshotTest, WriteAndThenRead)
{
    // TODO: Add checksums.
    const i32 recordCount = 1024;

    TSnapshotWriter writer(TemporaryFile->Name(), 0);
    writer.Open(-1);
    TOutputStream& outputStream = writer.GetStream();

    for (i32 i = 0; i < recordCount; ++i) {
        outputStream.Write(&i, sizeof(i32));
    }

    writer.Close();

    TSnapshotReader reader(TemporaryFile->Name(), 0);
    reader.Open();
    TInputStream& inputStream = reader.GetStream();

    for (i32 i = 0; i < recordCount; ++i) {
        i32 data;
        i32 bytesRead = inputStream.Load(&data, sizeof(i32));

        ASSERT_EQ(static_cast<i32>(sizeof(i32)), bytesRead);
        ASSERT_EQ(i, data);
    }

    reader.Close();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

