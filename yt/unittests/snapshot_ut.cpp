#include "../ytlib/master/snapshot.h"

#include <library/unittest/registar.h>
#include <util/system/tempfile.h>

namespace NYT {

class TSnapshotTest
    : public TTestBase
{
    UNIT_TEST_SUITE(TSnapshotTest);
        UNIT_TEST(TestEmptySnapshot);
        UNIT_TEST(TestWriteRead);
    UNIT_TEST_SUITE_END();


public:
    void TestEmptySnapshot()
    {
        //TODO: add checksums
        TTempFile tempFile("tmp");
        {
            TSnapshotWriter writer(tempFile.Name(), 0);
            writer.Open(-1);
            writer.Close();
        }
        {
            TSnapshotWriter reader(tempFile.Name(), 0);
            reader.Open(-1);
            reader.Close();
        }
    }

    void TestWriteRead()
    {
        //TODO: add checksums
        TTempFile tempFile("tmp");
        TSnapshotWriter writer(tempFile.Name(), 0);
        writer.Open(-1);
        TOutputStream& outputStream = writer.GetStream();

        i32 recordCount = 1024;
        for (i32 i = 0; i < recordCount; ++i) {
               outputStream.Write(&i, sizeof(i32));
        }
        writer.Close();

        TSnapshotReader reader(tempFile.Name(), 0);
        reader.Open();
        TInputStream& inputStream = reader.GetStream();
        for (i32 i = 0; i < recordCount; ++i) {
            i32 data;
            i32 bytesRead = inputStream.Load(&data, sizeof(i32));
            UNIT_ASSERT_EQUAL(bytesRead, sizeof(i32));
            UNIT_ASSERT_EQUAL(i, data);
        }
        reader.Close();
    }
};

UNIT_TEST_SUITE_REGISTRATION(TSnapshotTest);

} // namespace NYT
