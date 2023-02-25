#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/library/blob_table/blob_table.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>


using namespace NYT;
using namespace NYT::NTesting;
using namespace NYtBlobTable;

Y_UNIT_TEST_SUITE(BlobTable) {
    Y_UNIT_TEST(Simple)
    {
        auto client = CreateTestClient();

        const TBlobTableSchema schema;

        NYtBlobTable::TBlobTable blobTable(client, "//testing/blobtable", schema, TBlobTableOpenOptions().Create(true));

        {
            auto fooStream = blobTable.AppendBlob({"foo.txt"});
            fooStream->Write("FOO");
            fooStream->Finish();
        }

        const TString bigData = GenerateRandomData(10 * 1024 * 1024);

        {
            auto bigStream = blobTable.AppendBlob({"big.txt"});
            bigStream->Write(bigData);
            bigStream->Finish();
        }

        UNIT_ASSERT_VALUES_EQUAL(blobTable.IsSorted(), false);
        blobTable.Sort();
        UNIT_ASSERT_VALUES_EQUAL(blobTable.IsSorted(), true);

        {
            auto fooStream = blobTable.ReadBlob({"foo.txt"});
            UNIT_ASSERT_VALUES_EQUAL(fooStream->ReadAll(), "FOO");
        }

        {
            auto bigStream = blobTable.ReadBlob({"big.txt"});
            UNIT_ASSERT_EQUAL(bigStream->ReadAll(), bigData);
        }
    }

    Y_UNIT_TEST(UnexistingTable)
    {
        auto client = CreateTestClient();

        auto create = [&] () {
            NYtBlobTable::TBlobTable blobTable(client, "//testing/blobtable", TBlobTableSchema());
        };
        UNIT_ASSERT_EXCEPTION(create(), yexception);
    }

    Y_UNIT_TEST(SortFailForNonuniqueBlobs)
    {
        auto client = CreateTestClient();

        const TBlobTableSchema schema;

        NYtBlobTable::TBlobTable blobTable(client, "//testing/blobtable", schema, TBlobTableOpenOptions().Create(true));

        {
            auto fooStream = blobTable.AppendBlob({"foo.txt"});
            fooStream->Write("FOO");
            fooStream->Finish();
        }

        {
            auto fooStream = blobTable.AppendBlob({"foo.txt"});
            fooStream->Write("BAR");
            fooStream->Finish();
        }

        UNIT_ASSERT_EXCEPTION(blobTable.Sort(), yexception);
    }

    Y_UNIT_TEST(ResetSorting)
    {
        auto client = CreateTestClient();

        const TBlobTableSchema schema;

        NYtBlobTable::TBlobTable blobTable(client, "//testing/blobtable", schema, TBlobTableOpenOptions().Create(true));

        {
            auto stream = blobTable.AppendBlob({"foo.txt"});
            stream->Write("FOO");
            stream->Finish();
        }

        blobTable.Sort();

        auto addBar = [&] {
            auto stream = blobTable.AppendBlob({"bar.txt"});
            stream->Write("BAR");
            stream->Finish();
        };

        UNIT_ASSERT_EXCEPTION(addBar(), yexception);
        blobTable.ResetSorting();
        addBar();

        blobTable.Sort();

        UNIT_ASSERT_VALUES_EQUAL(blobTable.ReadBlob("foo.txt")->ReadAll(), "FOO");
        UNIT_ASSERT_VALUES_EQUAL(blobTable.ReadBlob("bar.txt")->ReadAll(), "BAR");
    }
}
