#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/library/blob_table/blob_table.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/yexception.h>


using namespace NYT;
using namespace NYT::NTesting;
using namespace NYtBlobTable;

TEST(TBlobTableTest, Simple)
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

    EXPECT_EQ(blobTable.IsSorted(), false);
    blobTable.Sort();
    EXPECT_EQ(blobTable.IsSorted(), true);

    {
        auto fooStream = blobTable.ReadBlob({"foo.txt"});
        EXPECT_EQ(fooStream->ReadAll(), "FOO");
    }

    {
        auto bigStream = blobTable.ReadBlob({"big.txt"});
        EXPECT_EQ(bigStream->ReadAll(), bigData);
    }
}

TEST(TBlobTableTest, UnexistingTable)
{
    auto client = CreateTestClient();

    auto create = [&] () {
        NYtBlobTable::TBlobTable blobTable(client, "//testing/blobtable", TBlobTableSchema());
    };
    EXPECT_THROW(create(), yexception);
}

TEST(TBlobTableTest, SortFailForNonuniqueBlobs)
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

    EXPECT_THROW(blobTable.Sort(), yexception);
}

TEST(TBlobTableTest, ResetSorting)
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

    EXPECT_THROW(addBar(), yexception);
    blobTable.ResetSorting();
    addBar();

    blobTable.Sort();

    EXPECT_EQ(blobTable.ReadBlob("foo.txt")->ReadAll(), "FOO");
    EXPECT_EQ(blobTable.ReadBlob("bar.txt")->ReadAll(), "BAR");
}
