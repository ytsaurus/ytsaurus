#include <yt/core/test_framework/framework.h>

#include <yt/core/logging/random_access_gzip.h>

#include <util/system/fs.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

TEST(TRandomAccessGZipTest, Write)
{
    NFs::Remove("test.txt.gz");

    {
        TFile rawFile("test.txt.gz", OpenAlways|RdWr|CloseOnExec);
        TRandomAccessGZipFile file(&rawFile);
        file << "foo\n";
        file.Flush();
        file << "bar\n";
        file.Finish();
    }
    {
        TFile rawFile("test.txt.gz", OpenAlways|RdWr|CloseOnExec);
        TRandomAccessGZipFile file(&rawFile);
        file << "zog\n";
        file.Finish();
    }

    auto input = TUnbufferedFileInput("test.txt.gz");
    TZLibDecompress decompress(&input);
    EXPECT_EQ("foo\nbar\nzog\n", decompress.ReadAll());

    NFs::Remove("test.txt.gz");
}

TEST(TRandomAccessGZipTest, RepairIncompleteBlocks)
{
    NFs::Remove("test.txt.gz");

    {
        TFile rawFile("test.txt.gz", OpenAlways|RdWr|CloseOnExec);
        TRandomAccessGZipFile file(&rawFile);
        file << "foo\n";
        file.Flush();
        file << "bar\n";
        file.Finish();
    }

    i64 fullSize;
    {
        TFile file("test.txt.gz", OpenAlways|RdWr);
        fullSize = file.GetLength();
        file.Resize(fullSize - 1);
    }

    {
        TFile rawFile("test.txt.gz", OpenAlways | RdWr | CloseOnExec);
        TRandomAccessGZipFile file(&rawFile);
    }

    {
        TFile file("test.txt.gz", OpenAlways|RdWr);
        EXPECT_LE(file.GetLength(), fullSize - 1);
    }

    NFs::Remove("test.txt.gz");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
