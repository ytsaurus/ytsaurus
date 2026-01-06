#include <yt/yt/server/lib/squash_fs/squash_fs_layout_builder.h>

#include <yt/yt/server/lib/nbd/random_access_file_reader.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/system/shellcommand.h>

namespace NYT {

using namespace NConcurrency;
using namespace NGTest;
using namespace NNbd;
using namespace NSquashFS;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TMockReader
    : public IRandomAccessFileReader
{
public:
    explicit TMockReader(i64 size)
        : Size_(size)
    { }

    void Initialize() override
    { }

    TFuture<TSharedRef> Read(
        i64 offset,
        i64 length,
        const TReadOptions& /*options*/) override
    {
        if (offset < 0 ||
            length < 0 ||
            offset + length > Size_)
        {
            return MakeFuture<TSharedRef>(TError(
                "Wrong read offset %v with length %v (Size: %v)",
                offset,
                length,
                Size_));
        }

        if (length == 0) {
            return MakeFuture<TSharedRef>({});
        }

        return MakeFuture<TSharedRef>(TSharedMutableRef::Allocate(length));
    }

    i64 GetSize() const override
    {
        return Size_;
    }

    TReadersStatistics GetStatistics() const override
    {
        return TReadersStatistics();
    }

    const std::string& GetPath() const override
    {
        return Path_;
    }

private:
    const std::string Path_ = "MockRandomAccessFileReader";

    i64 Size_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRandomAccessFileReaderPtr CreateMockReader(i64 size)
{
    return New<TMockReader>(size);
}

////////////////////////////////////////////////////////////////////////////////

std::string BuildAndGetText(ISquashFSLayoutBuilderPtr builder)
{
    auto layout = builder->Build();
    TBlobOutput blobOutput;
    layout->DumpHexText(blobOutput);
    TBlob& blob = blobOutput.Blob();
    return std::string(blob.Begin(), blob.End());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSquashFSCanondataTest, EmptySquashFS)
{
    TSquashFSLayoutBuilderOptions options = {
        .BlockSize = 512_KB
    };
    auto builder = CreateSquashFSLayoutBuilder(options);
    auto result = BuildAndGetText(builder);
    EXPECT_THAT(result, GoldenFileEq(SRC_("canondata/1_empty_squash_fs")));
}

TEST(TSquashFSCanondataTest, SimpleSquashFS)
{
    TSquashFSLayoutBuilderOptions options = {
        .BlockSize = 512_KB
    };
    auto builder = CreateSquashFSLayoutBuilder(options);
    builder->AddFile(
        /*path*/ "/a/b.txt",
        /*permissions*/ 0777,
        /*reader*/ CreateMockReader(/*size*/ 1));
    builder->AddFile(
        /*path*/ "/a/c.txt",
        /*permissions*/ 0777,
        /*reader*/ CreateMockReader(/*size*/ 2));
    builder->AddFile(
        /*path*/ "/d.txt",
        /*permissions*/ 0777,
        /*reader*/ CreateMockReader(/*size*/ 3));
    auto result = BuildAndGetText(builder);
    EXPECT_THAT(result, GoldenFileEq(SRC_("canondata/2_simple_squash_fs")));
}

TEST(TSquashFSCanondataTest, BigFileSquashFS)
{
    TSquashFSLayoutBuilderOptions options = {
        .BlockSize = 4_KB
    };
    auto builder = CreateSquashFSLayoutBuilder(options);
    builder->AddFile(
        /*path*/ "/a",
        /*permissions*/ 0777,
        /*reader*/ CreateMockReader(/*size*/ 5000));
    auto result = BuildAndGetText(builder);
    EXPECT_THAT(result, GoldenFileEq(SRC_("canondata/3_big_file_squash_fs")));
}

TEST(TSquashFSCanondataTest, ManyFilesSquashFS)
{
    TSquashFSLayoutBuilderOptions options = {
        .BlockSize = 512_KB
    };
    auto builder = CreateSquashFSLayoutBuilder(options);
    for (size_t i = 1; i <= 140; ++i) {
        builder->AddFile(
            /*path*/ "/" + std::to_string(i),
            /*permissions*/ 0777,
            /*reader*/ CreateMockReader(/*size*/ 1));
    }

    auto result = BuildAndGetText(builder);
    EXPECT_THAT(result, GoldenFileEq(SRC_("canondata/4_many_files_squash_fs")));
}

////////////////////////////////////////////////////////////////////////////////

void BuildAndDump(
    const ISquashFSLayoutBuilderPtr& builder,
    const std::string& folderName)
{
    auto fileName = folderName + ".img";
    auto layout = builder->Build();
    TFileOutput fileOutput(fileName);
    layout->Dump(fileOutput);
}

bool Unsquash(const TString& folderName)
{
    auto fileName = folderName + ".img";
    if (!NFS::Exists(fileName)) {
        return false;
    }

    TShellCommand cmd("unsquashfs -d " + folderName + " " + fileName);
    cmd.Run();
    cmd.Wait();
    return NFS::Exists(folderName);
}

bool VerifyDirectory(
    const TYPath& path,
    const std::set<std::string>& expectedEntries)
{
    if (!NFS::Exists(path)) {
        return false;
    }

    std::set<std::string> realEntries;
    for (auto entry : NFS::EnumerateDirectories(path)) {
        realEntries.insert(entry);
    }

    for (auto entry : NFS::EnumerateFiles(path)) {
        realEntries.insert(entry);
    }

    return expectedEntries == realEntries;
}

bool VerifyFile(
    const TYPath& path,
    i64 size)
{
    return NFS::Exists(path) && NFS::GetPathStatistics(path).Size == size;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSquashFSUnsquashfsTest, EmptySquashFS)
{
    {
        // Prepare.
        TSquashFSLayoutBuilderOptions options = {
            .BlockSize = 1_MB
        };
        auto builder = CreateSquashFSLayoutBuilder(options);
        BuildAndDump(
            builder,
            "empty");
    }
    {
        // Check.
        EXPECT_TRUE(Unsquash("empty"));
    }
}

TEST(TSquashFSUnsquashfsTest, SimpleSquashFS)
{
    {
        // Prepare.
        TSquashFSLayoutBuilderOptions options = {
            .BlockSize = 1_MB
        };
        auto builder = CreateSquashFSLayoutBuilder(options);
        builder->AddFile(
            /*path*/ "/a/b.txt",
            /*permissions*/ 0775,
            /*reader*/ CreateMockReader(/*size*/ 1));
        builder->AddFile(
            /*path*/ "/a/c.txt",
            /*permissions*/ 0775,
            /*reader*/ CreateMockReader(/*size*/ 2));
        builder->AddFile(
            /*path*/ "/d.txt",
            /*permissions*/ 0775,
            /*reader*/ CreateMockReader(/*size*/ 3));
        BuildAndDump(
            builder,
            "simple");
    }
    {
        // Check.
        EXPECT_TRUE(Unsquash("simple"));

        EXPECT_TRUE(VerifyDirectory(
            /*path*/ "simple",
            /*expectedEntries*/ { "a", "d.txt" }));
        EXPECT_TRUE(VerifyDirectory(
            /*path*/ "simple/a",
            /*expectedEntries*/ { "b.txt", "c.txt" }));

        EXPECT_TRUE(VerifyFile(
            /*path*/ "simple/a/b.txt",
            /*size*/ 1));
        EXPECT_TRUE(VerifyFile(
            /*path*/ "simple/a/c.txt",
            /*size*/ 2));
        EXPECT_TRUE(VerifyFile(
            /*path*/ "simple/d.txt",
            /*size*/ 3));
    }
}

TEST(TSquashFSUnsquashfsTest, BigFileSquashFS)
{
    {
        // Prepare.
        TSquashFSLayoutBuilderOptions options = {
            .BlockSize = 4_KB
        };
        auto builder = CreateSquashFSLayoutBuilder(options);
        builder->AddFile(
            /*path*/ "/a",
            /*permissions*/ 0775,
            /*reader*/ CreateMockReader(/*size*/ 5000));
        BuildAndDump(
            builder,
            "big_file");
    }
    {
        // Check.
        EXPECT_TRUE(Unsquash("big_file"));

        EXPECT_TRUE(VerifyDirectory(
            /*path*/ "big_file",
            /*expectedEntries*/ { "a" }));

        EXPECT_TRUE(VerifyFile(
            /*path*/ "big_file/a",
            /*size*/ 5000));
    }
}

TEST(TSquashFSUnsquashfsTest, ManyFilesSquashFS)
{
    std::set<std::string> entries;
    {
        // Prepare.
        TSquashFSLayoutBuilderOptions options = {
            .BlockSize = 1_MB
        };
        auto builder = CreateSquashFSLayoutBuilder(options);
        for (int i = 1; i <= 200; ++i) {
            entries.insert(ToString(i));
            builder->AddFile(
                /*path*/ "/" + ToString(i),
            /*permissions*/ 0775,
            /*reader*/ CreateMockReader(/*size*/ i));
        }

        BuildAndDump(
            builder,
            "many_files");
    }
    {
        // Check.
        EXPECT_TRUE(Unsquash("many_files"));

        EXPECT_TRUE(VerifyDirectory(
            /*path*/ "many_files",
            /*expectedEntries*/ entries));

        for (int i = 1; i <= 200; i++) {
            EXPECT_TRUE(VerifyFile(
                /*path*/ "many_files/" + ToString(i),
                /*size*/ i));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
