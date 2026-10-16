#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TFilePartitionTest
    : public TApiTestBase
{
protected:
    static inline const TYPath Path = "//tmp/file_partition_test";

    void SetUp() override
    {
        TCreateNodeOptions options;
        options.Force = true;

        WaitFor(Client_->CreateNode(Path, EObjectType::File, options))
            .ThrowOnError();
    }

    //! Writes parts sequentially; every part past the first is appended,
    //! and every append session produces at least one new chunk,
    //! so passing several parts makes the file multichunk.
    static TString WriteFile(const std::vector<TString>& parts)
    {
        TString content;
        bool append = false;
        for (const auto& part : parts) {
            TRichYPath richPath(Path);
            richPath.SetAppend(append);
            append = true;

            auto writer = Client_->CreateFileWriter(richPath);
            WaitFor(writer->Open())
                .ThrowOnError();
            WaitFor(writer->Write(TSharedRef::FromString(part)))
                .ThrowOnError();
            WaitFor(writer->Close())
                .ThrowOnError();

            content += part;
        }
        return content;
    }

    static TFileReadRange MakeRange(i64 begin, std::optional<i64> end)
    {
        TFileReadRange range;
        range.Begin = begin;
        range.End = end;
        return range;
    }

    static TString ReadPartition(
        const TFilePartitionCookiePtr& cookie,
        const IClientPtr& client = Client_)
    {
        auto reader = WaitFor(client->CreateFilePartitionReader(cookie))
            .ValueOrThrow();
        auto data = reader->ReadAll();
        return TString(data.ToStringBuf());
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFilePartitionTest, PartitionAndReadRoundTrip)
{
    auto content = WriteFile({TString(1000, 'a'), TString(1000, 'b'), TString(1000, 'c')});
    auto fileLength = std::ssize(content);

    std::vector<TFileReadRange> ranges{
        MakeRange(0, 300),                // inside the first chunk
        MakeRange(300, 1500),             // crosses a chunk boundary
        MakeRange(1500, 1500),            // empty
        MakeRange(1500, 2900),
        MakeRange(2900, std::nullopt),    // up to EOF
    };

    auto partitions = WaitFor(Client_->PartitionFile(Path, ranges))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(partitions.Partitions), std::ssize(ranges));

    TString readContent;
    for (int index = 0; index < std::ssize(ranges); ++index) {
        const auto& partition = partitions.Partitions[index];

        auto begin = ranges[index].Begin;
        auto end = ranges[index].End.value_or(fileLength);
        EXPECT_EQ(partition.Length, end - begin);

        auto data = ReadPartition(partition.Cookie);
        EXPECT_EQ(data, content.substr(begin, end - begin));
        readContent += data;
    }
    EXPECT_EQ(readContent, content);
}

TEST_F(TFilePartitionTest, WholeFileSingleRange)
{
    auto content = WriteFile({TString(500, 'x'), TString(500, 'y')});

    auto partitions = WaitFor(Client_->PartitionFile(Path, {MakeRange(0, std::nullopt)}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(partitions.Partitions), 1);
    EXPECT_EQ(partitions.Partitions[0].Length, std::ssize(content));
    EXPECT_EQ(ReadPartition(partitions.Partitions[0].Cookie), content);
}

TEST_F(TFilePartitionTest, EndClampedToFileLength)
{
    auto content = WriteFile({TString(1000, 'z')});
    auto fileLength = std::ssize(content);

    auto partitions = WaitFor(Client_->PartitionFile(Path, {MakeRange(100, 10 * fileLength)}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(partitions.Partitions), 1);
    EXPECT_EQ(partitions.Partitions[0].Length, fileLength - 100);
    EXPECT_EQ(ReadPartition(partitions.Partitions[0].Cookie), content.substr(100));
}

TEST_F(TFilePartitionTest, InvalidRanges)
{
    auto content = WriteFile({TString(100, 'x')});
    auto fileLength = std::ssize(content);

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->PartitionFile(Path, {MakeRange(fileLength + 1, std::nullopt)})).ValueOrThrow(),
        "past the end of file");

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->PartitionFile(Path, {MakeRange(50, 10)})).ValueOrThrow(),
        "Invalid file read range");

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->PartitionFile(Path, {MakeRange(-1, std::nullopt)})).ValueOrThrow(),
        "Invalid file read range");
}

TEST_F(TFilePartitionTest, EmptyRangeList)
{
    WriteFile({TString(100, 'x')});

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->PartitionFile(Path, {})).ValueOrThrow(),
        "At least one file read range");
}

TEST_F(TFilePartitionTest, TooManyPartitions)
{
    WriteFile({TString(100, 'x')});

    // Default max_file_partition_count is 10'000.
    std::vector<TFileReadRange> ranges(10'001, MakeRange(0, std::nullopt));

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Client_->PartitionFile(Path, ranges)).ValueOrThrow(),
        "Too many file partitions requested");
}

TEST_F(TFilePartitionTest, CookieIsBoundToUser)
{
    WriteFile({TString(100, 'x')});

    auto partitions = WaitFor(Client_->PartitionFile(Path, {MakeRange(0, std::nullopt)}))
        .ValueOrThrow();
    ASSERT_EQ(std::ssize(partitions.Partitions), 1);

    static const std::string User = "file_partition_test_user";
    if (!WaitFor(Client_->NodeExists("//sys/users/" + User)).ValueOrThrow()) {
        TCreateObjectOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", User);
        options.Attributes = std::move(attributes);
        WaitFor(Client_->CreateObject(EObjectType::User, options))
            .ThrowOnError();
    }
    auto otherClient = CreateClient(User);

    EXPECT_THROW_WITH_SUBSTRING(
        ReadPartition(partitions.Partitions[0].Cookie, otherClient),
        "must be read by the same user");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
