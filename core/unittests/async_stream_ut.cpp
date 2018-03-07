#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

TString GetString(const TSharedRef& sharedRef)
{
    return TString(sharedRef.Begin(), sharedRef.Size());
}

TSharedRef ReadAlreadySetValue(const IAsyncZeroCopyInputStreamPtr& input)
{

    auto result = input->Read();
    {
        EXPECT_TRUE(result.IsSet());
        // We can't use ASSERT_ in non-void functions (check gtest FAQ)
        // so we use TryGet() here in order to avoid hanging and make test crash.
        EXPECT_TRUE(result.TryGet()->IsOK());
    }
    return result.TryGet()->Value();
}

TEST(TAsyncOutputStreamTest, Simple)
{
    auto pipe = New<TAsyncStreamPipe>();
    auto asyncWriter = CreateZeroCopyAdapter(static_cast<IAsyncOutputStreamPtr>(pipe));

    auto writeResult = asyncWriter->Write(TSharedRef::FromString("foo"));
    ASSERT_FALSE(writeResult.IsSet());

    auto readResult1 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult1), "foo");
    ASSERT_TRUE(writeResult.IsSet());
    ASSERT_TRUE(writeResult.Get().IsOK());

    auto closeResult = asyncWriter->Close();
    ASSERT_TRUE(writeResult.IsSet());

    auto readResult2 = ReadAlreadySetValue(pipe);
    ASSERT_FALSE(readResult2);
}

TEST(TAsyncOutputStreamTest, MultipleWrites)
{
    auto pipe = New<TAsyncStreamPipe>();
    auto asyncWriter = CreateZeroCopyAdapter(static_cast<IAsyncOutputStreamPtr>(pipe));

    auto writeResult1 = asyncWriter->Write(TSharedRef::FromString("foo"));
    auto writeResult2 = asyncWriter->Write(TSharedRef::FromString("bar"));
    auto writeResult3 = asyncWriter->Write(TSharedRef::FromString("baz"));
    auto closeResult = asyncWriter->Close();

    ASSERT_FALSE(writeResult1.IsSet());
    ASSERT_FALSE(writeResult2.IsSet());
    ASSERT_FALSE(writeResult3.IsSet());
    ASSERT_FALSE(closeResult.IsSet());

    auto readResult1 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult1), "foo");
    ASSERT_TRUE(writeResult1.IsSet());
    ASSERT_FALSE(writeResult2.IsSet());
    ASSERT_FALSE(writeResult3.IsSet());
    ASSERT_FALSE(closeResult.IsSet());

    auto readResult2 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult2), "bar");
    ASSERT_TRUE(writeResult1.IsSet());
    ASSERT_TRUE(writeResult2.IsSet());
    ASSERT_FALSE(writeResult3.IsSet());
    ASSERT_FALSE(closeResult.IsSet());

    auto readResult3 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult3), "baz");
    ASSERT_TRUE(writeResult1.IsSet());
    ASSERT_TRUE(writeResult2.IsSet());
    ASSERT_TRUE(writeResult3.IsSet());
    ASSERT_TRUE(closeResult.IsSet());

    auto readResult4 = ReadAlreadySetValue(pipe);
    ASSERT_FALSE(readResult4);
}

TEST(TAsyncOutputStreamTest, TestEmptyString)
{
    auto pipe = New<TAsyncStreamPipe>();
    auto asyncWriter = CreateZeroCopyAdapter(static_cast<IAsyncOutputStreamPtr>(pipe));

    auto writeResult1 = asyncWriter->Write(TSharedRef::FromString(""));
    auto writeResult2 = asyncWriter->Write(TSharedRef::FromString(""));
    auto closeResult = asyncWriter->Close();

    auto readResult1 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult1), "");

    auto readResult2 = ReadAlreadySetValue(pipe);
    ASSERT_EQ(GetString(readResult1), "");

    auto readResult3 = ReadAlreadySetValue(pipe);
    ASSERT_FALSE(readResult3);
    ASSERT_TRUE(writeResult1.IsSet());
    ASSERT_TRUE(writeResult2.IsSet());
    ASSERT_TRUE(closeResult.IsSet());
}

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NYT
