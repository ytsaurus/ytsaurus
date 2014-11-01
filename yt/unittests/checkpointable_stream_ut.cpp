#include "stdafx.h"
#include "framework.h"

#include <core/misc/checkpointable_stream.h>

#include <util/stream/str.h>
#include <util/stream/mem.h>

#include <array>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCheckpointableStreamTest, Simple)
{

    TStringStream stringOutput;
    auto output = CreateCheckpointableOutputStream(&stringOutput);

    output->Write("abc");
    output->Write("111");
    output->Write("ololo");

    TStringInput stringInput(stringOutput.Str());
    auto input = CreateCheckpointableInputStream(&stringInput);

    std::array<char, 10> buffer;

    EXPECT_EQ(2, input->Read(buffer.data(), 2));
    EXPECT_EQ("ab", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(2, input->Read(buffer.data(), 2));
    EXPECT_EQ("c1", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(7, input->Read(buffer.data(), 10));
    EXPECT_EQ("11ololo", TStringBuf(buffer.data(), 7));
}

TEST(TCheckpointableStreamTest, Checkpoints)
{
    TStringStream stringOutput;
    auto output = CreateCheckpointableOutputStream(&stringOutput);

    output->Write("abc");
    output->Write("111");
    output->MakeCheckpoint();
    output->Write("u");
    output->MakeCheckpoint();
    output->Write("ololo");

    TStringInput stringInput(stringOutput.Str());
    auto input = CreateCheckpointableInputStream(&stringInput);

    std::array<char, 10> buffer;

    EXPECT_EQ(2, input->Read(buffer.data(), 2));
    EXPECT_EQ("ab", TStringBuf(buffer.data(), 2));

    input->SkipToCheckpoint();

    EXPECT_EQ(1, input->Read(buffer.data(), 1));
    EXPECT_EQ("u", TStringBuf(buffer.data(), 1));

    input->SkipToCheckpoint();

    EXPECT_EQ(2, input->Read(buffer.data(), 2));
    EXPECT_EQ("ol", TStringBuf(buffer.data(), 2));

    EXPECT_EQ(2, input->Read(buffer.data(), 2));
    EXPECT_EQ("ol", TStringBuf(buffer.data(), 2));

    input->SkipToCheckpoint();

    EXPECT_EQ(0, input->Read(buffer.data(), 10));
}

TEST(TCheckpointableStreamTest, Encapsulated)
{
    Stroka data("this is a test");
    TStringInput rawInput(data);
    auto encapsulatedInput = EscapsulateAsCheckpointableInputStream(&rawInput);
    auto checkpointableInput = CreateCheckpointableInputStream(encapsulatedInput.get());
    checkpointableInput->SkipToCheckpoint();
    EXPECT_EQ(data, checkpointableInput->ReadAll());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
