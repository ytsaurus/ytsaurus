#include "stdafx.h"

#include <core/misc/checkpointable_stream.h>

#include <util/stream/str.h>

#include <contrib/testing/framework.h>

namespace NYT {

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

    char buffer[10];

    EXPECT_EQ(2, input->Read(buffer, 2));
    EXPECT_EQ("ab", TStringBuf(buffer, 2));

    EXPECT_EQ(2, input->Read(buffer, 2));
    EXPECT_EQ("c1", TStringBuf(buffer, 2));

    EXPECT_EQ(7, input->Read(buffer, 10));
    EXPECT_EQ("11ololo", TStringBuf(buffer, 7));
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

    char buffer[10];

    EXPECT_EQ(2, input->Read(buffer, 2));
    EXPECT_EQ("ab", TStringBuf(buffer, 2));

    input->Skip();

    EXPECT_EQ(1, input->Read(buffer, 1));
    EXPECT_EQ("u", TStringBuf(buffer, 1));

    input->Skip();

    EXPECT_EQ(2, input->Read(buffer, 2));
    EXPECT_EQ("ol", TStringBuf(buffer, 2));

    EXPECT_EQ(2, input->Read(buffer, 2));
    EXPECT_EQ("ol", TStringBuf(buffer, 2));

    input->Skip();

    EXPECT_EQ(0, input->Read(buffer, 10));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
