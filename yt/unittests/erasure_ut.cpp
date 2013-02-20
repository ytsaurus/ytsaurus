#include <ytlib/misc/foreach.h>
#include <ytlib/erasure_codecs/codec.h>

#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/erasure_writer.h>

#include <contrib/testing/framework.h>

#include <util/random/randcpp.h>
#include <util/system/fs.h>

////////////////////////////////////////////////////////////////////////////////

using NYT::TRef;
using NYT::TSharedRef;
using NYT::NErasure::ECodec;
using NYT::NErasure::GetCodec;
using namespace NYT::NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TErasureCodingTest
    : public ::testing::Test
{ };

TEST_F(TErasureCodingTest, SmallTest)
{
    TRand rand;

    std::map<ECodec::EDomain, int> guaranteedRecoveryCount;
    guaranteedRecoveryCount[ECodec::ReedSolomon_6_3] = 3;
    guaranteedRecoveryCount[ECodec::Lrc_12_2_2] = 3;

    std::vector<char> data;
    for (int i = 0; i < 16 * 64; ++i) {
        data.push_back(static_cast<char>('a' + (std::abs(rand.random()) % 26)));
    }

    FOREACH (auto codecId, ECodec::GetDomainValues()) {
        if (codecId == ECodec::None)
            continue;

        auto codec = GetCodec(codecId);

        int blocksCount = codec->GetDataBlockCount() + codec->GetParityBlockCount();
        YCHECK(blocksCount <= 16);

        std::vector<TSharedRef> dataBlocks;
        for (int i = 0; i < codec->GetDataBlockCount(); ++i) {
            auto begin = data.begin() + i * 64;
            std::vector<char> blob(begin, begin + 64);
            dataBlocks.push_back(TSharedRef::FromBlob(std::move(blob)));
        }
        

        auto parityBlocks = codec->Encode(dataBlocks);
        
        std::vector<TSharedRef> allBlocks(dataBlocks);
        std::copy(parityBlocks.begin(), parityBlocks.end(), std::back_inserter(allBlocks));
        
        for (int mask = 0; mask < (1 << blocksCount); ++mask) {
            std::vector<int> erasedIndices;
            for (int i = 0; i < blocksCount; ++i) {
                if ((mask & (1 << i)) > 0) {
                    erasedIndices.push_back(i);
                }
            }
            if (erasedIndices.size() == 1) continue;

            auto recoveryIndices = codec->GetRecoveryIndices(erasedIndices);
            if (erasedIndices.size() <= guaranteedRecoveryCount[codecId]) {
                EXPECT_TRUE(recoveryIndices);
            }


            if (recoveryIndices) {
                std::vector<TSharedRef> aliveBlocks;
                for (int i = 0; i < recoveryIndices->size(); ++i) {
                    aliveBlocks.push_back(allBlocks[(*recoveryIndices)[i]]);
                }
                std::vector<TSharedRef> recoveredBlocks = codec->Decode(aliveBlocks, erasedIndices);
                EXPECT_TRUE(recoveredBlocks.size() == erasedIndices.size());
                for (int i = 0; i < erasedIndices.size(); ++i) {
                    EXPECT_TRUE(TRef::CompareContent(
                        allBlocks[erasedIndices[i]],
                        recoveredBlocks[i]));
                }
            }
        }

    }
}

class TErasureWriterTest
    : public ::testing::Test
{
};


TEST_F(TErasureWriterTest, General)
{
    auto codec = GetCodec(ECodec::Lrc_12_2_2);

    std::vector<IAsyncWriterPtr> writers;
    for (int i = 0; i < codec->GetTotalBlockCount(); ++i) {
        Stroka filename = "block" + ToString(i + 1);
        writers.push_back(NYT::New<TFileWriter>(filename));
    }

    FOREACH(auto writer, writers) {
        writer->Open();
    }

    NProto::TChunkMeta meta;
    // What does it mean?
    meta.set_type(1);
    meta.set_version(1);

    auto erasureWriter = GetErasureWriter(codec, writers);
    erasureWriter->TryWriteBlock(TSharedRef::FromString("a"));
    erasureWriter->TryWriteBlock(TSharedRef::FromString("b"));
    erasureWriter->TryWriteBlock(TSharedRef::FromString(""));
    erasureWriter->TryWriteBlock(TSharedRef::FromString("Hello world"));
    erasureWriter->AsyncClose(meta).Get();

    FOREACH(auto writer, writers) {
        EXPECT_TRUE(writer->AsyncClose(meta).Get().IsOK());
    }

    for (int i = 0; i < codec->GetTotalBlockCount(); ++i) {
        Stroka filename = "block" + ToString(i + 1);
        if (i == 0) {
            EXPECT_EQ(Stroka("ab"), TFileInput("block" + ToString(i + 1)).ReadAll());
        }
        else if (i == 1) {
            EXPECT_EQ(Stroka("Hello world"), TFileInput("block" + ToString(i + 1)).ReadAll());
        }
        else if (i < 12) {
            EXPECT_EQ("", TFileInput("block" + ToString(i + 1)).ReadAll());
        }
        else {
            EXPECT_EQ(64, TFileInput("block" + ToString(i + 1)).ReadAll().Size());
        }
        NFs::Remove(filename.c_str());
        NFs::Remove((filename + ".meta").c_str());
    }
}
