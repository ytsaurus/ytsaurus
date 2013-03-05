#include <ytlib/misc/foreach.h>
#include <ytlib/erasure_codecs/codec.h>

#include <ytlib/chunk_client/file_reader.h>
#include <ytlib/chunk_client/file_writer.h>
#include <ytlib/chunk_client/erasure_writer.h>
#include <ytlib/chunk_client/erasure_reader.h>

#include <contrib/testing/framework.h>

#include <util/random/randcpp.h>
#include <util/system/fs.h>

////////////////////////////////////////////////////////////////////////////////

using NYT::TRef;
using NYT::TSharedRef;
using NYT::NErasure::ECodec;
using NYT::NErasure::GetCodec;
using namespace NYT::NChunkClient;

Stroka ToString(TSharedRef ref) {
    return NYT::ToString(TRef(ref));
}

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
                    EXPECT_EQ(
                        ToString(allBlocks[erasedIndices[i]]),
                        ToString(recoveredBlocks[i]));
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

    // Prepare data
    Stroka data[] = {
        "a",
        "b",
        "",
        "Hello world"};
    std::vector<Stroka> dataStrings(data, data + sizeof(data) / sizeof(Stroka));
    
    std::vector<TSharedRef> dataRefs;
    FOREACH (const auto& str, dataStrings) {
        dataRefs.push_back(TSharedRef::FromString(str));
    }

    // Write data
    auto erasureWriter = GetErasureWriter(codec, writers);
    FOREACH (const auto& ref, dataRefs) {
        erasureWriter->TryWriteBlock(ref);
    }
    erasureWriter->AsyncClose(meta).Get();

    FOREACH(auto writer, writers) {
        EXPECT_TRUE(writer->AsyncClose(meta).Get().IsOK());
    }

    // Manually check that data in files is correct
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
    }


    // Check reader
    std::vector<IAsyncReaderPtr> readers;
    for (int i = 0; i < codec->GetDataBlockCount(); ++i) {
        Stroka filename = "block" + ToString(i + 1);
        auto reader = NYT::New<TFileReader>(filename);
        reader->Open();
        readers.push_back(reader);
    }
    auto erasureReader = CreateErasureReader(readers);
    
    int index = 0;
    FOREACH (const auto& ref, dataRefs) {
        auto result = erasureReader->AsyncReadBlocks(std::vector<int>(1, index++)).Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = result.GetOrThrow().front();

        EXPECT_EQ(ToString(ref), ToString(resultRef));
    }
    
    {
        // Check some non-trivial read request
        std::vector<int> indices;
        indices.push_back(1);
        indices.push_back(3);
        auto result = erasureReader->AsyncReadBlocks(indices).Get();
        EXPECT_TRUE(result.IsOK());
        auto resultRef = result.GetOrThrow();
        EXPECT_EQ(ToString(dataRefs[1]), ToString(resultRef[0]));
        EXPECT_EQ(ToString(dataRefs[3]), ToString(resultRef[1]));
    }
    
    // CLeanup
    for (int i = 0; i < codec->GetTotalBlockCount(); ++i) {
        Stroka filename = "block" + ToString(i + 1);
        NFs::Remove(filename.c_str());
        NFs::Remove((filename + ".meta").c_str());
    }
}
