#include <ytlib/misc/foreach.h>
#include <ytlib/erasure_codecs/codec.h>

#include <contrib/testing/framework.h>

#include <util/random/randcpp.h>

using NYT::TSharedRef;
using NYT::NErasure::ECodec;
using NYT::NErasure::GetCodec;

class TErasureCodingTest
    : public ::testing::Test
{
};

Stroka ToStroka(const TSharedRef& ref)
{
    return Stroka(ref.Begin(), ref.End());
}


TEST_F(TErasureCodingTest, SmallTest)
{
    TRand rand;

    std::map<ECodec::EDomain, int> guaranteedRecoveryCount;
    guaranteedRecoveryCount[ECodec::ReedSolomon3] = 3;


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
                        ToStroka(allBlocks[erasedIndices[i]]),
                        ToStroka(recoveredBlocks[i]));
                }
            }
        }

    }
}

