#include <library/cpp/testing/unittest/registar.h>

#include "codec.h"
#include "jerasure.h"
#include "lrc_jerasure.h"
#include "lrc_isa.h"
#include "public.h"
#include "reed_solomon.h"
#include "reed_solomon_isa.h"
#include "reed_solomon_jerasure.h"

#include <util/generic/hash.h>
#include <util/generic/serialized_enum.h>
#include <util/memory/blob.h>
#include <util/random/random.h>

#include <algorithm>
#include <optional>
#include <vector>

static constexpr size_t MaxBlockNumber = 20;
static constexpr size_t DefaultWordSize = 8;
static constexpr size_t DefaultAlignment = DefaultWordSize * sizeof(long);

std::vector<char> GenData(size_t bufferSize) {
    std::vector<char> data(bufferSize);
    for (size_t i = 0; i < bufferSize; ++i) {
        data[i] = RandomNumber(256u);
    }
    return data;
}

template <class TCodec, class TData>
void CheckCodecEncodeDecode(
    TCodec* codec,
    TData& data,
    size_t blockSize,
    std::optional<size_t> guaranteeRepair,
    bool partial = false,
    bool unaligned = false)
{

    size_t blocksCount = codec->GetTotalPartCount();

    std::vector<TBlob> dataBlocks;
    dataBlocks.reserve(codec->GetDataPartCount());

    // create data blocks with blocksize
    for (int i = 0; i < codec->GetDataPartCount(); ++i) {
        dataBlocks.push_back(TBlob::Copy(data.data() + i * blockSize, blockSize));
        UNIT_ASSERT_EQUAL(dataBlocks.back().Size(), blockSize);
    }

    std::vector<TBlob> parityBlocks = codec->Encode(dataBlocks);
    std::vector<TBlob> allBlocks(dataBlocks);
    std::copy(parityBlocks.begin(), parityBlocks.end(), std::back_inserter(allBlocks));

    // traverse all masks to be sure that everything is correct
    for (size_t mask = 0; mask < (1ull << blocksCount); ++mask) {
        NErasure::TPartIndexList erasedIndices;
        for (size_t i = 0; i < blocksCount; ++i) {
            if ((mask & (size_t(1) << i))) {
                erasedIndices.push_back(i);
            }
        }

        if (unaligned) {
            if (erasedIndices.size() > 2) {
                continue;
            }
            if (erasedIndices.size() == 2 && (erasedIndices.front() >= (codec->GetDataPartCount() / 2) || erasedIndices.back() < (codec->GetDataPartCount() / 2) || erasedIndices.back() >= codec->GetDataPartCount())) {
                continue;
            }
            if (erasedIndices.size() == 1 && erasedIndices.front() >= codec->GetDataPartCount() + codec->GetParityPartCount() / 2) {
                continue;
            }
        }

        std::optional<NErasure::TPartIndexList> repairIndices = codec->GetRepairIndices(erasedIndices);
        UNIT_ASSERT_EQUAL(static_cast<bool>(repairIndices), codec->CanRepair(erasedIndices));
        UNIT_ASSERT(erasedIndices.size() > static_cast<size_t>(codec->GetGuaranteedRepairablePartCount()) || static_cast<bool>(repairIndices));

        if (repairIndices) {
            std::vector<TBlob> aliveBlocks;
            std::vector<size_t> partialOffsets;
            std::vector<size_t> partialRecoverBytes;
            aliveBlocks.reserve(repairIndices->size());
            partialOffsets.reserve(repairIndices->size());
            partialRecoverBytes.reserve(repairIndices->size());

            size_t numToRecover = Max(size_t(1), RandomNumber(blockSize / DefaultAlignment));
            size_t offset = 0;

            if (allBlocks.front().Size() > numToRecover * DefaultAlignment) {
                offset = RandomNumber(allBlocks.front().Size() - numToRecover * DefaultAlignment);
                if (!unaligned) {
                    offset &= ~(DefaultAlignment - 1);
                }
            }

            for (int ind : *repairIndices) {
                if (!partial) {
                    aliveBlocks.push_back(allBlocks[ind]);
                } else {
                    UNIT_ASSERT_GE(allBlocks[ind].Size(), numToRecover * DefaultAlignment);
                    partialOffsets.push_back(offset);
                    partialRecoverBytes.push_back(numToRecover * DefaultAlignment);
                    aliveBlocks.push_back(allBlocks[ind].SubBlob(offset, offset + numToRecover * DefaultAlignment));
                }
            }
            std::vector<TBlob> recoveredBlocks = codec->Decode(aliveBlocks, erasedIndices);
            UNIT_ASSERT_EQUAL(recoveredBlocks.size(), erasedIndices.size());
            for (size_t i = 0; i < erasedIndices.size(); ++i) {
                if (!partial) {
                    UNIT_ASSERT_EQUAL(allBlocks[erasedIndices[i]].Size(), recoveredBlocks[i].Size());
                    UNIT_ASSERT(memcmp(allBlocks[erasedIndices[i]].Data(), recoveredBlocks[i].Data(), recoveredBlocks[i].Size()) == 0);
                } else {
                    UNIT_ASSERT_EQUAL(partialRecoverBytes[i], recoveredBlocks[i].Size());
                    UNIT_ASSERT(memcmp(allBlocks[erasedIndices[i]].AsCharPtr() + partialOffsets[i], recoveredBlocks[i].Data(), recoveredBlocks[i].Size()) == 0);
                }
            }
        } else if (guaranteeRepair) {
            UNIT_ASSERT_GT(erasedIndices.size(), guaranteeRepair);
        }
    }
}

template <int W>
void TestManyLrc(size_t blockMultiplier, bool many = true, bool partial = false, bool unaligned = false) {
    if constexpr (W == 8) {
        size_t bufferSize = MaxBlockNumber * W * sizeof(long) * blockMultiplier;
        size_t blockSize = W * sizeof(long) * blockMultiplier;
        std::vector<char> data = GenData(bufferSize);
        NErasure::TLrcIsa<12, 4, W, NErasure::TDefaultCodecTraits> lrc124W;
        CheckCodecEncodeDecode(&lrc124W, data, blockSize, 3, partial, unaligned);
        if (many) {
            NErasure::TLrcIsa<6, 4, W, NErasure::TDefaultCodecTraits> lrc64W;
            NErasure::TLrcIsa<8, 4, W, NErasure::TDefaultCodecTraits> lrc84W;
            NErasure::TLrcIsa<10, 4, W, NErasure::TDefaultCodecTraits> lrc104W;
            NErasure::TLrcIsa<14, 4, W, NErasure::TDefaultCodecTraits> lrc144W;
            NErasure::TLrcIsa<16, 4, W, NErasure::TDefaultCodecTraits> lrc164W;
            CheckCodecEncodeDecode(&lrc64W, data, blockSize, 3, partial, unaligned);
            CheckCodecEncodeDecode(&lrc84W, data, blockSize, 3, partial, unaligned);
            CheckCodecEncodeDecode(&lrc104W, data, blockSize, 3, partial, unaligned);
            CheckCodecEncodeDecode(&lrc144W, data, blockSize, 3, partial, unaligned);
            CheckCodecEncodeDecode(&lrc164W, data, blockSize, 3, partial, unaligned);
        }
    }
    size_t bufferSize = MaxBlockNumber * W * sizeof(long) * blockMultiplier;
    size_t blockSize = W * sizeof(long) * blockMultiplier;
    std::vector<char> data = GenData(bufferSize);
    NErasure::TLrcJerasure<12, 4, W, NErasure::TDefaultCodecTraits> lrc124W;
    CheckCodecEncodeDecode(&lrc124W, data, blockSize, 3, partial, unaligned);
    if (many) {
        NErasure::TLrcJerasure<6, 4, W, NErasure::TDefaultCodecTraits> lrc64W;
        NErasure::TLrcJerasure<8, 4, W, NErasure::TDefaultCodecTraits> lrc84W;
        NErasure::TLrcJerasure<10, 4, W, NErasure::TDefaultCodecTraits> lrc104W;
        NErasure::TLrcJerasure<14, 4, W, NErasure::TDefaultCodecTraits> lrc144W;
        NErasure::TLrcJerasure<16, 4, W, NErasure::TDefaultCodecTraits> lrc164W;
        CheckCodecEncodeDecode(&lrc64W, data, blockSize, 3, partial, unaligned);
        CheckCodecEncodeDecode(&lrc84W, data, blockSize, 3, partial, unaligned);
        CheckCodecEncodeDecode(&lrc104W, data, blockSize, 3, partial, unaligned);
        CheckCodecEncodeDecode(&lrc144W, data, blockSize, 3, partial, unaligned);
        CheckCodecEncodeDecode(&lrc164W, data, blockSize, 3, partial, unaligned);
    }
}

template <int W>
void TestManyCauchyReedSolomon(size_t blockMultiplier, bool many = true, bool partial = false, bool unaligned = false) {
    size_t bufferSize = MaxBlockNumber * W * sizeof(long) * blockMultiplier;
    size_t blockSize = W * sizeof(long) * blockMultiplier;
    std::vector<char> data = GenData(bufferSize);

    NErasure::TCauchyReedSolomonJerasure<3, 3, W, NErasure::TDefaultCodecTraits> crs33W;
    NErasure::TCauchyReedSolomonJerasure<6, 3, W, NErasure::TDefaultCodecTraits> crs63W;
    CheckCodecEncodeDecode(&crs33W, data, blockSize, 3, partial, unaligned);
    CheckCodecEncodeDecode(&crs63W, data, blockSize, 3, partial, unaligned);

    if (many) {
        NErasure::TCauchyReedSolomonJerasure<6, 2, W, NErasure::TDefaultCodecTraits> crs62W;
        NErasure::TCauchyReedSolomonJerasure<7, 2, W, NErasure::TDefaultCodecTraits> crs72W;
        NErasure::TCauchyReedSolomonJerasure<8, 4, W, NErasure::TDefaultCodecTraits> crs84W;
        NErasure::TCauchyReedSolomonJerasure<9, 4, W, NErasure::TDefaultCodecTraits> crs94W;
        NErasure::TCauchyReedSolomonJerasure<10, 5, W, NErasure::TDefaultCodecTraits> crs105W;
        NErasure::TCauchyReedSolomonJerasure<12, 1, W, NErasure::TDefaultCodecTraits> crs121W;
        CheckCodecEncodeDecode(&crs62W, data, blockSize, 2, partial, unaligned);
        CheckCodecEncodeDecode(&crs72W, data, blockSize, 2, partial, unaligned);
        CheckCodecEncodeDecode(&crs84W, data, blockSize, 4, partial, unaligned);
        CheckCodecEncodeDecode(&crs94W, data, blockSize, 4, partial, unaligned);
        CheckCodecEncodeDecode(&crs105W, data, blockSize, 5, partial, unaligned);
        CheckCodecEncodeDecode(&crs121W, data, blockSize, 1, partial, unaligned);
    }

    if constexpr (W == 8) {
        NErasure::TReedSolomonIsa<3, 3, W, NErasure::TDefaultCodecTraits> rs33;
        NErasure::TReedSolomonIsa<6, 3, W, NErasure::TDefaultCodecTraits> rs63;
        CheckCodecEncodeDecode(&rs33, data, blockSize, 3, partial, unaligned);
        CheckCodecEncodeDecode(&rs63, data, blockSize, 3, partial, unaligned);

        if (many) {
            NErasure::TReedSolomonIsa<6, 2, W, NErasure::TDefaultCodecTraits> rs62;
            NErasure::TReedSolomonIsa<7, 3, W, NErasure::TDefaultCodecTraits> rs73;
            NErasure::TReedSolomonIsa<8, 4, W, NErasure::TDefaultCodecTraits> rs84;
            CheckCodecEncodeDecode(&rs62, data, blockSize, 2, partial, unaligned);
            CheckCodecEncodeDecode(&rs73, data, blockSize, 3, partial, unaligned);
            CheckCodecEncodeDecode(&rs84, data, blockSize, 4, partial, unaligned);
        }
    }
}

Y_UNIT_TEST_SUITE(TestRandomText) {

    Y_UNIT_TEST(TestRandomBytesFullEncodeDecode) {
        TestManyCauchyReedSolomon<8>(1, /*many=*/false);
        TestManyLrc<8>(1, /*many=*/false);
        TestManyCauchyReedSolomon<8>(3, /*many=*/false);
        TestManyLrc<8>(3, /*many=*/false);
        TestManyCauchyReedSolomon<8>(4, /*many=*/false);
        TestManyLrc<8>(4, /*many=*/false);
        TestManyCauchyReedSolomon<8>(10, /*many=*/false);
        TestManyLrc<8>(10, /*many=*/false);
        TestManyCauchyReedSolomon<8>(42, /*many=*/false);
        TestManyLrc<8>(42, /*many=*/false);
        TestManyCauchyReedSolomon<8>(1000, /*many=*/false);
        TestManyLrc<9>(1000, /*many=*/false);
    }

    Y_UNIT_TEST(TestRandomBytesPartialDecode) {
        TestManyCauchyReedSolomon<8>(100, /*many=*/false, /*partial=*/true);
        TestManyLrc<8>(100, /*many=*/false, /*partial=*/true);
        TestManyCauchyReedSolomon<8>(200, /*many=*/false, /*partial=*/true);
        TestManyLrc<8>(200, /*many=*/false, /*partial=*/true);
        TestManyCauchyReedSolomon<8>(1000, /*many=*/false, /*partial=*/true);
        TestManyLrc<8>(1000, /*many=*/false, /*partial=*/true);
    }

    Y_UNIT_TEST(TestRandomBytesUnalignedPartialDecode) {
        TestManyLrc<8>(100, /*many=*/false, /*partial=*/true, /*unaligned=*/true);
        TestManyLrc<8>(200, /*many=*/false, /*partial=*/true, /*unaligned=*/true);
        TestManyLrc<8>(1000, /*many=*/false, /*partial=*/true, /*unaligned=*/true);
    }

    Y_UNIT_TEST(TestManyReedSolomon) {
        for (size_t blockMultiplier = 1; blockMultiplier < 3; ++blockMultiplier) {
            TestManyCauchyReedSolomon<8>(blockMultiplier);
            TestManyCauchyReedSolomon<16>(blockMultiplier);
            // TestManyCauchyReedSolomon<32>(); // too long to invert matrix
        }
    }

    Y_UNIT_TEST(TestManyLrc) {
        for (size_t blockMultiplier = 1; blockMultiplier < 3; ++blockMultiplier) {
            TestManyLrc<8>(blockMultiplier);
            TestManyLrc<16>(blockMultiplier); // only for jerasure
            // TestManyLrc<32>(); // too long to invert matrix
        }
    }
}
