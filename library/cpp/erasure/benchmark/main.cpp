#include <library/cpp/erasure/public.h>
#include <library/cpp/erasure/helpers.h>
#include <library/cpp/erasure/lrc_jerasure.h>
#include <library/cpp/erasure/lrc_isa.h>
#include <library/cpp/erasure/reed_solomon.h>
#include <library/cpp/erasure/reed_solomon_isa.h>
#include <library/cpp/erasure/reed_solomon_jerasure.h>
#include <library/cpp/erasure/codec.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/random/random.h>
#include <util/system/hp_timer.h>
#include <util/random/shuffle.h>
#include <util/memory/blob.h>

std::vector<int> RandomSubset(int n, int k) {
    std::vector<int> numbers;
    for (int i = 0; i < n; ++i) {
        numbers.push_back(i);
    }
    Shuffle(numbers.begin(), numbers.end());
    numbers.resize(k);
    return numbers;
}

struct TOptions {
    TString Input;
    bool CheckCodec = false;
};

static constexpr size_t DefaultChunkSize = 16384;
static constexpr size_t ItersNumForSmallChunks = 3 * 1000;
static constexpr size_t ItersNumForFullDecode = 10;
static constexpr size_t ItersNumForEncode = 10;
static constexpr size_t DataPartSize = 1 << 16;

void CheckOutput(const std::vector<int>& erasureIndices, const std::vector<TBlob>& recovery, const std::vector<TBuffer>& erasedData) {
    for (size_t j = 0; j < erasureIndices.size(); ++j) {
        TBlob dataCur = recovery[j];
        Y_ENSURE(dataCur.Size() == erasedData[j].Size());
        if (memcmp(dataCur.AsCharPtr(), erasedData[j].Data(), dataCur.Size()) != 0) {
            for (size_t i = 0; i < erasedData[j].size(); ++i) {
                if (dataCur[i] != erasedData[j].data()[i]) {
                    Cerr << "Error in byte number " << i << ' ' << (int)dataCur[i] << ' ' << (int)erasedData[j].data()[i] << Endl;
                    Y_FAIL();
                }
            }
        }
    }
}

void ParseOptions(int argc, const char* argv[], TOptions* options) {
    NLastGetopt::TOpts opts;
    opts.AddLongOption("input", "Input file").OptionalArgument("<path>").StoreResult(&options->Input);
    opts.AddLongOption("check-output", "Check producible output").OptionalArgument().NoArgument().SetFlag(&options->CheckCodec);
    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);
}

template <int N, int K, int W, class Codec>
void TestCodec(TStringBuf name, const TOptions& options) {
    TBlob data;
    TBuffer generatedData;

    if (!options.Input.empty()) {
        data = TBlob::LockedFromFile(options.Input);
        Cerr << "File";
    } else {
        generatedData = NErasure::TDefaultCodecTraits::AllocateBuffer(DataPartSize * N);
        for (size_t i = 0; i < generatedData.size(); ++i) {
            generatedData.data()[i] = RandomNumber(256u);
        }
        data = TBlob::FromBuffer(generatedData);
        Cerr << "Generated data";
    }

    Cerr << " size is " << data.Size() << " bytes" << Endl;
    // subblobs
    Y_ENSURE(data.Size() % N == 0, "File size must be the divisor of N");
    size_t factor = data.Size() / N;
    Y_ENSURE(factor % (sizeof(long) * W) == 0, "data.Size() / N must be divisible by 64 for window reason");

    Cerr << name << Endl << Endl;

    Codec codec;

    std::vector<TBlob> chunks;
    chunks.reserve(codec.GetDataPartCount());
    for (int i = 0; i < codec.GetDataPartCount(); ++i) {
        chunks.push_back(TBlob::Copy(data.AsCharPtr() + i * factor, factor));
    }

    std::vector<TBlob> parity;
    NHPTimer::STime start;
    NHPTimer::GetTime(&start);
    for (size_t encodeCount = 0; encodeCount < ItersNumForEncode; ++encodeCount) {
        parity.clear();
        parity = codec.Encode(chunks);
        Y_ENSURE(parity.size() == static_cast<size_t>(K));
    }

    Cerr << "Encoding speed = " << static_cast<double>(data.Size()) * ItersNumForEncode / (NHPTimer::GetTimePassed(&start) * 1024.0 * 1024.0) << " Mb/s" << Endl << Endl;

    TStringStream smallStats;
    TStringStream bigStats;

    NHPTimer::GetTime(&start);

    for (int failuresCount = 1; failuresCount <= codec.GetGuaranteedRepairablePartCount(); ++failuresCount) {
        double res = 0.0;
        for (size_t iter = 0; iter < ItersNumForSmallChunks; ++iter) {
            std::vector<int> erasureIndices = NErasure::UniqueSortedIndices(RandomSubset(N + K, failuresCount));
            std::vector<TBuffer> erasedData;

            ui64 seek = RandomNumber(factor - DefaultChunkSize) & ~(63ull);
            std::vector<TBuffer> dataBlocks(N, NErasure::TDefaultCodecTraits::AllocateBuffer(DefaultChunkSize));
            std::vector<TBuffer> parityBlocks(K, NErasure::TDefaultCodecTraits::AllocateBuffer(DefaultChunkSize));
            for (size_t i = 0; i < dataBlocks.size(); ++i) {
                memcpy(dataBlocks[i].Data(), chunks[i].AsCharPtr() + seek, DefaultChunkSize);
            }
            for (size_t i = 0; i < static_cast<size_t>(K); ++i) {
                memcpy(parityBlocks[i].Data(), parity[i].AsCharPtr() + seek, DefaultChunkSize);
            }

            for (size_t j = 0; j < erasureIndices.size(); ++j) {
                TBuffer* dataCur;
                if (erasureIndices[j] < static_cast<int>(N)) {
                    dataCur = &dataBlocks[erasureIndices[j]];
                } else {
                    dataCur = &parityBlocks[erasureIndices[j] - N];
                }
                erasedData.push_back(*dataCur);
                memset(dataCur->Data(), 0, dataCur->Size());
            }
            auto repairIndices = codec.GetRepairIndices(erasureIndices);
            std::vector<TBlob> aliveBlocks;
            for (int ind : *repairIndices) {
                if (ind < static_cast<int>(N)) {
                    aliveBlocks.push_back(TBlob::FromBuffer(dataBlocks[ind]));
                } else {
                    aliveBlocks.push_back(TBlob::FromBuffer(parityBlocks[ind - N]));
                }
            }

            NHPTimer::GetTime(&start);
            auto recovery = codec.Decode(aliveBlocks, erasureIndices);
            res += NHPTimer::GetTimePassed(&start);

            if (options.CheckCodec) {
                CheckOutput(erasureIndices, recovery, erasedData);
            }
        }

        smallStats << "Decoding 16Kb chunks speed with " << failuresCount << " failure(s) = " << ItersNumForSmallChunks / res << " per second\n";
        res = 0.0;

        for (size_t iter = 0; iter < ItersNumForFullDecode; ++iter) {
            std::vector<int> erasureIndices = NErasure::UniqueSortedIndices(RandomSubset(N + K, failuresCount));
            std::vector<TBuffer> erasedData;

            std::vector<TBuffer> dataBlocks(N, NErasure::TDefaultCodecTraits::AllocateBuffer(factor));
            std::vector<TBuffer> parityBlocks(K, NErasure::TDefaultCodecTraits::AllocateBuffer(factor));
            for (size_t i = 0; i < dataBlocks.size(); ++i) {
                memcpy(dataBlocks[i].Data(), chunks[i].AsCharPtr(), factor);
            }
            for (size_t i = 0; i < static_cast<size_t>(K); ++i) {
                memcpy(parityBlocks[i].Data(), parity[i].AsCharPtr(), factor);
            }

            for (size_t j = 0; j < erasureIndices.size(); ++j) {
                TBuffer* dataCur;
                if (erasureIndices[j] < static_cast<int>(N)) {
                    dataCur = &dataBlocks[erasureIndices[j]];
                } else {
                    dataCur = &parityBlocks[erasureIndices[j] - N];
                }
                erasedData.push_back(*dataCur);
                memset(dataCur->Data(), 0, dataCur->Size());
            }
            auto repairIndices = codec.GetRepairIndices(erasureIndices);
            std::vector<TBlob> aliveBlocks;
            for (int ind : *repairIndices) {
                if (ind < static_cast<int>(N)) {
                    aliveBlocks.push_back(TBlob::FromBuffer(dataBlocks[ind]));
                } else {
                    aliveBlocks.push_back(TBlob::FromBuffer(parityBlocks[ind - N]));
                }
            }

            NHPTimer::GetTime(&start);
            auto recovery = codec.Decode(aliveBlocks, erasureIndices);
            res += NHPTimer::GetTimePassed(&start);

            if (options.CheckCodec) {
                CheckOutput(erasureIndices, recovery, erasedData);
            }
        }
        bigStats << "Full chunk decode speed with " << failuresCount << " failure(s) = " << static_cast<double>(data.Size() * ItersNumForFullDecode) / (res * 1024.0 * 1024.0) << " Mb/s" << Endl;
    }
    Cout << smallStats.Str() << Endl;
    Cout << bigStats.Str() << Endl;
}

int main(int argc, const char* argv[]) {
    TOptions options;
    ParseOptions(argc, argv, &options);

    TestCodec<3, 3, 8, NErasure::TCauchyReedSolomonJerasure<3, 3, 8, NErasure::TDefaultCodecTraits>>("Cauchy Reed Solomon 3-3 codec", options);
    TestCodec<6, 3, 8, NErasure::TCauchyReedSolomonJerasure<6, 3, 8, NErasure::TDefaultCodecTraits>>("Cauchy Reed Solomon 6-3 codec", options);
    TestCodec<9, 3, 8, NErasure::TCauchyReedSolomonJerasure<9, 3, 8, NErasure::TDefaultCodecTraits>>("Cauchy Reed Solomon 9-3 codec", options);

    TestCodec<3, 3, 8, NErasure::TReedSolomonIsa<3, 3, 8, NErasure::TDefaultCodecTraits>>("Reed Solomon ISA 3-3 codec", options);
    TestCodec<6, 3, 8, NErasure::TReedSolomonIsa<6, 3, 8, NErasure::TDefaultCodecTraits>>("Reed Solomon ISA 6-3 codec", options);
    TestCodec<9, 3, 8, NErasure::TReedSolomonIsa<9, 3, 8, NErasure::TDefaultCodecTraits>>("Reed Solomon ISA 9-3 codec", options);

    TestCodec<6, 4, 8, NErasure::TLrcJerasure<6, 4, 8, NErasure::TDefaultCodecTraits>>("LRC 6-2-2", options);
    TestCodec<10, 4, 8, NErasure::TLrcJerasure<10, 4, 8, NErasure::TDefaultCodecTraits>>("LRC 10-2-2", options);
    TestCodec<12, 4, 8, NErasure::TLrcJerasure<12, 4, 8, NErasure::TDefaultCodecTraits>>("LRC 12-2-2", options);
    TestCodec<14, 4, 8, NErasure::TLrcJerasure<14, 4, 8, NErasure::TDefaultCodecTraits>>("LRC 14-2-2", options);

    TestCodec<12, 4, 8, NErasure::TLrcIsa<12, 4, 8, NErasure::TDefaultCodecTraits>>("LrcIsa 12-2-2", options);

    return 0;
}
