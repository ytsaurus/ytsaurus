#include <yt/core/compression/codec.h>
#include <yt/core/misc/optional.h>

#include <util/string/cast.h>
#include <util/string/type.h>

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

using namespace NYT;

std::vector<TSharedRef> Read(std::istream& in, std::optional<std::vector<size_t>> sizes, const size_t blockSize = 16752344)
{
    std::vector<TSharedRef> refs;
    size_t index = 0;
    while (!in.eof()) {
        size_t size = blockSize;
        if (sizes) {
            if (index == sizes->size()) {
                break;
            }
            size = (*sizes)[index++];
        }
        auto buffer = TBlob(TDefaultBlobTag(), size, false);
        in.read(buffer.Begin(), buffer.Size());
        buffer.Resize(in.gcount());
        refs.push_back(TSharedRef::FromBlob(std::move(buffer)));
    }
    return refs;
}

int main(int argc, char** argv)
{
    if (argc != 3 && argc != 4) {
        std::cerr << "Usage: " << argv[0] << " {compress|decompress} <codec> [<file_with_sizes>]" << std::endl;
        return 1;
    }

    const TString action = argv[1];
    const TString codecName = argv[2];

    std::optional<std::vector<size_t>> sizes;
    if (argc == 4) {
        sizes = std::vector<size_t>();
        const TString sizesFilename = argv[3];
        std::ifstream fin(sizesFilename);
        while (!(fin >> std::ws).eof()) {
            size_t size;
            fin >> size;
            sizes->push_back(size);
        }
    }

    NCompression::ECodec codecId;
    if (IsNumber(codecName)) {
        codecId = NCompression::ECodec(FromString<int>(codecName));
    } else {
        codecId = ParseEnum<NCompression::ECodec>(codecName);
    }

    std::cerr << ToString(codecId) << std::endl;
    auto codec = NCompression::GetCodec(codecId);

    std::vector<TSharedRef> input = Read(std::cin, sizes);
    TSharedRef output;

    if (action == "compress") {
        output = codec->Compress(input);
    } else if (action == "decompress") {
        output = codec->Decompress(input);
    } else {
        std::cerr << "Incorrect action " << action << std::endl;
        return 1;
    }

    std::cout.write(output.Begin(), output.Size());

    return 0;
}
