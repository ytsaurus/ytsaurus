#include <yt/core/compression/codec.h>

#include <util/string/cast.h>
#include <util/string/type.h>

#include <iostream>
#include <string>
#include <vector>

using namespace NYT;

std::vector<TSharedRef> Read(std::istream& in, const size_t blockSize = 16752344)
{
    std::vector<TSharedRef> refs;
    while (!in.eof()) {
        auto buffer = TBlob(TDefaultBlobTag(), blockSize, false);
        in.read(buffer.Begin(), buffer.Size());
        buffer.Resize(in.gcount());
        refs.push_back(TSharedRef::FromBlob(std::move(buffer)));
    }
    return refs;
}

int main(int argc, char** argv)
{
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " {compress|decompress} <codec>" << std::endl;
        return 1;
    }

    const Stroka action = argv[1];
    const Stroka codecName = argv[2];

    NCompression::ECodec codecId;
    if (IsNumber(codecName)) {
        codecId = NCompression::ECodec(FromString<int>(codecName));
    } else {
        codecId = ParseEnum<NCompression::ECodec>(codecName);
    }

    std::cerr << ToString(codecId) << std::endl;
    auto codec = NCompression::GetCodec(codecId);

    std::vector<TSharedRef> input = Read(std::cin);
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
