#include <util/digest/murmur.h>

#include <library/cpp/blockcodecs/core/stream.h>
#include <library/cpp/blockcodecs/core/codecs.h>

using namespace NBlockCodecs;

int main(int argc, char* argv[])
{
    try {
        if (argc == 2 && TString{argv[1]} == "list") {
            for (auto codecName : ListAllCodecs()) {
                union {
                    ui16 Parts[2];
                    ui32 Data;
                } x;

                x.Data = MurmurHash<ui32>(codecName.data(), codecName.size());
                Cout << codecName << "  " << (x.Parts[1] ^ x.Parts[0]) << Endl;
            }

            return 0;
        }

        if (argc != 3) {
            return 1;
        }

        auto codec = Codec(argv[2]);
        if (TString{argv[1]} == "encode") {
            TCodedOutput output(&Cout, codec, 1024);
            Cin.ReadAll(output);
            output.Finish();
        } else if (TString{argv[1]} == "decode") {
            TDecodedInput input(&Cin);
            input.ReadAll(Cout);
        } else {
            return 1;
        }
    } catch (const std::exception& ex) {
        Cerr << ex.what() << Endl;
        return 1;
    }

    return 0;
}
