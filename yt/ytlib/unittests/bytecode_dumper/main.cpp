//
// ya make cannot dump bytecode to dedicated files
// So we use this program that stores all bytecode that we require in tests
// and is able to dump it by request.
//

#include <library/resource/resource.h>

#include <util/stream/output.h>

int main(int argc, const char** argv) {
    if (argc != 2) {
        Cerr << "USAGE: " << argv[0] << " UDF_NAME" << Endl;
        return 1;
    }

    TString data;
    try {
        data = NResource::Find(argv[1]);
        Cout.Write(data);
        Cout.Flush();
        return 0;
    } catch (const std::exception& ex) {
        Cerr << "Cannot requested resource: " << ex.what() << Endl;
        Cerr << "List of all keys:" << Endl;
        for (const auto& key : NResource::ListAllKeys()) {
            Cerr << "  " << key << Endl;
        }
        return 1;
    }
}
