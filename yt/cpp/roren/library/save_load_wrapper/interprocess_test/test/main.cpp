#include "virtual.h"

#include <memory>
#include <util/stream/file.h>

void Write(const char* filename) {
    TVirtualPtr v1 = std::make_shared<TImpl1>(100);
    TVirtualPtr v2 = std::make_shared<TImpl2>(200);
    TVirtualPtr v3 = std::make_shared<TImpl3>(300);

    TFileOutput output(filename);
    ::SaveMany(&output, v1, v2, v3);
}

void Read(const char* filename) {
    TFileInput input(filename);
    for (int i = 0; i < 3; ++i) {
        TVirtualPtr v;
        ::Load(&input, v);
        Cout << v->GetData() << Endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        abort();
    }
    if (strcmp(argv[1], "read") == 0) {
        Read(argv[2]);
    } else if (strcmp(argv[1], "write") == 0) {
        Write(argv[2]);
    } else {
        abort();
    }
    return 0;
}
