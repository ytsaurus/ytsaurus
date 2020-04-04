#include <iostream>
#include <string>

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " {system name}" << std::endl;
    }

    const int keyCount = 4;
    std::string keys[keyCount] = {"time", "log_level", "subsystem", "value"};

    std::ios_base::sync_with_stdio(false);

    while (!std::cin.eof()) {
        std::cout << "system=" << argv[1] << "\t";
        for (int i = 0; i < keyCount; ++i) {
            char delim = (i + 1 < keyCount) ? '\t' : '\n';
            std::string value;
            getline(std::cin, value, delim);
            std::cout << keys[i] << "=" << value << delim;
        }
    }

    return 0;
}
