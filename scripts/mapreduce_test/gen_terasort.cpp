#include <sstream>
#include <string>
#include <iostream>
#include <vector>
#include <cstdlib>
    
std::vector<char> symbols;

std::string generate(int len) {
    std::string str(len, ' ');
    for (int i = 0; i < len; ++i) {
        str[i] = symbols[rand() % symbols.size()];
    }
    return str;
}

int main(int argc, char** argv) {
    std::ios_base::sync_with_stdio(0);
    if (argc != 2) {
        std::cerr << "You should pas number of records to generate" << std::endl;
        return 1;
    }
    
    int len;
    {
        std::istringstream is(argv[1]);
        is >> len;
    }
    
    for (char i = 0; i < 26; ++i) {
        symbols.push_back('a' + i);
        symbols.push_back('A' + i);
    }
    for (char i = 0; i < 10; ++i) {
        symbols.push_back('0' + i);
    }
    symbols.push_back('_');

    int seed;
    std::cin >> seed;
    while (std::cin) {
        std::cin.ignore(1000);
    }
    srand(seed);

    for (int i = 0; i < len; ++i) {
        std::cout << generate(10) << "\t\t" << generate(90) << "\n";
    }

    return 0;
}
