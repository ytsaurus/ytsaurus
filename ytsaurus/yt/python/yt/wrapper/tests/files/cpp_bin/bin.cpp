#include <iostream>

int main() {
    while (!(std::cin >> std::ws).eof()) {
        std::string s;
        std::getline(std::cin, s);
    }

    for (int t = 0; t < 5; ++t) {
        std::cout << "key" << t << "\tsubkey\tvalue=value\n";
    }

    return 0;
}
