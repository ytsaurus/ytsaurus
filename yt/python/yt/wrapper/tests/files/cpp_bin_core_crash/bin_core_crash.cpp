#include <iostream>

int t;

int main() {
    for (t = 0; t < 5; ++t) {
        if (t == 1) {
            abort();
        }
        std::cerr << (t + 1) / (t - 1) << "\n";
    }

    return 13;
}
