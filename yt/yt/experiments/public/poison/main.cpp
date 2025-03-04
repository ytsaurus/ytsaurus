#include <library/cpp/yt/memory/poison.h>

#include <array>

using namespace NYT;

int main()
{
    std::array<char, 100> buf;
    auto ref = TMutableRef(buf.data(), buf.size());
    buf[0] = 'x';
    PoisonMemory(ref);
    Cout << buf[0] << Endl;
    UnpoisonMemory(ref);
    buf[0] = 'y';
    Cout << buf[0] << Endl;
}
