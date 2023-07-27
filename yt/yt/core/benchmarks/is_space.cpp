#include <benchmark/benchmark.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

const char Lyrics[] =
    "So, so you think you can tell\n"
    "\tHeaven from Hell,\n"
    "\tblue skies from pain.\n"
    "Can you tell a green field from a cold steel rail?\n"
    "\tA smile from a veil?\n"
    "\tDo you think you can tell?\n"
    "And did they get you to trade your heroes for ghosts?\n"
    "\tHot ashes for trees?\n"
    "\tHot air for a cool breeze?\n"
    "\tCold comfort for change?\n"
    "And did you exchange a walk on part in the war for a lead role in a cage?\n"
    "How I wish, how I wish you were here.\n"
    "We're just two lost souls swimming in a fish bowl, year after year,\n"
    "Running over the same old ground.\n"
    "What have we found?\n"
    "The same old fears.\n"
    "Wish you were here.\n";

int BooleanTableLookup(char c)
{
    const unsigned char table[] = {
        0,0,0,0,0,0,0,0, 0,1,1,1,1,1,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        1,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,

        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,

        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,

        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0
    };

    return table[static_cast<unsigned char>(c)];
}

int BitTableLookup(char c)
{
    const unsigned char table[] = {
        0, 124,   0,   0, 128,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0,
        0,   0,   0,   0,   0,   0,   0,   0
    };

    unsigned char mask[] = { 1, 2, 4, 8, 16, 32, 64, 128 };
    const  unsigned char byte   = static_cast<unsigned char>(c);

    return table[byte >> 3] & mask[byte & 7];
}

int RangeCheck1(char c)
{
    return c == ' ' || (c >= 9 && c <= 13);
}

int RangeCheck2(char c)
{
    unsigned char byte = static_cast<unsigned char>(c - 9);
    return (byte == (' ' - 9)) || (byte <= 13 - 9);
}

#define BM_IMPL(f) \
do { \
    while (state.KeepRunning()) { \
        for (size_t i = 0; i < sizeof(Lyrics); ++i) { \
            int x = f(Lyrics[i]); \
            ((void)x); \
        } \
    } \
    state.SetItemsProcessed((size_t)state.iterations() * (size_t)sizeof(Lyrics)); \
} while(0)

void BM_IsSpace(benchmark::State& state)
{
    BM_IMPL(std::isspace);
}

BENCHMARK(BM_IsSpace);

#define BM_CC(a, b) a ## b
#define BM_XX(f) \
void BM_IsSpace_##f(benchmark::State& state) { BM_IMPL(f); } \
BENCHMARK(BM_IsSpace_##f);

BM_XX(BooleanTableLookup)
BM_XX(BitTableLookup)
BM_XX(RangeCheck1)
BM_XX(RangeCheck2)

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
