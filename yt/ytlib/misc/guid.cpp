#include "stdafx.h"
#include "guid.h"

#include <ytlib/misc/error.h>

#include <util/datetime/cputimer.h>

#include <util/system/atomic.h>
#include <util/system/hostname.h>

#include <time.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJenkinsHashFunc2
{
    ui32 Seed1, Seed2;
public:
    TJenkinsHashFunc2() :Seed1(13), Seed2(7) {}
    void SetSeed(ui32 val1, ui32 val2) { Seed1 = val1; Seed2 = val2; }
    ui32 GetSeed1() const { return Seed1; }
    ui32 GetSeed2() const { return Seed2; }
    void CalcHash(const void *data, int dataLen, ui32 *rc, ui32 *rb);
};

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bits set, and the deltas of all three
high bits or all three low bits, whether the original value of a,b,c
is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a
structure that could supported 2x parallelism, like so:
a -= b;
a -= c; x = (c>>13);
b -= c; a ^= x;
b -= a; x = (a<<8);
c -= a; b ^= x;
c -= b; x = (b>>13);
...
Unfortunately, superscalar Pentiums and Sparcs can't take advantage
of that parallelism.  They've also turned some of those single-cycle
latency instructions into multi-cycle latency instructions.  Still,
this is the fastest good hash I could find.  There were about 2^^68
to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/
#define mix(a,b,c) \
{ \
    a -= b; a -= c; a ^= (c>>13); \
    b -= c; b -= a; b ^= (a<<8); \
    c -= a; c -= b; c ^= (b>>13); \
    a -= b; a -= c; a ^= (c>>12);  \
    b -= c; b -= a; b ^= (a<<16); \
    c -= a; c -= b; c ^= (b>>5); \
    a -= b; a -= c; a ^= (c>>3);  \
    b -= c; b -= a; b ^= (a<<10); \
    c -= a; c -= b; c ^= (b>>15); \
}

/*//#define final(a,b,c) \
//{ \
//    c ^= b; c -= rot(b,14); \
//    a ^= c; a -= rot(c,11); \
//    b ^= a; b -= rot(a,25); \
//    c ^= b; c -= rot(b,16); \
//    a ^= c; a -= rot(c,4);  \
//    b ^= a; b -= rot(a,14); \
//    c ^= b; c -= rot(b,24); \
//}*/

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
k       : the key (the unaligned variable-length array of bytes)
len     : the length of the key, counting by bytes
initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 6*len+35 instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (cmph_uint8 **)k, do it like this:
for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

void TJenkinsHashFunc2::CalcHash(const void *data, int dataLen, ui32 *rc, ui32 *rb)
{
    const char *k = static_cast<const char*>(data);
    ui32 keylen = (ui32)dataLen;

    ui32 a, b, c;
    ui32 len, length;

    /* Set up the internal state */
    length = keylen;
    len = length;
    a = b = c = 0xdeadbeef + ((ui32)(length<<2)) + Seed1;
    c += Seed2;

    /*-----handle most of the key */
    while (len >= 12) {
        a += ((ui32*)k)[0];
        b += ((ui32*)k)[1];
        c += ((ui32*)k)[2];
        mix(a,b,c);
        k += 12; len -= 12;
    }

    /*-----handle the last 3 uint32_t's */
    switch(len) {              /* all the case statements fall through */
    case 11:
        c +=((ui32)k[10]<<24);
    case 10:
        c +=((ui32)k[9]<<16);
    case 9 :
        c +=((ui32)k[8]<<8);
        /* the first byte of c is reserved for the length */
    case 8 :
        b += ((ui32*)k)[1];
        a += ((ui32*)k)[0];
        break;
    case 7 :
        b +=((ui32)k[6]<<16);
    case 6 :
        b +=((ui32)k[5]<<8);
    case 5 :
        b +=k[4];
    case 4 :
        a += ((ui32*)k)[0];
        break;
    case 3 :
        a +=((ui32)k[2]<<16);
    case 2 :
        a +=((ui32)k[1]<<8);
    case 1 :
        a +=k[0];
        /* case 0: nothing left to add */
    }
    mix(a,b,c);
    //final(a,b,c);

    *rc=c; *rb=b;
}

////////////////////////////////////////////////////////////////////////////////

static TAtomic Counter;

struct TGuidSeed
{
    ui64 StartCycleCount;
    char HostName[32];
    ui32 Hz;
    ui64 StartMicroseconds;
};

static TGuidSeed GuidSeed;

static void InitGuidSeed()
{
    GuidSeed.StartCycleCount = GetCycleCount();

    const char *hostName = GetHostName();
    strncpy(GuidSeed.HostName, hostName, sizeof(GuidSeed.HostName));
    GuidSeed.Hz = 0xc186a511;
    GuidSeed.StartMicroseconds = MicroSeconds();
}

TGuid::TGuid()
{
    memset(&Parts, 0, sizeof (Parts));
}

TGuid::TGuid(ui32 part0, ui32 part1, ui32 part2, ui32 part3)
{
    Parts[0] = part0;
    Parts[1] = part1;
    Parts[2] = part2;
    Parts[3] = part3;
}

TGuid::TGuid(ui64 part0, ui64 part1)
{
    Parts[0] = static_cast<ui32>(part0);
    Parts[1] = static_cast<ui32>(part0 >> 32);
    Parts[2] = static_cast<ui32>(part1);
    Parts[3] = static_cast<ui32>(part1 >> 32);
}

TGuid::TGuid(const TGuid &guid)
{
    memcpy(Parts, guid.Parts, sizeof(Parts));
}

bool TGuid::IsEmpty() const
{
    return (Parts[0] | Parts[1] | Parts[2] | Parts[3]) == 0;
}

TGuid TGuid::Create()
{
    TGuid res;
    static bool guidSeedIsInit;
    if (!guidSeedIsInit) {
        InitGuidSeed();
        guidSeedIsInit = true;
    }
    Zero(res);

    long counter = AtomicAdd(Counter, 1);

    ui64 fin = GetCycleCount();
    const int N_ADD_BYTES = 12;
    char info[sizeof(GuidSeed) + N_ADD_BYTES];
    memcpy(info + N_ADD_BYTES, &GuidSeed, sizeof(GuidSeed));
    *((ui64*)info) = fin;
    *((ui32*)(info + 8)) = counter;

    TJenkinsHashFunc2 hf;
    hf.SetSeed(0x853122ef, 0x1c39dbb5);
    hf.CalcHash(info, sizeof(info), &res.Parts[0], &res.Parts[1]);
    res.Parts[2] = MurmurHash<ui32>(info, sizeof(info));
    res.Parts[3] = counter;
    return res;
}

Stroka TGuid::ToString() const
{
    char buf[1000];
    sprintf(buf, "%x-%x-%x-%x", Parts[3], Parts[2], Parts[1], Parts[0]);
    return buf;
}

TGuid TGuid::FromString(const TStringBuf& str)
{
    TGuid guid;
    if (!FromString(str, &guid)) { 
        THROW_ERROR_EXCEPTION("Error parsing GUID: %s", ~Stroka(str).Quote());
    }
    return guid;
}

bool TGuid::FromString(const TStringBuf &str, TGuid* guid)
{
    if (sscanf(
        str.data(),
        "%x-%x-%x-%x",
        &guid->Parts[3],
        &guid->Parts[2],
        &guid->Parts[1],
        &guid->Parts[0]) != 4)
    {
        return false;
    }
    return true;
}

TGuid TGuid::FromProto(const NProto::TGuid &protoGuid)
{
    return TGuid(protoGuid.first(), protoGuid.second());
}

NProto::TGuid TGuid::ToProto() const
{
    ui64 first = (static_cast<ui64>(Parts[1]) << 32) + Parts[0];
    ui64 second = (static_cast<ui64>(Parts[3]) << 32) + Parts[2];
    NProto::TGuid protoGuid;
    protoGuid.set_first(first);
    protoGuid.set_second(second);
    return protoGuid;
}

bool operator == (const TGuid& lhs, const TGuid& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (lhs)) == 0;
}

bool operator != (const TGuid& lhs, const TGuid& rhs)
{
    return !(lhs == rhs);
}

bool operator < (const TGuid& lhs, const TGuid& rhs)
{
    return memcmp(&lhs, &rhs, sizeof (lhs)) < 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
