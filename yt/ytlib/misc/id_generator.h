#pragma once

#include "common.h"

class TInputStream;
class TOutputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
class TIdGenerator;

void Save(TOutputStream* output, const TIdGenerator& generator);
void Load(TInputStream* input, TIdGenerator& generator);

//! A thread-safe generator producing increasing sequence of numbers.
class TIdGenerator
{
public:
    TIdGenerator();

    ui64 Next();
    void Reset();

private:
    TAtomic Current;

    friend void Save(TOutputStream* output, const TIdGenerator& generator);
    friend void Load(TInputStream* input, TIdGenerator& generator);

};

void Save(TOutputStream* output, const TIdGenerator& generator);
void Load(TInputStream* input, TIdGenerator& generator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
