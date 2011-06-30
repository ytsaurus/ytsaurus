#pragma once

#include "io.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// user interface for any type of calculation

struct IProcessor
{
public:

    virtual void Start(size_t taskIdx, TRead& input, TWrite& output)
    {
        (void)taskIdx;
        (void)input;
        (void)output;
    }

    virtual void Finish(size_t taskIdx, TRead& input, TWrite& output)
    {
        (void)taskIdx;
        (void)input;
        (void)output;
    }

    virtual void Process(TRead& input, TWrite& output) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}
