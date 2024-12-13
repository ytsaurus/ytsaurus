#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A thread-safe generator producing increasing sequence of numbers.
class TIdGenerator
{
public:
    //! For persistence only.
    TIdGenerator& operator=(const TIdGenerator& other);

    //! Return current value.
    ui64 Get() const;
    //! Return current value and advance generator.
    ui64 Next();
    //! Reset generator to zero.
    void Reset();

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    std::atomic<ui64> Current_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
