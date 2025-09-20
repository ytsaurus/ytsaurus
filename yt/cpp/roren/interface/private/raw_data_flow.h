#pragma once

#include "../fwd.h"

#include "fwd.h"

#include <util/generic/hash.h>
#include <util/stream/fwd.h>

#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawOutput
    : public virtual NYT::TRefCounted
{
public:
    virtual void AddRaw(const void* row, ssize_t count) = 0;
    virtual void MoveRaw(void* row, ssize_t count)
    {
        AddRaw(row, count);
    }

    virtual void Close() = 0;

    template <typename TRow>
    Y_FORCE_INLINE TOutput<TRow>* Upcast()
    {
        return reinterpret_cast<TOutput<TRow>*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(IRawOutput);

////////////////////////////////////////////////////////////////////////////////

class TNullOutput
    : public IRawOutput
{
public:
    void AddRaw(const void*, ssize_t) override
    { }

    void Close() override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class IRawInput
    : public virtual NYT::TRefCounted
{
public:
    virtual const void* NextRaw() = 0;

    template <typename TRow>
    Y_FORCE_INLINE TInput<TRow>* Upcast()
    {
        return reinterpret_cast<TInput<TRow>*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(IRawInput);

////////////////////////////////////////////////////////////////////////////////

// A set of inputs, grouped together.
//
// Needed for CoGroupByKey, etc.
//
// It is assumed that the class implementation stores a set of IRawInput.
// Each IRawInput allows iterating over some group of values.
class IRawGroupedInput
    : public virtual NYT::TRefCounted
{
public:
    //
    // Returns a set of IRawInputPtr.
    //
    // The list of returned pointers must not change during the lifetime of IRawGroupedInput.
    virtual ssize_t GetInputCount() const = 0;
    virtual IRawInputPtr GetInput(ssize_t i) const = 0;

    //
    // Switches the state of internal IRawInput to read the next group.
    //
    // The IRawInput objects themselves remain the same, but their state is switched
    // so that they now iterate over the next group.
    virtual const void* NextKey() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
