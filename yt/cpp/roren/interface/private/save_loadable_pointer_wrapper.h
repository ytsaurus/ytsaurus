#pragma once

#include <util/system/defaults.h>
#include <util/ysaveload.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
struct TSaveLoadablePointerWrapper
{
    static_assert(sizeof(ui64) >= sizeof(TValue*));

    void Save(IOutputStream* output) const
    {
        ui64 intValue = reinterpret_cast<ui64>(Value);
        ::Save(output, intValue);
    }

    void Load(IInputStream* input)
    {
        ui64 intValue;
        ::Load(input, intValue);
        Value = reinterpret_cast<TValue*>(intValue);
    }

    TValue* Value;
};

template <typename TValue>
TSaveLoadablePointerWrapper<TValue>& SaveLoadablePointer(TValue*& value)
{
    return *reinterpret_cast<TSaveLoadablePointerWrapper<TValue>*>(&value);
}

template <typename TValue>
const TSaveLoadablePointerWrapper<TValue>& SaveLoadablePointer(TValue* const & value)
{
    return *reinterpret_cast<const TSaveLoadablePointerWrapper<TValue>*>(&value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
