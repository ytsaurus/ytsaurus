#pragma once

#include "../fwd.h"

#include "fwd.h"

#include <util/generic/ptr.h>
#include <util/generic/hash.h>
#include <util/stream/fwd.h>

#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawOutput
    : public virtual TThrRefBase
{
public:
    virtual void AddRaw(const void* row, ssize_t count) = 0;
    virtual void Close() = 0;

    template <typename TRow>
    Y_FORCE_INLINE TOutput<TRow>* Upcast()
    {
        return reinterpret_cast<TOutput<TRow>*>(this);
    }
};

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
    : public virtual TThrRefBase
{
public:
    virtual const void* NextRaw() = 0;

    template <typename TRow>
    Y_FORCE_INLINE TInput<TRow>* Upcast()
    {
        return reinterpret_cast<TInput<TRow>*>(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

//
// Набор входов, которые разбиты по группам.
//
// Нужно для CoGroupByKey и т.п.
//
// Подразумевается, что реализация класса хранит набор IRawInput.
// Каждый IRawInput позволяет проитерироваться по какой-то группе значений.
class IRawGroupedInput
    : public virtual TThrRefBase
{
public:
    //
    // Возвращает набор IRawInputPtr.
    //
    // Список возвращаемых адресов не должен меняться в процессе жизни IRawGroupedInput.
    virtual ssize_t GetInputCount() const = 0;
    virtual IRawInputPtr GetInput(ssize_t i) const = 0;

    //
    // Переключает состояние внутренних IRawInput на чтение следующей группы.
    //
    // Сами объекты IRawInput остаются теми же, но их состояние переключается так,
    // что они теперь итерируются поверх следующей группы.
    virtual const void* NextKey() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
