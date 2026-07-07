#pragma once

#include <optional>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TOneShotFlag
{
public:
    void Set(bool value) noexcept;
    bool Get() const noexcept;

    void operator=(bool value) noexcept;

    operator bool () const noexcept;

private:
    std::optional<bool> Flag_;
};

// NB(pogorelov): Doesn't need to be an atomic,
// cause it can be modified only once (and its modification will happen before it can be read).
inline TOneShotFlag DeliveryFencedWriteEnabled;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
