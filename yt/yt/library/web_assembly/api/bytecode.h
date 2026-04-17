#pragma once

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/hash_set.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBytecodeFormat,
    (HumanReadable)
    (Binary)
);

struct TModuleBytecode
{
    EBytecodeFormat Format;
    TSharedRef Data;
    TSharedRef ObjectCode;

    bool operator==(const TModuleBytecode& other) const;
    operator size_t() const;
};

////////////////////////////////////////////////////////////////////////////////

class TModuleBytecodeHashSet final
    : public THashSet<TModuleBytecode>
{ };

DEFINE_REFCOUNTED_TYPE(TModuleBytecodeHashSet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
