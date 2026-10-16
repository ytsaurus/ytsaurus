#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TNoopSpecValidator
{
    static void Validate(const auto& /*spec*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

#define YT_FLOW_EXTEND_SPEC_VALIDATION(validateFunction) \
    using TValidatorBase = TValidator;                   \
    struct TValidator                                    \
    {                                                    \
        static void Validate(const auto& spec)           \
        {                                                \
            TValidatorBase::Validate(spec);              \
            validateFunction(spec);                      \
        }                                                \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
