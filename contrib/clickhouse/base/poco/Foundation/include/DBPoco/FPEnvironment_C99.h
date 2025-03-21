//
// FPEnvironment_C99.h
//
// Library: Foundation
// Package: Core
// Module:  FPEnvironment
//
// Definitions of class FPEnvironmentImpl for C99.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_FPEnvironment_C99_INCLUDED
#define DB_Foundation_FPEnvironment_C99_INCLUDED


#include <cmath>
#include <fenv.h>
#include "DBPoco/Foundation.h"


namespace DBPoco
{


class FPEnvironmentImpl
{
protected:
    enum RoundingModeImpl
    {
        FP_ROUND_DOWNWARD_IMPL = FE_DOWNWARD,
        FP_ROUND_UPWARD_IMPL = FE_UPWARD,
        FP_ROUND_TONEAREST_IMPL = FE_TONEAREST,
        FP_ROUND_TOWARDZERO_IMPL = FE_TOWARDZERO
    };
    enum FlagImpl
    {
        FP_DIVIDE_BY_ZERO_IMPL = FE_DIVBYZERO,
        FP_INEXACT_IMPL = FE_INEXACT,
        FP_OVERFLOW_IMPL = FE_OVERFLOW,
        FP_UNDERFLOW_IMPL = FE_UNDERFLOW,
        FP_INVALID_IMPL = FE_INVALID
    };
    FPEnvironmentImpl();
    FPEnvironmentImpl(const FPEnvironmentImpl & env);
    ~FPEnvironmentImpl();
    FPEnvironmentImpl & operator=(const FPEnvironmentImpl & env);
    void keepCurrentImpl();
    static void clearFlagsImpl();
    static bool isFlagImpl(FlagImpl flag);
    static void setRoundingModeImpl(RoundingModeImpl mode);
    static RoundingModeImpl getRoundingModeImpl();
    static bool isInfiniteImpl(float value);
    static bool isInfiniteImpl(double value);
    static bool isInfiniteImpl(long double value);
    static bool isNaNImpl(float value);
    static bool isNaNImpl(double value);
    static bool isNaNImpl(long double value);
    static float copySignImpl(float target, float source);
    static double copySignImpl(double target, double source);
    static long double copySignImpl(long double target, long double source);

private:
    fenv_t _env;
};


//
// inlines
//
inline bool FPEnvironmentImpl::isInfiniteImpl(float value)
{
    return std::isinf(value) != 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(double value)
{
    return std::isinf(value) != 0;
}


inline bool FPEnvironmentImpl::isInfiniteImpl(long double value)
{
    return std::isinf((double)value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(float value)
{
    return std::isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(double value)
{
    return std::isnan(value) != 0;
}


inline bool FPEnvironmentImpl::isNaNImpl(long double value)
{
    return std::isnan((double)value) != 0;
}


inline float FPEnvironmentImpl::copySignImpl(float target, float source)
{
    return copysignf(target, source);
}


inline double FPEnvironmentImpl::copySignImpl(double target, double source)
{
    return copysign(target, source);
}


} // namespace DBPoco


#endif // DB_Foundation_FPEnvironment_C99_INCLUDED
