#pragma once

#include <cmath>
#include <util/system/atomic.h>
#include <util/system/yassert.h>

// TODO: code review pending

class TMetric
{
    TAtomic Sum;
    TAtomic SumSq;
    TAtomic Num;
     
public:
    TMetric()
       : Sum(0)
       , SumSq(0)
       , Num(0)
    { }

    inline void AddValue(ui64 v)
    {
        ui64 max = (1 << sizeof(TAtomic) * 8) - 1;
        YASSERT(max - Sum > v);
        YASSERT(max - SumSq > v * v);
        AtomicAdd(Sum, v);
        AtomicAdd(SumSq, v * v);
        AtomicIncrement(Num);
    }

    inline double GetMean()
    {
        if (!Num)
            return 0.;
        return (1. * Sum) / Num;
    }

    inline double GetStd()
    {
        if (Num < 2)
            return 0;
        return sqrt(abs((1. * SumSq) / Num - GetMean()) * (Num / (Num - 1)));
    }

    inline ui64 GetNum()
    {
        return Num;
    }
};
