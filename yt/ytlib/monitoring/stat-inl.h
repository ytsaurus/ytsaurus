#pragma once

#include <util/system/tls.h>

namespace NSTAT {

////////////////////////////////////////////////////////////////////////////////
//
// Scalar measure points

template <class T>
inline void LOG_VALUE(const char *name, const T &value) {
    TLogCommand *cmd = new TLogCommandScalar(Stroka(name), ToAnyValue(value));
    LogQueueAppend(cmd);
}

// spec pointer could be 0, then it would be a bit slower
// equivalent of LOG_VALUE(const char *, const T &)
template <class T>
inline void LOG_VALUE(const char *name, const char *const spec, const T &value) {
    TLogCommand *cmd = new TLogCommandScalar(Stroka(name), ToAnyValue(value), ToString(spec));
    LogQueueAppend(cmd);
}

////////////////////////////////////////////////////////////////////////////////
//
// Vector measure points (or values with living state)
//

enum ECounterType {
    CT_TIME
};

struct TCounter {
    const int TYPE;
    TCounter(int type) : TYPE(type) {}
};

struct TTimeCounter : public TCounter {
    static const ECounterType Type = CT_TIME;
    TTimeCounter() : TCounter(Type) {}

    bool FirstRun;
    int PointsMet;

    struct TPoint {
        i64 Time;
        Stroka Name;
    };
    yvector<TPoint> TimePoints;
};

inline void _VisitPoint(TTimeCounter *counter, const char *pointName) {
    if (counter->FirstRun) {
        VERIFY(counter->PointsMet == counter->TimePoints.ysize(), "invalid time point trace: %d != %d", counter->PointsMet, counter->TimePoints.ysize());
        TTimeCounter::TPoint p;
        p.Name = pointName;
        counter->TimePoints.push_back(p);
    }
    TTimeCounter::TPoint &p = counter->TimePoints[counter->PointsMet];
    YASSERT(p.Name == pointName);
    p.Time = GetCycleCount();

    counter->PointsMet += 1;
}

inline void _StartSeq(TTimeCounter *counter, const char *name) {
    UNUSED(name);
    if (counter->TimePoints.ysize() == 0) {
        counter->FirstRun = true;
    }
    counter->PointsMet = 0;
    _VisitPoint(counter, "._init");
}

inline void _FinishSeq(TTimeCounter *counter, const char *name) {
    _VisitPoint(counter, "._fini");

    if (counter->FirstRun) {
        counter->FirstRun = false;
    }

    //XXX: we need min/max/average for every point and the whole interval
    // + total elapsed time and series hit count
    TLogCommandVector *cmd = new TLogCommandVector(name, "tmvV");
    for (size_t i = 0; i < counter->TimePoints.size(); ++i) {
        const TTimeCounter::TPoint &p = counter->TimePoints[i];
        // имя подаётся без родительского префикса, полное собирает та сторона
        cmd->AddValue(p.Name, ToAnyValue(p.Time));

        //XXX: clear all point times from the pointer maybe?
    }

    LogQueueAppend(cmd);
}

////////////////////////////////////////////////////////////////////////////////

typedef yhash_map<const char *, TAutoPtr<TCounter> > TTempCounters;

extern THREAD(TTempCounters) Counters;

TCounter* CreateCounter(ECounterType type);

template <class T>
T* CreateCounter() {
    return static_cast<T*>(CreateCounter(T::Type));
}

template <class T>
T* GetCounter(const char *name) {
    const TTempCounters &counters = Counters.Get();
    TTempCounters::const_iterator i = counters.find(name);
    if (i != Counters.Get().end()) {
        VERIFY(i->second->TYPE == T::Type,
               "%s is type %d counter but %d type was expected",
               name, i->second->TYPE, T::Type);
        return static_cast<T*>(i->second.Get());
    }
    return 0;
}

template <class T>
T* GetOrCreateCounter(const char *name) {
    T *counter = GetCounter<T>(name);
    if (!counter) {
        counter = CreateCounter<T>();
        TTempCounters &counters = Counters.Get();
        counters[name] = counter;
    }
    return counter;
}

inline void _ThreadTimeStart(const char *name) {
    TTimeCounter *counter = GetOrCreateCounter<TTimeCounter>(name);
    _StartSeq(counter, name);
}

inline void _ThreadTimePoint(const char *name, const char *pointName) {
    TTimeCounter *counter = GetCounter<TTimeCounter>(name);
    YASSERT(counter);
    _VisitPoint(counter, pointName);
}

inline void _ThreadTimeEnd(const char *name) {
    TTimeCounter *counter = GetCounter<TTimeCounter>(name);
    YASSERT(counter);
    _FinishSeq(counter, name);
}

////////////////////////////////////////////////////////////////////////////////

}
