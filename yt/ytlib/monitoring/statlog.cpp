#include "stdafx.h"

#include <util/string/vector.h>
#include <util/string/cast.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>
#include <quality/Misc/HPTimer.h>

#include "statlog.h"

namespace NSTAT {

struct TDataPoint {
    i64 Time;
    TAnyValue Value;
};

}

// for running JoinStroku over TSeries
template<>
inline Stroka ToString<NSTAT::TDataPoint>(const NSTAT::TDataPoint& p) {
    return p.Value;
}

namespace NSTAT {

////////////////////////////////////////////////////////////////////////////////

//FIXME: звать GetClockRate только один раз, не использовать статических констант
inline i64 Microseconds(i64 clocks) {
    static const i64 CLOCKS_IN_MICROSECOND = i64(NHPTimer::GetClockRate() / 1e6);
    return clocks / CLOCKS_IN_MICROSECOND;
}

inline i64 Nanoseconds(i64 clocks) {
    static const i64 CLOCKS_IN_NANOSECOND = i64(NHPTimer::GetClockRate() / 1e9);
    return clocks / CLOCKS_IN_NANOSECOND;
}

inline i64 FromMicroseconds(i64 t) {
    static const i64 CLOCKS_IN_MICROSECOND = i64(NHPTimer::GetClockRate() / 1e6);
    return t * CLOCKS_IN_MICROSECOND;
}

inline i64 FromNanoseconds(i64 t) {
    static const i64 CLOCKS_IN_NANOSECOND = i64(NHPTimer::GetClockRate() / 1e9);
    return t * CLOCKS_IN_NANOSECOND;
}

//#define CLOCKS_TO_SI Nanoseconds
#define CLOCKS_TO_SI Microseconds

////////////////////////////////////////////////////////////////////////////////

// Measurements for use in statlog internals (that bypass InputQueue entirely).
#define SELF_TIMEIT(log, name, spec, code) \
{ \
    (log)->RegisterProcessors(name, spec); \
    i64 start = GetCycleCount(); \
    code; \
    i64 end = GetCycleCount(); \
    (log)->Append(name, ToAnyValue(end - start), end); \
}
#define SELF_LOG(log, name, spec, value) \
{ \
    (log)->RegisterProcessors(name, spec); \
    (log)->Append(name, ToAnyValue(value), GetCycleCount()); \
}

////////////////////////////////////////////////////////////////////////////////

typedef yvector<TDataPoint> TSeries;
typedef yhash_map<Stroka, TSeries> TValueLogs;

static inline const TAnyValue& LastValue(TValueLogs *logs, const Stroka &name) {
    return (*logs)[name].back().Value;
}
static inline void PushNewPoint(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &value) {
    TSeries &series = (*logs)[name];
    series.push_back();
    TDataPoint &point = series.back();
    point.Time = time;
    point.Value = value;
}
static inline void UpdateLastPoint(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &value) {
    TDataPoint &point = (*logs)[name].back();
    point.Time = time;
    point.Value = value;
}
static inline void UpdateLastPointTime(TValueLogs *logs, const Stroka &name, i64 time) {
    (*logs)[name].back().Time = time;
}

//DEBUG
// finds min and max time points of the entire log
void FindTimeRange(const TValueLogs &logs, i64 *min, i64 *max) {
    *min = *max = 0;
    for (TValueLogs::const_iterator t = logs.begin(); t != logs.end(); ++t) {
        const TSeries &series = t->second;
        if (series.size() > 0) {
            *max = Max(*max, series.back().Time);
            if (*min == 0)
                *min = series.front().Time;
            else
                *min = Min(*min, series.front().Time);
        }
    }
}

template <class T, i64 T::value_type::*TimeField>
void CutTimedSequence(T *sequence, const i64 keepPeriod) {
    const i64 time = sequence->back().*TimeField - keepPeriod;
    typename T::iterator i = sequence->begin();
    while (i->*TimeField < time)
        ++i;
    sequence->assign(i, sequence->end());
}

typedef void (*TProcessor)(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &value);
TProcessor ResolveProcessor(const char id);

typedef void (*TVectorProcessor)(TValueLogs *logs, const Stroka &name, i64 time, const yvector<TNamedValue> &values);
TVectorProcessor ResolveVectorProcessor(const char id);

////////////////////////////////////////////////////////////////////////////////

struct TLogData {
    typedef yhash_map<Stroka, Stroka> TValueProcessors;

    TValueProcessors ProcessorSpecs;
    TValueLogs Log;

    void RegisterProcessors(const Stroka &name, const Stroka &procSpec) {
        //XXX: да, существующая спецификация заменяется на новую
        ProcessorSpecs[name] = procSpec;
    }

    void Append(const Stroka &name, const TAnyValue &value, const i64 time) {
        TValueProcessors::const_iterator found = ProcessorSpecs.find(name);
        if (found != ProcessorSpecs.end()) {
            const Stroka &spec = found->second;
            for (size_t i = 0; i < spec.size(); ++i) {
                const char c = spec[i];
                if (TProcessor proc = ResolveProcessor(c)) {
                    proc(&Log, name, time, value);
                } else {
                    //XXX: добавить диагностику
                    //VERIFY(false, "Value \"%s\" uses unknown processor specification \'%c\'", name.c_str(), c);
                }
            }
        } else {
            // shortcut для пустого процессора
            PushNewPoint(&Log, name, time, value);
        }
    }

    void Append(const Stroka &name, const yvector<TNamedValue> &values, const i64 time) {
        TValueProcessors::const_iterator found = ProcessorSpecs.find(name);
        if (found != ProcessorSpecs.end()) {
            const Stroka &spec = found->second;
            for (size_t i = 0; i < spec.size(); ++i) {
                const char c = spec[i];
                if (TVectorProcessor proc = ResolveVectorProcessor(c)) {
                    proc(&Log, name, time, values);
                } else {
                    //XXX: добавить диагностику
                    //YASSERT("Value \"%s\" uses unknown processor specification '%c\'", name.c_str(), c);
                }
            }
        } else {
            // shortcut для пустого процессора
            Stroka qname;
            for (size_t i = 0; i < values.size(); ++i) {
                qname = name + values[i].Name;
                PushNewPoint(&Log, qname, time, values[i].Value);
            }
        }
    }

    void Recycle(const i64 keepPeriod) {
        for (TValueLogs::iterator t = Log.begin(); t != Log.end(); ++t) {
            TSeries &series = t->second;
            if (series.size() > 0) {
                const i64 storedPeriod = series.back().Time - series.front().Time;
                if (storedPeriod > (2 * keepPeriod)) {
                    CutTimedSequence<TSeries, &TSeries::value_type::Time>(&series, keepPeriod);
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TRealTimeline maps time points expressed in "cpu clock cycles"
// to timestamps (as microseconds since epoch) in real time.
//
struct TRealTimeLine {
    struct TMark {
        i64 MicrosecondsTimestamp;
        i64 Clocks;

        static TMark Now() {
            i64 start = GetCycleCount();
            i64 timestamp = TInstant::Now().MicroSeconds();
            i64 end = GetCycleCount();
            TMark result;
            result.MicrosecondsTimestamp = timestamp;
            result.Clocks = i64(start + float(end - start) / 2);
            return result;
        }
    };

    typedef yvector<TMark> TTimeline;
    TTimeline Timeline;

    void AddSyncMark() {
        Timeline.push_back(TMark::Now());
    }

    void TryAddSyncMark(const i64 updatePeriod) {
        if (Timeline.empty() || (GetCycleCount() - updatePeriod) > Timeline.back().Clocks) {
            AddSyncMark();
        }
    }

    void Recycle(const i64 keepPeriod) {
        if (Timeline.empty())
            return;
        const i64 storedPeriod = Timeline.back().Clocks - Timeline.front().Clocks;
        if (storedPeriod > 2 * keepPeriod) {
            CutTimedSequence<TTimeline, &TTimeline::value_type::Clocks>(&Timeline, keepPeriod);
        }
    }

    //XXX: somewhat optimized for periods of monotonically increasing sequences
    i64 FindTimestamp(i64 time/*, size_t *hint = 0*/) const {
        if (Timeline.empty())
            return 0;

        // beyond edge cases
        if (time < Timeline.front().Clocks) {
            return Timeline.front().MicrosecondsTimestamp - Microseconds(Timeline.front().Clocks - time);
        }
        if (time > Timeline.back().Clocks) {
            return Timeline.back().MicrosecondsTimestamp + Microseconds(time - Timeline.back().Clocks);
        }

        // normal case
        size_t i = 1;
        //size_t i = ((hint && *hint) ? *hint : 1);
        for (; i < Timeline.size(); ++i) {
            if (Timeline[i].Clocks > time) {
                break;
            }
        }
        //if (hint)
        //    *hint = i;
        const TMark &first = Timeline[i - 1];
        const TMark &second = Timeline[i];

        double k = double(second.MicrosecondsTimestamp - first.MicrosecondsTimestamp)
                / (second.Clocks - first.Clocks);

        return first.MicrosecondsTimestamp + i64(k * (time - first.Clocks));
    }
};

Stroka DumpLastAsPlainText(const TValueLogs &log) {
    Stroka r;
    i64 pointCount = 0;
    for (TValueLogs::const_iterator t = log.begin(); t != log.end(); ++t) {
        const Stroka &name = t->first;
        const TSeries &series = t->second;
        r += name + "\t" + (series.empty() ? "" : ToString(series.back())) + "\n";
        pointCount += series.size();
    }

    r += "self.traces";
    r += " " + ToString(log.size()) + "\n";
    r += "self.points";
    r += " " + ToString(pointCount) + "\n";

    return r;
}

Stroka DumpAsPlainText(const TValueLogs &log) {
    Stroka r;
    i64 pointCount = 0;
    i64 size = 0;
    for (TValueLogs::const_iterator t = log.begin(); t != log.end(); ++t) {
        const Stroka &name = t->first;
        const TSeries &series = t->second;
        r += name + ' ';
        r += JoinStroku(series.begin(), series.end(), " ");
        r += '\n';
        pointCount += series.size();

        for (TSeries::const_iterator i = series.begin(); i != series.end(); ++i) {
            size += sizeof(Stroka) + Stroka::TDataTraits::CalcDataSize(i->Value.size());
        }
    }

    r += "self.traces";
    r += " " + ToString(log.size()) + "\n";
    r += "self.points";
    r += " " + ToString(pointCount) + "\n";
    r += "self.pointdatasize";
    r += " " + ToString(size) + "\n";

    return r;
}

Stroka DumpWithTimesAsPlainText(const TValueLogs &log, const TRealTimeLine &realTimeLine) {
    Stroka r;
    for (TValueLogs::const_iterator t = log.begin(); t != log.end(); ++t) {
        const Stroka &name = t->first;
        const TSeries &series = t->second;
        r += name + ' ';

        if (!series.empty()) {
            r += "[[";
            TSeries::const_iterator i = series.begin();
            r += ToString(realTimeLine.FindTimestamp(i->Time) / 1000);
            r += ", ";
            r += i->Value;
            for (++i; i != series.end(); ++i) {
                r += "], [";
                r += ToString(realTimeLine.FindTimestamp(i->Time) / 1000);
                r += ", ";
                r += i->Value;
            }
            r += "]]";
        }

        r += '\n';
    }
    return r;
}

Stroka DumpWithTimesAsJSON(const TValueLogs &log, const TRealTimeLine &realTimeLine) {
    Stroka r;
    r += "[\n";
    for (TValueLogs::const_iterator t = log.begin(); t != log.end(); ++t) {
        const Stroka &name = t->first;
        const TSeries &series = t->second;

        r += "{ label: \"";
        r += name;
        r += "\", data: [";

        if (!series.empty()) {
            TSeries::const_iterator i = series.begin();
            r += "[";
            i64 t = realTimeLine.FindTimestamp(i->Time) / 1000;
            r += ToString(t);
            r += ", ";
            r += i->Value;
            i64 mark = t;
            for (++i; i != series.end(); ++i) {
                i64 t = realTimeLine.FindTimestamp(i->Time) / 1000;
                // with time precision lowered from microseconds to milliseconds
                // there would be points collapsed into excactly the same moment,
                // we drop all those points but first
                if (t == mark)
                    continue;
                r += "], [";
                r += ToString(t);
                r += ", ";
                r += i->Value;
            }
            r += "]";
        }
        r += "] },\n";
    }
    r += "]";

    return r;
}

////////////////////////////////////////////////////////////////////////////////

struct TLatestDumper {
    TLogData* Log;
    TLatestDumper(TLogData *log) : Log(log) {}
    Stroka operator()() {
        Stroka result;
        SELF_TIMEIT(Log, "self.runloop.dump.last", "tmv",
            result = DumpLastAsPlainText(Log->Log);
        )
        SELF_LOG(Log, "self.runloop.dump.last.size", "s", result.size());
        return result;
    }
};

struct TFullDumper {
    TLogData* Log;
    TFullDumper(TLogData *log) : Log(log) {}
    Stroka operator()() {
        Stroka result;
        SELF_TIMEIT(Log, "self.runloop.dump.full", "tmv",
            result = DumpAsPlainText(Log->Log);
        )
        SELF_LOG(Log, "self.runloop.dump.full.size", "s", result.size());
        return result;
    }
};

struct TFullWithTimesDumper {
    TLogData *Log;
    const TRealTimeLine &RealTimeLine;
    TFullWithTimesDumper(TLogData *log, const TRealTimeLine &realTimeLine)
        : Log(log), RealTimeLine(realTimeLine)
    {}
    Stroka operator()() {
        Stroka result;
        SELF_TIMEIT(Log, "self.runloop.dump.fullwithtimes", "tmv",
            result = DumpWithTimesAsJSON(Log->Log, RealTimeLine);
        )
        SELF_LOG(Log, "self.runloop.dump.fullwithtimes.size", "s", result.size());
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCachedDump {
    const i64 UpdatePeriod;
    Stroka Dump;
    i64 LastUpdateTime;

    TCachedDump(const i64 updatePeriod)
        : UpdatePeriod(FromMicroseconds(updatePeriod))
        , LastUpdateTime(0)
    {}

    template <class Func>
    void TryUpdateWith(Func func) {
        i64 time = GetCycleCount();
        if ((time - LastUpdateTime) > UpdatePeriod) {
            Dump = func();
            LastUpdateTime = time;
        }
    }
};

struct TElementCounter {
    i64 ElementCount;

    TElementCounter() : ElementCount(0) {}

    template <class T>
    void IncCount(const T &data)
    {
        (void)data;
        ++ElementCount;
    }
    template <class T>
    void DecCount(const T &data)
    {
        (void)data;
        --ElementCount;
    }
};

class TLogDataProxy {

    TLockFreeQueue<TLogCommand*, TElementCounter> InputQueue;
    Event QueueHasData;
    TThread Thread;

    TLogData *const Log;
    TAutoPtr<TRealTimeLine> RealTimeLine;
    yvector<TAutoPtr<TCachedDump> > Dumps;

    static void* ThreadProc(void *data);
    void RunLoop();
    void ProcessInput(TLogCommand *cmd);

public:
    TLogDataProxy(TLogData *log);

    void Append(TLogCommand *cmd) {
        InputQueue.Enqueue(cmd);
        QueueHasData.Signal();
    }

    Stroka Dump(EYsonFormat format) {
        return Dumps[format]->Dump;
    }
};

//XXX: why microseconds? use plain seconds instead
static const i64 LOG_DUMP_UPDATE_PERIOD = (1 * 1000 * 1000); // in μs
static const i64 LOG_KEEP_PERIOD = (5 * 1000 * 1000); // in μs
static const i64 TIME_SYNC_UPDATE_PERIOD = (5 * 1000 * 1000); // in μs

TLogDataProxy::TLogDataProxy(TLogData *log)
    : Thread(ThreadProc, this), Log(log)
{
    Dumps.resize(FORMAT_COUNT);
    for (int i = 0; i < FORMAT_COUNT; ++i) {
        Dumps[i] = new TCachedDump(LOG_DUMP_UPDATE_PERIOD);
    }
    Thread.Start();
    Thread.Detach();
}

void* TLogDataProxy::ThreadProc(void *data) {
    TLogDataProxy *self = reinterpret_cast<TLogDataProxy*>(data);
    self->RunLoop();
    return NULL;
}

void TLogDataProxy::RunLoop() {
    Log->Append("self.clockspermicrosecond", ToAnyValue(100000000 / Microseconds(100000000)), GetCycleCount());

    const i64 KeepPeriod = FromMicroseconds(LOG_KEEP_PERIOD);
    const i64 TimeSyncPeriod = FromMicroseconds(TIME_SYNC_UPDATE_PERIOD);

    RealTimeLine = new TRealTimeLine;
    RealTimeLine->AddSyncMark();

    TLogCommand *cmd = NULL;
    for(;;) {
        // двойной Dequeue нужен при работе по event
        if (!InputQueue.Dequeue(&cmd)) {
            QueueHasData.Reset();
            if(!InputQueue.Dequeue(&cmd)) {
                QueueHasData.Wait();
                continue;
            }
        }
        SELF_LOG(Log, "self.inputqueuesize", "s", InputQueue.GetCounter().ElementCount);

        SELF_TIMEIT(Log, "self.runloop.process", "tmv",
        ProcessInput(cmd);
        );

        SELF_TIMEIT(Log, "self.runloop.recycle", "tmv",
        Log->Recycle(KeepPeriod);
        );

        SELF_TIMEIT(Log, "self.runloop.timesync", "tmv",
        RealTimeLine->TryAddSyncMark(TimeSyncPeriod);
        RealTimeLine->Recycle(3 * KeepPeriod);
        );

        Dumps[PLAINTEXT_LATEST]->TryUpdateWith(TLatestDumper(Log));
        Dumps[PLAINTEXT_FULL]->TryUpdateWith(TFullDumper(Log));
        Dumps[PLAINTEXT_FULL_WITH_TIMES]->TryUpdateWith(TFullWithTimesDumper(Log, *RealTimeLine.Get()));
    }
}

void TLogDataProxy::ProcessInput(TLogCommand *cmd) {
    if (cmd->ValueType == TLogCommand::SCALAR) {
        TLogCommandScalar *s = (TLogCommandScalar*)cmd;
        Log->RegisterProcessors(s->Name, s->Processors);
        Log->Append(s->Name, s->Value, s->ClockTime);
        delete s;
    } else if (cmd->ValueType == TLogCommand::VECTOR) {
        TLogCommandVector *v = (TLogCommandVector*)cmd;
        Log->RegisterProcessors(v->Name, v->Processors);
        Log->Append(v->Name, v->Values, v->ClockTime);
        delete v;
    }
}

////////////////////////////////////////////////////////////////////////////////

static bool StatlogEnabled = false;

TLogDataProxy* GetLogger() {
    static TAutoPtr<TLogData> log;
    static TAutoPtr<TLogDataProxy> proxy;
    if (StatlogEnabled && !proxy) {
        log = new TLogData;
        // одновременно создаётся дополнительный поток исполнения
        proxy = new TLogDataProxy(log.Get());
    }
    return proxy.Get();
}

////////////////////////////////////////////////////////////////////////////////

// statlog actual interface

bool IsStatlogEnabled() {
    return StatlogEnabled;
}
void EnableStatlog(bool state) {
    StatlogEnabled = state;
}

void LogQueueAppend(TLogCommand *cmd) {
    if (TLogDataProxy* log = GetLogger()) {
        cmd->ClockTime = GetCycleCount();
        log->Append(cmd);
    } else {
        delete cmd; // virtual destructor kicks in
    }
}

Stroka GetDump(EYsonFormat format) {
    if (TLogDataProxy* log = GetLogger()) {
        return log->Dump(format);
    } else {
        return Stroka("Statlog disabled, no dump");
    }
}

////////////////////////////////////////////////////////////////////////////////

static void ScalarClocksToTime(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &value) {
    PushNewPoint(logs, name, time, ToAnyValue(CLOCKS_TO_SI(FromAnyValue<i64>(value))));
}
static void ScalarStore(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &value) {
    PushNewPoint(logs, name, time,  value);
}
static void ScalarMinMax(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &/*value*/) {
    const TAnyValue &nvalue = LastValue(logs, name);
    i64 value = FromAnyValue<i64>(nvalue);
    Stroka qname;
    i64 prev;
    i64 curr;
    qname = name + ".min";
    if ((*logs)[qname].size() > 0) {
        prev = FromAnyValue<i64>(LastValue(logs, qname));
        curr = Min(prev, value);
        if (curr != prev) {
            PushNewPoint(logs, qname, time, ToAnyValue(curr));
        } else {
            UpdateLastPointTime(logs, qname, time);
        }
    } else {
        PushNewPoint(logs, qname, time, nvalue);
    }
    qname = name + ".max";
    if ((*logs)[qname].size() > 0) {
        prev = FromAnyValue<i64>(LastValue(logs, qname));
        curr = Max(prev, value);
        if (curr != prev) {
            PushNewPoint(logs, qname, time, ToAnyValue(curr));
        } else {
            UpdateLastPointTime(logs, qname, time);
        }
    } else {
        PushNewPoint(logs, qname, time, nvalue);
    }
}
static void ScalarAverage(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &/*value*/) {
    Stroka qname;
    i64 count = 0;

    qname = name + ".count";
    if ((*logs)[qname].size() > 0) {
        count = FromAnyValue<i64>(LastValue(logs, qname));
        UpdateLastPoint(logs, qname, time, ToAnyValue(count + 1));
    } else {
        PushNewPoint(logs, qname, time, ToAnyValue(count + 1));
    }

    const TAnyValue &nvalue = LastValue(logs, name);
    qname = name + ".avg";
    if ((*logs)[qname].size() > 0) {
        i64 value = FromAnyValue<i64>(nvalue);
        i64 prev = FromAnyValue<i64>(LastValue(logs, qname));
        i64 curr = (prev * (count - 1) + value) / count;
        if (curr != prev) {
            PushNewPoint(logs, qname, time, ToAnyValue(curr));
        } else {
            UpdateLastPointTime(logs, qname, time);
        }
    } else {
        PushNewPoint(logs, qname, time, nvalue);
    }
}
static void ScalarModifiedMovingAverage(TValueLogs *logs, const Stroka &name, i64 time, const TAnyValue &/*value*/) {
    const double A = 0.1;
    Stroka qname;

    const TAnyValue &nvalue = LastValue(logs, name);
    //TODO: составление имени можно оптимизировать
    qname = name + ".mma";
    if ((*logs)[qname].size() > 0) {
        i64 value = FromAnyValue<i64>(nvalue);
        i64 prev = FromAnyValue<i64>(LastValue(logs, qname));
        i64 curr = prev + A * (value - prev);
        //if (curr != prev) {
            PushNewPoint(logs, qname, time, ToAnyValue(curr));
        //} else {
        //    UpdateLastPointTime(logs, qname, time);
        //}
    } else {
        PushNewPoint(logs, qname, time, nvalue);
    }
}

TProcessor ResolveProcessor(const char id) {
    switch(id) {
        case 't': return ScalarClocksToTime; break;
        case 's': return ScalarStore; break;
        case 'm': return ScalarMinMax; break;
        case 'v': return ScalarAverage; break;
        case 'V': return ScalarModifiedMovingAverage; break;
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

// from N points calculates N-1 intervals
//XXX: also incorporates clocks to time conversion
static void VectorCalcIntervals(TValueLogs *logs, const Stroka &name, i64 time, const yvector<TNamedValue> &values) {
    i64 elapsed = 0;
    Stroka qname;
    i64 prev = FromAnyValue<i64>(values[0].Value);
    for (int i = 0; i < values.ysize() - 1; ++i) {
        i64 next = FromAnyValue<i64>(values[i+1].Value);
        elapsed = CLOCKS_TO_SI(next - prev);
        prev = next;
        //TODO: составление имени можно оптимизировать
        qname = name + values[i].Name;
        PushNewPoint(logs, qname, time, ToAnyValue(elapsed));
    }
    // duration of the entire interval
    {
        i64 start = FromAnyValue<i64>(values.back().Value);
        i64 end = FromAnyValue<i64>(values.front().Value);
        elapsed = CLOCKS_TO_SI(start - end);
        PushNewPoint(logs, name, time, ToAnyValue(elapsed));
    }
}
static void VectorMinMax(TValueLogs *logs, const Stroka &name, i64 time, const yvector<TNamedValue> &values) {
    TAnyValue nvalue;
    i64 value;
    Stroka qname;
    Stroka sourceName;
    i64 prev;
    i64 curr;
    // skip last name, as it unused after VectorCalcIntervals
    for (int i = 0; i < values.ysize() - 1; ++i) {
        //TODO: составление имени можно оптимизировать
        sourceName = name + values[i].Name;
        nvalue = LastValue(logs, sourceName);
        value = FromAnyValue<i64>(nvalue);

        qname = sourceName + ".min";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = Min(prev, value);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }

        //TODO: составление имени можно оптимизировать
        qname = sourceName + ".max";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = Max(prev, value);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
    // and that of the entire interval
    {
        i64 value = FromAnyValue<i64>(LastValue(logs, name));

        //TODO: составление имени можно оптимизировать
        qname = name + ".min";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = Min(prev, value);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }

        //TODO: составление имени можно оптимизировать
        qname = name + ".max";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = Max(prev, value);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
}
static void VectorAverage(TValueLogs *logs, const Stroka &name, i64 time, const yvector<TNamedValue> &values) {
    Stroka qname;
    i64 count = 0;

    qname = name + ".count";
    if ((*logs)[qname].size() > 0) {
        count = FromAnyValue<i64>(LastValue(logs, qname));
        UpdateLastPoint(logs, qname, time, ToAnyValue(count + 1));
    } else {
        PushNewPoint(logs, qname, time, ToAnyValue(count + 1));
    }

    Stroka sourceName;
    i64 prev;
    i64 curr;
    TAnyValue nvalue;
    i64 value;
    // skip last name, as it unused after VectorCalcIntervals
    for (int i = 0; i < values.ysize() - 1; ++i) {
        //TODO: составление имени можно оптимизировать
        sourceName = name + values[i].Name;
        nvalue = LastValue(logs, sourceName);
        value = FromAnyValue<i64>(LastValue(logs, sourceName));

        qname = sourceName + ".avg";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = (prev * count + value) / (count + 1);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
    // and that of the entire interval
    {
        const TAnyValue &nvalue = LastValue(logs, name);

        //TODO: составление имени можно оптимизировать
        qname = name + ".avg";
        if ((*logs)[qname].size() > 0) {
            i64 value = FromAnyValue<i64>(nvalue);
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = (prev * count + value) / (count + 1);
            if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            } else {
                UpdateLastPointTime(logs, qname, time);
            }
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
}
static void VectorModifiedMovingAverage(TValueLogs *logs, const Stroka &name, i64 time, const yvector<TNamedValue> &values) {
    const double A = 0.1;//2.0 / 501;
    Stroka qname;

    Stroka sourceName;
    i64 prev;
    i64 curr;
    TAnyValue nvalue;
    i64 value;
    // skip last name, as it unused after VectorCalcIntervals
    for (int i = 0; i < values.ysize() - 1; ++i) {
        //TODO: составление имени можно оптимизировать
        sourceName = name + values[i].Name;
        nvalue = LastValue(logs, sourceName);
        value = FromAnyValue<i64>(LastValue(logs, sourceName));

        qname = sourceName + ".mma";
        if ((*logs)[qname].size() > 0) {
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = prev + A * (value - prev);
            //if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            //} else {
            //    UpdateLastPointTime(logs, qname, time);
            //}
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
    // and that of the entire interval
    {
        const TAnyValue &nvalue = LastValue(logs, name);

        //TODO: составление имени можно оптимизировать
        qname = name + ".mma";
        if ((*logs)[qname].size() > 0) {
            i64 value = FromAnyValue<i64>(nvalue);
            prev = FromAnyValue<i64>(LastValue(logs, qname));
            curr = prev + A * (value - prev);
            //if (curr != prev) {
                PushNewPoint(logs, qname, time, ToAnyValue(curr));
            //} else {
            //    UpdateLastPointTime(logs, qname, time);
            //}
        } else {
            PushNewPoint(logs, qname, time, nvalue);
        }
    }
}

TVectorProcessor ResolveVectorProcessor(const char id) {
    switch(id) {
        case 't': return VectorCalcIntervals; break;
        case 'm': return VectorMinMax; break;
        case 'v': return VectorAverage; break;
        case 'V': return VectorModifiedMovingAverage; break;
    }
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

}
