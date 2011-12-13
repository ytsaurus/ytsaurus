#pragma once

#include <util/string/cast.h>

namespace NSTAT {

////////////////////////////////////////////////////////////////////////////////

//TODO: заменить на настоящий any-value
typedef Stroka TAnyValue;

template<class T>
inline TAnyValue ToAnyValue(const T &value) {
    return ToString<T>(value);
}

template<class T>
inline T FromAnyValue(const TAnyValue &value) {
    return FromString<T>(value);
}

////////////////////////////////////////////////////////////////////////////////

// Команды вынесены наружу, чтобы они прямо создавались клиентом и
// в Append передавались уже готовые, тогда нет лишнего копирования
// отдельных полей в объект команды
struct TLogCommand {
    enum EType {
        SCALAR = 0,
        VECTOR
    };
    EType ValueType;
    i64 ClockTime;
    Stroka Name;
    Stroka Processors;

    TLogCommand(EType valueType, const Stroka &name)
        : ValueType(valueType), Name(name)
    {}

    TLogCommand(EType valueType, const Stroka &name, const Stroka &processors)
        : ValueType(valueType), Name(name), Processors(processors)
    {}

    // still need support for proper destruction
    virtual ~TLogCommand() {}
};

struct TLogCommandScalar : public TLogCommand {
    TAnyValue Value;

    TLogCommandScalar(const Stroka &name, const TAnyValue &value)
        : TLogCommand(SCALAR, name), Value(value)
    {}
    TLogCommandScalar(const Stroka &name, const TAnyValue &value, const Stroka &processors)
        : TLogCommand(SCALAR, name, processors), Value(value)
    {}
};

struct TNamedValue {
    Stroka Name;
    TAnyValue Value;
};

struct TLogCommandVector : public TLogCommand {
    yvector<TNamedValue> Values;

    TLogCommandVector(const Stroka &name)
        : TLogCommand(VECTOR, name)
    {}

    TLogCommandVector(const Stroka &name, const Stroka &processors)
        : TLogCommand(VECTOR, name, processors)
    {}

    void AddValue(const Stroka &name, const TAnyValue &value) {
        TNamedValue namedValue;
        namedValue.Name = name;
        namedValue.Value = value;
        Values.push_back(namedValue);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool IsStatlogEnabled();
void EnableStatlog(bool state);

void LogQueueAppend(TLogCommand *cmd);

enum EFormat {
    PLAINTEXT_LATEST = 0,
    PLAINTEXT_FULL,
    PLAINTEXT_FULL_WITH_TIMES,

    FORMAT_COUNT
};

Stroka GetDump(EFormat format);

////////////////////////////////////////////////////////////////////////////////

}
