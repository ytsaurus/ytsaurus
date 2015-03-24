#pragma once

#ifdef YT_ENABLE_SERIALIZATION_DUMP
#include "format.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_SERIALIZATION_DUMP

class TSerializationDumper
{
public:
    void Indent()
    {
        ++IndentCount_;
    }

    void Unindent()
    {
        --IndentCount_;
    }

    void Suspend()
    {
        ++SuspendCount_;
    }

    void Resume()
    {
        --SuspendCount_;
    }

    bool IsSuspended() const
    {
        return SuspendCount_ > 0;
    }

    template <class... TArgs>
    void Write(const char* format, const TArgs&... args)
    {
        if (IsSuspended())
            return;

        TStringBuilder builder;
        builder.AppendString("DUMP ");
        builder.AppendChar(' ', IndentCount_ * 2);
        builder.AppendFormat(format, args...);
        builder.AppendChar('\n');
        auto buffer = builder.GetBuffer();
        fwrite(buffer.begin(), buffer.length(), 1, stderr);
    }

private:
    int IndentCount_ = 0;
    int SuspendCount_ = 0;

};

class TSerializeDumpIndentGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpIndentGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->Indent();
    }

    TSerializeDumpIndentGuard(TSerializeDumpIndentGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpIndentGuard()
    {
        if (Dumper_) {
            Dumper_->Unindent();
        }
    }

    //! Needed for SERIALIZATION_DUMP_INDENT.
    operator bool() const
    {
        return false;
    }

private:
    TSerializationDumper* Dumper_;

};

class TSerializeDumpSuspendGuard
    : private TNonCopyable
{
public:
    explicit TSerializeDumpSuspendGuard(TSerializationDumper* dumper)
        : Dumper_(dumper)
    {
        Dumper_->Suspend();
    }

    TSerializeDumpSuspendGuard(TSerializeDumpSuspendGuard&& other)
        : Dumper_(other.Dumper_)
    {
        other.Dumper_ = nullptr;
    }

    ~TSerializeDumpSuspendGuard()
    {
        if (Dumper_) {
            Dumper_->Resume();
        }
    }

    //! Needed for SERIALIZATION_DUMP_SUSPEND.
    operator bool() const
    {
        return false;
    }

private:
    TSerializationDumper* Dumper_;

};

#define SERIALIZATION_DUMP_WRITE(context, ...) \
    if ((context).Dumper().IsSuspended()) \
        { } \
    else \
        (context).Dumper().Write(__VA_ARGS__)

#define SERIALIZATION_DUMP_INDENT(context) \
    if (auto SERIALIZATION_DUMP_INDENT__Guard = NYT::TSerializeDumpIndentGuard(&(context).Dumper())) \
        { YUNREACHABLE(); } \
    else

#define SERIALIZATION_DUMP_SUSPEND(context) \
    if (auto SERIALIZATION_DUMP_SUSPEND__Guard = NYT::TSerializeDumpSuspendGuard(&(context).Dumper())) \
        { YUNREACHABLE(); } \
    else

inline Stroka DumpRangeToHex(const TRef& data)
{
    TStringBuilder builder;
    builder.AppendChar('<');
    for (const char* ptr = data.Begin(); ptr != data.End(); ++ptr) {
        ui8 ch = *ptr;
        builder.AppendChar(Int2Hex[ch >> 4]);
        builder.AppendChar(Int2Hex[ch & 0xf]);
    }
    builder.AppendChar('>');
    return builder.Flush();
}

template <class T, class = void>
struct TSerializationDumpPodWriter
{
    template <class C>
    static void Do(C& context, const T& value)
    {
        SERIALIZATION_DUMP_WRITE(context, "pod[%v] %v", sizeof(T), DumpRangeToHex(TRef::FromPod(value)));
    }
};

template <class T>
struct TSerializationDumpPodWriter<T, decltype(ToString(T()), void())>
{
    template <class C>
    static void Do(C& context, const T& value)
    {
        SERIALIZATION_DUMP_WRITE(context, "pod %v", value);
    }
};

#define XX(type) \
    template <> \
    struct TSerializationDumpPodWriter<type> \
    { \
        template <class C> \
        static void Do(C& context, const type& value) \
        { \
            SERIALIZATION_DUMP_WRITE(context, #type " %v", value); \
        } \
    };

XX(i8)
XX(ui8)
XX(i16)
XX(ui16)
XX(i32)
XX(ui32)
XX(i64)
XX(ui64)
XX(float)
XX(double)
XX(char)
XX(bool)
XX(TInstant)
XX(TDuration)

#undef XX

#else

template <class T, class = void>
struct TSerializeDumpWriter
{
    template <class C>
    static void Do(C& /*context*/, const T& /*value*/)
    { }
};

#define SERIALIZATION_DUMP_WRITE(...)         (void) 0
#define SERIALIZATION_DUMP_INDENT(context)
#define SERIALIZATION_DUMP_SUSPEND(context)

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
