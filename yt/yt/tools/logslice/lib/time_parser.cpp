#include "time_parser.h"

#include <library/cpp/yt/error/error.h>

#include <util/string/strip.h>
#include <util/string/ascii.h>

#include <array>
#include <cstdio>
#include <cstring>
#include <ctime>

namespace NYT::NLogSlice {

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsDigits(const char* p, int count)
{
    for (int i = 0; i < count; ++i) {
        if (p[i] < '0' || p[i] > '9') {
            return false;
        }
    }
    return true;
}

int ReadInt(const char* p, int count)
{
    int result = 0;
    for (int i = 0; i < count; ++i) {
        result = result * 10 + (p[i] - '0');
    }
    return result;
}

//! Converts a broken-down calendar time given in the local timezone to a Unix
//! timestamp. This is the inverse of TInstant::LocalTime, which is what the
//! plain-text log formatter uses, so log lines round-trip exactly.
time_t LocalBrokenDownToEpoch(int year, int month, int day, int hour, int minute, int second)
{
    struct tm tm;
    std::memset(&tm, 0, sizeof(tm));
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    tm.tm_isdst = -1;
    return ::mktime(&tm);
}

//! Same as above but for a UTC broken-down time.
time_t UtcBrokenDownToEpoch(int year, int month, int day, int hour, int minute, int second)
{
    struct tm tm;
    std::memset(&tm, 0, sizeof(tm));
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    return ::timegm(&tm);
}

std::optional<int> ParseMonthName(TStringBuf name)
{
    static const std::array<TStringBuf, 12> Months = {
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    };
    for (int i = 0; i < 12; ++i) {
        if (AsciiEqualsIgnoreCase(name, Months[i])) {
            return i + 1;
        }
    }
    return std::nullopt;
}

void GetTodayLocal(int* year, int* month, int* day)
{
    time_t now = TInstant::Now().TimeT();
    struct tm tm;
    ::localtime_r(&now, &tm);
    *year = tm.tm_year + 1900;
    *month = tm.tm_mon + 1;
    *day = tm.tm_mday;
}

[[noreturn]] void ThrowBadFormat(TStringBuf input)
{
    THROW_ERROR_EXCEPTION("Unable to parse time %Qv", input);
}

//! Parses a "HH:MM" or "HH:MM:SS" time-of-day string against today's local date.
std::optional<TInstant> TryParseTimeOfDay(TStringBuf s)
{
    int hour, minute, second = 0;
    if (s.size() == 5 && IsDigits(s.data(), 2) && s[2] == ':' && IsDigits(s.data() + 3, 2)) {
        hour = ReadInt(s.data(), 2);
        minute = ReadInt(s.data() + 3, 2);
    } else if (s.size() == 8 && IsDigits(s.data(), 2) && s[2] == ':' &&
        IsDigits(s.data() + 3, 2) && s[5] == ':' && IsDigits(s.data() + 6, 2))
    {
        hour = ReadInt(s.data(), 2);
        minute = ReadInt(s.data() + 3, 2);
        second = ReadInt(s.data() + 6, 2);
    } else {
        return std::nullopt;
    }

    int year, month, day;
    GetTodayLocal(&year, &month, &day);
    return TInstant::Seconds(LocalBrokenDownToEpoch(year, month, day, hour, minute, second));
}

//! Parses a fractional-seconds suffix of 1 to 6 digits (right-padded to
//! microseconds, e.g. "5" -> 500000, "246" -> 246000) into [micros].
bool ParseFractionMicroseconds(TStringBuf fraction, ui64* micros)
{
    if (fraction.empty() || fraction.size() > 6 || !IsDigits(fraction.data(), fraction.size())) {
        return false;
    }
    ui64 value = 0;
    for (int i = 0; i < 6; ++i) {
        value = value * 10 + (i < std::ssize(fraction) ? fraction[i] - '0' : 0);
    }
    *micros = value;
    return true;
}

//! Parses "YYYY-MM-DD HH:MM:SS" in local time, with an optional subsecond
//! selector of 1 to 6 digits delimited by either ',' or '.' (e.g.
//! "2026-06-18 06:00:10,246995" or "2026-06-18 06:00:10.246995").
std::optional<TInstant> TryParseLocalFull(TStringBuf s)
{
    if (s.size() < 19) {
        return std::nullopt;
    }
    const char* p = s.data();
    if (!IsDigits(p, 4) || p[4] != '-' || !IsDigits(p + 5, 2) || p[7] != '-' ||
        !IsDigits(p + 8, 2) || p[10] != ' ' || !IsDigits(p + 11, 2) || p[13] != ':' ||
        !IsDigits(p + 14, 2) || p[16] != ':' || !IsDigits(p + 17, 2))
    {
        return std::nullopt;
    }

    ui64 micros = 0;
    if (s.size() > 19) {
        if ((s[19] != ',' && s[19] != '.') || !ParseFractionMicroseconds(s.SubStr(20), &micros)) {
            return std::nullopt;
        }
    }

    auto result = TInstant::Seconds(LocalBrokenDownToEpoch(
        ReadInt(p, 4), ReadInt(p + 5, 2), ReadInt(p + 8, 2),
        ReadInt(p + 11, 2), ReadInt(p + 14, 2), ReadInt(p + 17, 2)));
    return result + TDuration::MicroSeconds(micros);
}

//! Parses the web-interface format "16 Nov 2018 13:56:14" in local time.
std::optional<TInstant> TryParseWebFormat(TStringBuf s)
{
    TStringBuf rest = s;
    TStringBuf dayTok = rest.NextTok(' ');
    TStringBuf monTok = rest.NextTok(' ');
    TStringBuf yearTok = rest.NextTok(' ');
    TStringBuf timeTok = rest.NextTok(' ');
    if (!rest.empty() || dayTok.empty() || timeTok.empty()) {
        return std::nullopt;
    }
    if (dayTok.size() < 1 || dayTok.size() > 2 || !IsDigits(dayTok.data(), dayTok.size())) {
        return std::nullopt;
    }
    if (yearTok.size() != 4 || !IsDigits(yearTok.data(), 4)) {
        return std::nullopt;
    }
    auto month = ParseMonthName(monTok);
    if (!month) {
        return std::nullopt;
    }
    if (timeTok.size() != 8 || !IsDigits(timeTok.data(), 2) || timeTok[2] != ':' ||
        !IsDigits(timeTok.data() + 3, 2) || timeTok[5] != ':' || !IsDigits(timeTok.data() + 6, 2))
    {
        return std::nullopt;
    }
    return TInstant::Seconds(LocalBrokenDownToEpoch(
        ReadInt(yearTok.data(), 4), *month, ReadInt(dayTok.data(), dayTok.size()),
        ReadInt(timeTok.data(), 2), ReadInt(timeTok.data() + 3, 2), ReadInt(timeTok.data() + 6, 2)));
}

//! Parses the UTC ISO format "YYYY-MM-DDTHH:MM:SS[.ffffff]Z".
std::optional<TInstant> TryParseIsoUtc(TStringBuf s)
{
    if (s.size() < 20 || s[10] != 'T' || s.back() != 'Z') {
        return std::nullopt;
    }
    const char* p = s.data();
    if (!IsDigits(p, 4) || p[4] != '-' || !IsDigits(p + 5, 2) || p[7] != '-' ||
        !IsDigits(p + 8, 2) || !IsDigits(p + 11, 2) || p[13] != ':' ||
        !IsDigits(p + 14, 2) || p[16] != ':' || !IsDigits(p + 17, 2))
    {
        return std::nullopt;
    }

    auto epoch = UtcBrokenDownToEpoch(
        ReadInt(p, 4), ReadInt(p + 5, 2), ReadInt(p + 8, 2),
        ReadInt(p + 11, 2), ReadInt(p + 14, 2), ReadInt(p + 17, 2));
    auto result = TInstant::Seconds(epoch);

    // Optional fractional seconds between position 19 and the trailing 'Z'.
    TStringBuf frac(p + 19, s.size() - 20);
    if (!frac.empty()) {
        if (frac[0] != '.') {
            return std::nullopt;
        }
        frac.Skip(1);
        ui64 micros = 0;
        for (int i = 0; i < 6; ++i) {
            char c = i < std::ssize(frac) ? frac[i] : '0';
            if (c < '0' || c > '9') {
                return std::nullopt;
            }
            micros = micros * 10 + (c - '0');
        }
        result += TDuration::MicroSeconds(micros);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TInstant ParseQueryTime(TStringBuf input)
{
    TStringBuf s = StripString(input);
    if (s.empty()) {
        ThrowBadFormat(input);
    }

    if (AsciiEqualsIgnoreCase(s, "now")) {
        return TInstant::Now();
    }
    if (auto result = TryParseTimeOfDay(s)) {
        return *result;
    }
    if (auto result = TryParseIsoUtc(s)) {
        return *result;
    }
    if (auto result = TryParseLocalFull(s)) {
        return *result;
    }
    if (auto result = TryParseWebFormat(s)) {
        return *result;
    }

    ThrowBadFormat(input);
}

std::optional<TInstant> ParseLogLineTime(TStringBuf line)
{
    // "YYYY-MM-DD HH:MM:SS,uuuuuu"
    constexpr int PrefixLength = 26;
    if (std::ssize(line) < PrefixLength) {
        return std::nullopt;
    }
    const char* p = line.data();
    if (!IsDigits(p, 4) || p[4] != '-' || !IsDigits(p + 5, 2) || p[7] != '-' ||
        !IsDigits(p + 8, 2) || p[10] != ' ' || !IsDigits(p + 11, 2) || p[13] != ':' ||
        !IsDigits(p + 14, 2) || p[16] != ':' || !IsDigits(p + 17, 2) || p[19] != ',' ||
        !IsDigits(p + 20, 6))
    {
        return std::nullopt;
    }

    // Cache the second-resolution epoch keyed by the 19-char prefix: consecutive
    // log lines very often share the same second, and mktime is comparatively slow.
    thread_local bool CacheValid = false;
    thread_local char CacheKey[19];
    thread_local time_t CacheEpoch = 0;

    time_t epoch;
    if (CacheValid && std::memcmp(CacheKey, p, 19) == 0) {
        epoch = CacheEpoch;
    } else {
        epoch = LocalBrokenDownToEpoch(
            ReadInt(p, 4), ReadInt(p + 5, 2), ReadInt(p + 8, 2),
            ReadInt(p + 11, 2), ReadInt(p + 14, 2), ReadInt(p + 17, 2));
        std::memcpy(CacheKey, p, 19);
        CacheEpoch = epoch;
        CacheValid = true;
    }

    return TInstant::Seconds(epoch) + TDuration::MicroSeconds(ReadInt(p + 20, 6));
}

TString FormatLogTime(TInstant instant)
{
    time_t seconds = instant.TimeT();
    struct tm tm;
    ::localtime_r(&seconds, &tm);

    char buffer[32];
    int length = std::snprintf(
        buffer, sizeof(buffer),
        "%04d-%02d-%02d %02d:%02d:%02d,%06u",
        tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
        tm.tm_hour, tm.tm_min, tm.tm_sec,
        static_cast<unsigned>(instant.MicroSecondsOfSecond()));
    return TString(buffer, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogSlice
