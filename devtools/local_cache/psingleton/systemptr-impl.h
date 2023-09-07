#pragma once

#include "systemptr.h"

namespace NSystemWideName {
    struct TAddressBare {
        /// In milliseconds.
        time_t Ctime = 0;
        TProcessId Pid = 0;
    };
    /// POD to read from lock file.
    struct TAddress : TAddressBare {
        /// AF_LOCAL, AF_INET, AF_INET6.
        int Namespace = 0;
        /// String address in host format.
        TString Address;
        /// Port for inet addresses.
        TIpPort Port = 0;
    };

    struct TParseException: public virtual TBadArgumentException {};

    void ReadProcessInfo(const TStringBuf& input, TAddress& out);
    void ReadProcessInfo(const TStringBuf& input, TAddressBare& out);

    /// Get process' create time in OS-dependent units.
    /// We need _change_ of status to disambiguate collisions among pids,
    /// so errors are ignored.
    time_t GetProcessCtime(TProcessId) noexcept;
}

template <>
struct THash<TProcessUID> {
    inline size_t operator()(const TProcessUID& p) const {
        return THash<ui64>()(p.Pid + p.StartTime);
    }
};

template <>
inline TString ToString<TProcessUID>(const TProcessUID& n) {
    TStringStream out;
    out.Reserve(64);
    out << "Pid=" << n.Pid << Endl;
    out << "Ctime=" << n.StartTime << Endl;
    return out.Str();
}

inline TString TProcessUID::ToPlainString() const {
    TStringStream out;
    out.Reserve(64);
    out << "Pid=" << Pid << ", Ctime=" << StartTime;
    return out.Str();
}

template <>
inline TString ToString<TSystemWideName>(const TSystemWideName& n) {
    TStringStream out;
    out.Reserve(TSystemWideName::MAX_SIZE + 3);

    if (auto* neta = std::get_if<0>(&n.Address)) {
        auto port = neta->second;
        if (auto* ip4 = std::get_if<TIp4>(&neta->first)) {
            out << "Address=\"inet:" << IpToString(*ip4) << '"' << Endl;
        } else if (auto* ip6 = std::get_if<TIp6>(&neta->first)) {
            out << "Address=\"inet6:" << Ip6ToString(*ip6) << '"' << Endl;
        }
        out << "Port=" << port << Endl;
    } else if (auto* local = std::get_if<1>(&n.Address)) {
        out << "Address=\"local:" << *local << '"' << Endl;
    }

    out << ToString<TProcessUID>(n);

    Y_ASSERT(out.Str().length() <= TSystemWideName::MAX_SIZE);

    return out.Str();
}

inline TString TSystemWideName::ToPlainString() const {
    TStringStream out;
    out.Reserve(TSystemWideName::MAX_SIZE + 3);

    if (auto* neta = std::get_if<0>(&Address)) {
        auto port = neta->second;
        if (auto* ip4 = std::get_if<TIp4>(&neta->first)) {
            out << "Address=\"inet:" << IpToString(*ip4) << "\", ";
        } else if (auto* ip6 = std::get_if<TIp6>(&neta->first)) {
            out << "Address=\"inet6:" << Ip6ToString(*ip6) << "\", ";
        }
        out << "Port=" << port << ", ";
    } else if (auto* local = std::get_if<1>(&Address)) {
        out << "Address=\"local:" << *local << "\", ";
    }

    out << TProcessUID::ToPlainString();

    Y_ASSERT(out.Str().length() <= TSystemWideName::MAX_SIZE);

    return out.Str();
}

template <>
inline bool TryFromStringImpl<TProcessUID, char>(const char* data,
                                                 size_t len,
                                                 TProcessUID& result) {
    NSystemWideName::TAddressBare out;
    try {
        ReadProcessInfo(TStringBuf(data, len), out);
    } catch (NSystemWideName::TParseException&) {
        return false;
    }
    result = TProcessUID(out.Pid, out.Ctime);
    return true;
}

template <>
inline bool TryFromStringImpl<TSystemWideName, char>(const char* data,
                                                     size_t len,
                                                     TSystemWideName& result) {
    NSystemWideName::TAddress out;
    try {
        ReadProcessInfo(TStringBuf(data, len), out);
    } catch (NSystemWideName::TParseException&) {
        return false;
    }

    switch (out.Namespace) {
#if !defined(_win_)
        case YAF_LOCAL:
            result = TSystemWideName(TSystemWideName::TAddress(out.Address),
                                     out.Pid, out.Ctime);
            return true;
#endif

        case AF_INET:
            result = TSystemWideName(
                TSystemWideName::TAddress(std::make_pair(
                    TIp4Or6(IpFromString(out.Address.c_str())), out.Port)),
                out.Pid, out.Ctime);
            return true;

        case AF_INET6:
            result = TSystemWideName(
                TSystemWideName::TAddress(std::make_pair(
                    TIp4Or6(Ip6FromString(out.Address.c_str())), out.Port)),
                out.Pid, out.Ctime);
            return true;
        default:
            break;
    }
    return false;
}

template <>
inline TProcessUID FromStringImpl<TProcessUID, char>(const char* data,
                                                     size_t len) {
    TProcessUID result;
    TryFromStringImpl<TProcessUID, char>(data, len, result);
    return result;
}

template <>
inline TSystemWideName FromStringImpl<TSystemWideName, char>(const char* data,
                                                             size_t len) {
    TSystemWideName result;
    TryFromStringImpl<TSystemWideName, char>(data, len, result);
    return result;
}
