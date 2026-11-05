#pragma once

#include <string>
#include <cstdint>

namespace clickhouse {

class InputStream;
class OutputStream;

class WireFormat {
public:
    template <typename T>
    static bool ReadFixed(InputStream& input, T* value);
    static bool ReadString(InputStream& input, std::string* value);
    static bool SkipString(InputStream& input);
    static bool ReadBytes(InputStream& input, void* buf, size_t len);
    static bool ReadUInt64(InputStream& input, uint64_t* value);
    static bool ReadVarint64(InputStream& output, uint64_t* value);

    template <typename T>
    static void WriteFixed(OutputStream& output, const T& value);
    static void WriteBytes(OutputStream& output, const void* buf, size_t len);
    static void WriteString(OutputStream& output, std::string_view value);
    static void WriteQuotedString(OutputStream& output, std::string_view value);
    static void WriteParamNullRepresentation(OutputStream& output);
    static void WriteUInt64(OutputStream& output, const uint64_t value);
    static void WriteVarint64(OutputStream& output, uint64_t value);

private:
    static bool ReadAll(InputStream& input, void* buf, size_t len);
    static void WriteAll(OutputStream& output, const void* buf, size_t len);
};

template <typename T>
inline bool WireFormat::ReadFixed(InputStream& input, T* value) {
    return ReadAll(input, value, sizeof(T));
}

inline bool WireFormat::ReadString(InputStream& input, std::string* value) {
    uint64_t len = 0;
    if (ReadVarint64(input, &len)) {
        if (len > 0x00FFFFFFULL) {
            return false;
        }
        value->resize((size_t)len);
        return ReadAll(input, value->data(), (size_t)len);
    }

    return false;
}

inline bool WireFormat::ReadBytes(InputStream& input, void* buf, size_t len) {
    return ReadAll(input, buf, len);
}

inline bool WireFormat::ReadUInt64(InputStream& input, uint64_t* value) {
    return ReadVarint64(input, value);
}

template <typename T>
inline void WireFormat::WriteFixed(OutputStream& output, const T& value) {
    WriteAll(output, &value, sizeof(T));
}

inline void WireFormat::WriteBytes(OutputStream& output, const void* buf, size_t len) {
    WriteAll(output, buf, len);
}

inline void WireFormat::WriteString(OutputStream& output, std::string_view value) {
    WriteVarint64(output, value.size());
    WriteAll(output, value.data(), value.size());
}

inline void WireFormat::WriteUInt64(OutputStream& output, const uint64_t value) {
    WriteVarint64(output, value);
}

}
