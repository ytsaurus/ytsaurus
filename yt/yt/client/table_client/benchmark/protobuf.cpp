#include "common_benchmarks.h"

#include <yt/yt/client/table_client/benchmark/row.pb.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt/client/formats/schemaless_writer_adapter.h>

#include <yt/yt/experiments/random_row/random_row.h>
#include <yt/yt/experiments/random_row/wild_schema_catalog.h>

#include <util/stream/null.h>

#include <util/system/unaligned_mem.h>

using namespace NYT;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTableClientBenchmark;
using namespace NRandomRow;
using namespace NWildSchemaCatalog;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

YT_BENCHMARK_FORMAT_ALL_MESSAGES(FormatWriterBenchmark, EBenchmarkedFormat::Protobuf);
YT_BENCHMARK_FORMAT_ALL_MESSAGES(FormatReaderBenchmark, EBenchmarkedFormat::Protobuf);

////////////////////////////////////////////////////////////////////////////////

constexpr char EntityChar = '#';

TString GenerateRandowProtoMessages(const TTableSchemaPtr& tableSchema, int count)
{
    auto gen = CreateRandomRowGenerator(tableSchema, 42);
    auto owningData = gen->GenerateRows(count);
    std::vector<TUnversionedRow> data;
    for (const auto& row : owningData) {
        data.emplace_back(row);
    }

    auto output = TStringStream();

    {
        auto asyncOutput = CreateAsyncAdapter(static_cast<IOutputStream*>(&output));
        auto writer = CreateFormatIOFactory(EBenchmarkedFormat::Protobuf, tableSchema)->CreateWriter(asyncOutput);
        TRange<TUnversionedRow> range(data);
        writer->Write(range);
        WaitFor(writer->Close())
            .ThrowOnError();
        WaitFor(asyncOutput->Close())
            .ThrowOnError();
    }

    return output.Str();
}

template <typename T>
void NativeProtobufWriterBenchmark(
    benchmark::State& state,
    const TTableSchemaPtr& tableSchema)
{
    auto serializedMessages = GenerateRandowProtoMessages(tableSchema, DatasetRowCount);
    auto current = serializedMessages.cbegin();
    auto end = serializedMessages.cend();

    std::vector<T> messages;
    while (current < end) {
        YT_VERIFY(current + sizeof(ui32) <= end);
        auto length = ReadUnaligned<ui32>(current);
        current += sizeof(length);
        YT_VERIFY(current + length <= end);
        TMemoryInput input(current, length);
        T message;
        YT_VERIFY(message.ParseFromArray(current, length));
        messages.push_back(std::move(message));
        current += length;
    }

    auto nullOutputStream = TNullOutput();

    for (auto _ : state) {
        for (const auto& message : messages) {
            message.SerializeToArcadiaStream(&nullOutputStream);
        }
    }
}

template <typename T>
void NativeProtobufReaderBenchmark(
    benchmark::State& state,
    const TTableSchemaPtr& tableSchema)
{
    auto serializedMessages = GenerateRandowProtoMessages(tableSchema, DatasetRowCount);

    std::vector<T> messages(DatasetRowCount);
    for (auto _ : state) {
        auto current = serializedMessages.cbegin();
        auto end = serializedMessages.cend();
        int messageIndex = 0;
        while (current < end) {
            YT_VERIFY(current + sizeof(ui32) <= end);
            auto length = ReadUnaligned<ui32>(current);
            current += sizeof(length);
            YT_VERIFY(current + length <= end);
            Y_PROTOBUF_SUPPRESS_NODISCARD messages[messageIndex].ParseFromArray(current, length);
            current += length;
        }
    }
}

int VarintSize(ui64 x)
{
    if (x == 0) {
        return 1;
    }
    int res = 0;
    while (x > 0) {
        x >>= 7U;
        ++res;
    }
    return res;
}

inline ui64 ComputeSemidupsDataMessageSizeRawParsing(TUnversionedValue value)
{
    auto it = value.Data.String;
    ++it;

    ui64 msgSize = 0;

    // DocId.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // MainDocId.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Distance.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Reason.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // IsNotMain.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // HasNoContentDups.
    if (*it++ != EntityChar) {
        msgSize += 2;
    }

    return msgSize;
}

inline i64 WriteSemidupsDataMessageRawParsing(TUnversionedValue value, char* buffer)
{
    auto it = value.Data.String;
    ++it;
    const auto oldBuffer = buffer;
    // DocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0xA;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // MainDocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0x10;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Distance.
    if (*it++ != EntityChar) {
        *buffer++ = 0x18;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Reason.
    if (*it++ != EntityChar) {
        *buffer++ = 0x22;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // IsNotMain.
    if (*it++ != EntityChar) {
        *buffer++ = 0x28;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // HasNoContentDups.
    if (*it != EntityChar) {
        *buffer++ = 0x30;
        *buffer++ = static_cast<uint8_t>(*it == 0x05);
        ++it;
    } else {
        ++it;
    }

    return buffer - oldBuffer;
}

Y_FORCE_INLINE void EnsureHasBytesLeft(const char* begin, const char* end, int count)
{
    if (begin + count > end) {
        THROW_ERROR_EXCEPTION("Premature end of stream");
    }
}

inline i64 WriteSemidupsDataMessageRawParsing_SinglePass(TUnversionedValue value, char* buffer)
{
    auto it = value.Data.String;
    auto end = it + value.Length;

    const auto oldBuffer = buffer;
    buffer += MaxVarUint64Size;
    const auto offsetBuffer = buffer;

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // DocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0xA;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // MainDocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0x10;
        while ((it < end) && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Distance.
    if (*it++ != EntityChar) {
        *buffer++ = 0x18;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Reason.
    if (*it++ != EntityChar) {
        *buffer++ = 0x22;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // IsNotMain.
    if (*it++ != EntityChar) {
        *buffer++ = 0x28;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // HasNoContentDups.
    if (*it != EntityChar) {
        *buffer++ = 0x30;
        *buffer++ = static_cast<uint8_t>(*it == 0x05);
        ++it;
    } else {
        ++it;
    }

    auto size = buffer - offsetBuffer;
    auto sizeSize = WriteVarUint64(oldBuffer, size);
    memmove(oldBuffer + sizeSize, offsetBuffer, size);
    return size + sizeSize;
}


inline ui64 ComputeSemidupsDataMessageSizePullParsing(TUnversionedValue value)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    cursor.Next();

    ui64 msgSize = 0;

    // DocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // MainDocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Distance.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Reason.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // IsNotMain.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // HasNoContentDups.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 2;
    }
    cursor.Next();

    return msgSize;
}

inline i64 WriteSemidupsDataMessagePullParsing(TUnversionedValue value, char* buffer)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    cursor.Next();

    const auto oldBuffer = buffer;
    // DocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0xA;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // MainDocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x10;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Distance.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x18;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Reason.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x22;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // IsNotMain.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x28;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // HasNoContentDups.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x30;
        *buffer++ = static_cast<uint8_t>(cursor->UncheckedAsBoolean());
    }
    cursor.Next();

    return buffer - oldBuffer;
}

inline i64 WriteSemidupsDataMessagePullParsing_SinglePass(TUnversionedValue value, char* buffer)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);

    parser.ParseBeginList();

    const auto oldBuffer = buffer;
    buffer += MaxVarUint64Size;
    const auto offsetBuffer = buffer;

    // DocId.
    auto str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0xA;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // MainDocId.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x10;
        buffer += len + 1;
    }

    // Distance.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x18;
         buffer += len + 1;
    }

    // Reason.
    str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0x22;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // IsNotMain.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x28;
        buffer += len + 1;
    }

    // HasNoContentDups.
    if (auto b = parser.ParseOptionalBoolean()) {
        *buffer++ = 0x30;
        *buffer++ = static_cast<uint8_t>(*b);
    }

    auto size = buffer - offsetBuffer;
    auto sizeSize = WriteVarUint64(oldBuffer, size);
    memmove(oldBuffer + sizeSize, offsetBuffer, size);
    return size + sizeSize;
}

inline ui64 ComputeUrlDataMessageSizeRawParsing(TUnversionedValue value)
{
    auto it = value.Data.String;
    ++it;

    ui64 msgSize = 0;

    // Url.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // DocId.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // HasNoContentDups.
    if (*it++ != EntityChar) {
        msgSize += 2;
    }
    ++it;

    // ContentHash.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Simhash.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // TitleHash.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // DocLength.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Language.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Host.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // DocId8.
    if (*it++ != EntityChar) {
        msgSize += 1;
        auto oldIt = it;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            ++it;
        }
        ++it;
        msgSize += (it - oldIt);
    }
    ++it;

    // Region.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    // MainMirror.
    if (*it++ != EntityChar) {
        msgSize += 1; // Tag size.
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        msgSize += VarintSize(strSize) + strSize;
        it += strSize;
    }
    ++it;

    return msgSize;
}

inline i64 WriteUrlDataMessageRawParsing(TUnversionedValue value, char* buffer)
{

    auto it = value.Data.String;
    ++it;
    const auto oldBuffer = buffer;

    // Url.
    if (*it++ != EntityChar) {
        *buffer++ = 0xA;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // DocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0x12;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // HasNoContentDups.
    if (*it != EntityChar) {
        *buffer++ = 0x18;
        *buffer++ = static_cast<uint8_t>(*it == 0x05);
        ++it;
    } else {
        ++it;
    }
    ++it;

    // ContentHash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x20;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Simhash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x28;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // TitleHash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x30;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // DocLength.
    if (*it++ != EntityChar) {
        *buffer++ = 0x38;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Language.
    if (*it++ != EntityChar) {
        *buffer++ = 0x38;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Host.
    if (*it++ != EntityChar) {
        *buffer++ = 0x4A;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // DocId8.
    if (*it++ != EntityChar) {
        *buffer++ = 0x50;
        while (static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        *buffer++ = *it++;
    }
    ++it;

    // Region.
    if (*it++ != EntityChar) {
        *buffer++ = 0x5A;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }
    ++it;

    // MainMirror.
    if (*it++ != EntityChar) {
        *buffer++ = 0x62;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    return buffer - oldBuffer;
}

inline i64 WriteUrlDataMessageRawParsing_SinglePass(TUnversionedValue value, char* buffer)
{

    auto it = value.Data.String;
    auto end = it + value.Length;

    const auto oldBuffer = buffer;
    buffer += MaxVarUint64Size;
    const auto offsetBuffer = buffer;

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Url.
    if (*it++ != EntityChar) {
        *buffer++ = 0xA;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // DocId.
    if (*it++ != EntityChar) {
        *buffer++ = 0x12;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // HasNoContentDups.
    if (*it != EntityChar) {
        *buffer++ = 0x18;
        *buffer++ = static_cast<uint8_t>(*it == 0x05);
        ++it;
    } else {
        ++it;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // ContentHash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x20;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Simhash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x28;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // TitleHash.
    if (*it++ != EntityChar) {
        *buffer++ = 0x30;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // DocLength.
    if (*it++ != EntityChar) {
        *buffer++ = 0x38;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Language.
    if (*it++ != EntityChar) {
        *buffer++ = 0x38;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Host.
    if (*it++ != EntityChar) {
        *buffer++ = 0x4A;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // DocId8.
    if (*it++ != EntityChar) {
        *buffer++ = 0x50;
        while (it < end && static_cast<uint8_t>(*it) & 0x80U) {
            *buffer++ = *it++;
        }
        EnsureHasBytesLeft(it, end, 1);
        *buffer++ = *it++;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // Region.
    if (*it++ != EntityChar) {
        *buffer++ = 0x5A;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    EnsureHasBytesLeft(it, end, 2);
    ++it;

    // MainMirror.
    if (*it++ != EntityChar) {
        *buffer++ = 0x62;
        ui64 strSizeEncoded;
        it += ReadVarUint64(it, end, &strSizeEncoded);
        auto strSize = ZigZagDecode32(strSizeEncoded);
        buffer += WriteVarUint32(buffer, strSize);
        EnsureHasBytesLeft(it, end, strSize);
        memcpy(buffer, it, strSize);
        buffer += strSize;
        it += strSize;
    }

    auto size = buffer - offsetBuffer;
    auto sizeSize = WriteVarUint64(oldBuffer, size);
    memmove(oldBuffer + sizeSize, offsetBuffer, size);
    return size + sizeSize;
}

inline ui64 ComputeUrlDataMessageSizePullParsing(TUnversionedValue value)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    ui64 msgSize = 0;
    cursor.Next();

    // Url.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // DocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // HasNoContentDups.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 2;
    }
    cursor.Next();

    // ContentHash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Simhash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // TitleHash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // DocLength.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Language.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Host.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // DocId8.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        msgSize += 1 + VarintSize(cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Region.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    // MainMirror.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        auto str = cursor->UncheckedAsString();
        msgSize += 1 + VarintSize(str.Size()) + str.Size();
    }
    cursor.Next();

    return msgSize;
}

inline i64 WriteUrlDataMessagePullParsing(TUnversionedValue value, char* buffer)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    cursor.Next();

    const auto oldBuffer = buffer;

    // Url.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0xA;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // DocId.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x12;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // HasNoContentDups.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x18;
        *buffer++ = static_cast<uint8_t>(cursor->UncheckedAsBoolean());
    }
    cursor.Next();

    // ContentHash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x20;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Simhash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x28;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // TitleHash.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x30;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // DocLength.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x38;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Language.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x38;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Host.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x4A;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // DocId8.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x50;
        buffer += WriteVarUint64(buffer, cursor->UncheckedAsUint64());
    }
    cursor.Next();

    // Region.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x5A;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    // MainMirror.
    if (cursor->GetType() != EYsonItemType::EntityValue) {
        *buffer++ = 0x62;
        auto str = cursor->UncheckedAsString();
        buffer += WriteVarUint64(buffer, str.Size());
        memcpy(buffer, str.Data(), str.Size());
        buffer += str.Size();
    }
    cursor.Next();

    return buffer - oldBuffer;
}

inline i64 WriteUrlDataMessagePullParsing_SinglePass(TUnversionedValue value, char* buffer)
{
    TMemoryInput input(value.Data.String, value.Length);
    TYsonPullParser parser(&input, EYsonType::Node);

    const auto oldBuffer = buffer;
    buffer += MaxVarUint64Size;
    const auto offsetBuffer = buffer;

    parser.ParseBeginList();

    // Url.
    auto str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0xA;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // DocId.
    str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0x12;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // HasNoContentDups.
    if (auto b = parser.ParseOptionalBoolean()) {
        *buffer++ = 0x18;
        *buffer++ = static_cast<uint8_t>(*b);
    }

    // ContentHash.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x20;
        buffer += len + 1;
    }

    // Simhash.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x28;
        buffer += len + 1;
    }

    // TitleHash.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x30;
        buffer += len + 1;
    }

    // DocLength.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x38;
        buffer += len + 1;
    }

    // Language.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x40;
        buffer += len + 1;
    }

    // Host.
    str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0x4A;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // DocId8.
    if (auto len = parser.ParseOptionalUint64AsVarint(buffer + 1)) {
        *buffer = 0x50;
        buffer += len + 1;
    }

    // Region.
    str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0x5A;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    // MainMirror.
    str = parser.ParseOptionalString();
    if (str) {
        *buffer++ = 0x62;
        buffer += WriteVarUint64(buffer, str->Size());
        memcpy(buffer, str->Data(), str->Size());
        buffer += str->Size();
    }

    auto size = buffer - offsetBuffer;
    auto sizeSize = WriteVarUint64(oldBuffer, size);
    memmove(oldBuffer + sizeSize, offsetBuffer, size);
    return size + sizeSize;
}



ui32 WriteIntermediateSemidupsData(const TUnversionedValue* values, int count, char* buffer, bool useRawParsing)
{
    auto it = buffer;
    for (int i = 0; i < count; ++i) {
        const auto& value = values[i];
        switch (value.Id) {
            case 0:
                if (value.Type != EValueType::Null) {
                    *it++ = 0x0A;
                    it += WriteVarUint64(it, value.Length);
                    memcpy(it, value.Data.String, value.Length);
                    it += value.Length;
                }
                break;
            case 1:
                if (value.Type != EValueType::Null) {
                    *it++ = 0x10;
                    it += WriteVarUint64(it, value.Data.Uint64);
                }
                break;
            case 2:
                if (value.Type != EValueType::Null) {
                    *it++ = 0x1A;
                    if (useRawParsing) {
                        it += WriteSemidupsDataMessageRawParsing_SinglePass(value, it);
                    } else {
                        it += WriteSemidupsDataMessagePullParsing_SinglePass(value, it);
                    }
                }
                break;
            case 3:
                if (value.Type != EValueType::Null) {
                    *it++ = 0x22;
                    if (useRawParsing) {
                        it += WriteUrlDataMessageRawParsing_SinglePass(value, it);
                    } else {
                        it += WriteUrlDataMessagePullParsing_SinglePass(value, it);
                    }
                }
                break;
        }
    }
    return it - buffer;
}

void HandCodegenProtobufWriteTIntermediateSemidupsDataRowBenchmark(
    benchmark::State& state,
    bool useRawParsing)
{
    auto gen = CreateRandomRowGenerator(GetTIntermediateSemidupsDataSchema(), 42);
    auto owningData = gen->GenerateRows(DatasetRowCount);

    TString buffer(1e7, 0);
    auto nullOutputStream = TNullOutput();
    for (auto _ : state) {
        for (auto& row : owningData) {
            auto it = buffer.Detach();
            ui32 size;
            size = WriteIntermediateSemidupsData(row.begin(), row.GetCount(), it, useRawParsing);
            WritePod(nullOutputStream, size);
            TIntermediateSemidupsDataProto message;
            nullOutputStream.Write(it, size);
        }
    }
}

static void NativeProtobuf_Write_TIntermediateSemidupsDataRow(benchmark::State& state)
{
    NativeProtobufWriterBenchmark<TIntermediateSemidupsDataProto>(state, GetTIntermediateSemidupsDataSchema());
}
BENCHMARK(NativeProtobuf_Write_TIntermediateSemidupsDataRow);

static void NativeProtobuf_Write_TCanonizedUrlRow(benchmark::State& state)
{
    NativeProtobufWriterBenchmark<TCanonizedUrlRowProto>(state, GetTCanonizedUrlSchema());
}
BENCHMARK(NativeProtobuf_Write_TCanonizedUrlRow);

static void NativeProtobuf_Write_TUrlDataRow(benchmark::State& state)
{
    NativeProtobufWriterBenchmark<TUrlDataRowProto>(state, GetTUrlDataSchema());
}
BENCHMARK(NativeProtobuf_Write_TUrlDataRow);

static void NativeProtobuf_Read_TIntermediateSemidupsDataRow(benchmark::State& state)
{
    NativeProtobufReaderBenchmark<TIntermediateSemidupsDataProto>(state, GetTIntermediateSemidupsDataSchema());
}
BENCHMARK(NativeProtobuf_Read_TIntermediateSemidupsDataRow);

static void NativeProtobuf_Read_TCanonizedUrlRow(benchmark::State& state)
{
    NativeProtobufReaderBenchmark<TCanonizedUrlRowProto>(state, GetTCanonizedUrlSchema());
}
BENCHMARK(NativeProtobuf_Read_TCanonizedUrlRow);

static void NativeProtobuf_Read_TUrlDataRow(benchmark::State& state)
{
    NativeProtobufReaderBenchmark<TUrlDataRowProto>(state, GetTUrlDataSchema());
}
BENCHMARK(NativeProtobuf_Read_TUrlDataRow);

static void HandCodegenProtobuf_Write_TIntermediateSemidupsDataRow_RawParsing(benchmark::State& state)
{
    HandCodegenProtobufWriteTIntermediateSemidupsDataRowBenchmark(state, /* useRawParsing */ true);
}
BENCHMARK(HandCodegenProtobuf_Write_TIntermediateSemidupsDataRow_RawParsing);

static void HandCodegenProtobuf_Write_TIntermediateSemidupsDataRow_PullParser(benchmark::State& state)
{
    HandCodegenProtobufWriteTIntermediateSemidupsDataRowBenchmark(state, /* useRawParsing */ false);
}
BENCHMARK(HandCodegenProtobuf_Write_TIntermediateSemidupsDataRow_PullParser);

////////////////////////////////////////////////////////////////////////////////
