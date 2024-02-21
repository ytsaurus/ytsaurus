#include "web_assembly_data_transfer.h"

#include <yt/yt/library/web_assembly/api/pointer.h>

namespace NYT::NWebAssembly {

using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

template <>
TCopyGuard CopyIntoCompartment(TRange<TPIValue> range, IWebAssemblyCompartment* compartment)
{
    i64 rangeByteLength = range.size() * sizeof(TPIValue);
    i64 byteLength = rangeByteLength;
    for (auto& value : range) {
        if (IsStringLikeType(value.Type)) {
            byteLength += value.Length;
        }
    }

    uintptr_t copyOffset = compartment->AllocateBytes(byteLength);

    auto* destination = ConvertPointerFromWasmToHost(std::bit_cast<char*>(copyOffset), byteLength);
    auto* copiedRangeAtHost = std::bit_cast<TPIValue*>(destination);

    ::memcpy(destination, range.Begin(), rangeByteLength);
    destination += rangeByteLength;

    for (size_t index = 0; index < range.size(); ++index) {
        if (IsStringLikeType(range[index].Type)) {
            ::memcpy(destination, range[index].AsStringBuf().data(), range[index].Length);
            copiedRangeAtHost[index].SetStringPosition(destination);
            destination += range[index].Length;
        }
    }

    return {compartment, copyOffset};
}

////////////////////////////////////////////////////////////////////////////////

TCopyGuard CopyOpaqueDataIntoCompartment(
    TRange<void*> opaqueData,
    TRange<size_t> opaqueDataSizes,
    IWebAssemblyCompartment* compartment)
{
    YT_ASSERT(opaqueData.size() == opaqueDataSizes.size());

    if (opaqueData.Empty()) {
        return {compartment, 0};
    }

    i64 rangeByteLength = opaqueData.size() * sizeof(void*);
    i64 byteLength = rangeByteLength;
    for (auto& size : opaqueDataSizes) {
        byteLength += size;
    }

    uintptr_t copyOffset = compartment->AllocateBytes(byteLength);

    auto* destination = ConvertPointerFromWasmToHost(std::bit_cast<char*>(copyOffset), byteLength);
    auto* copiedRangeAtHost = std::bit_cast<size_t*>(destination);

    destination += rangeByteLength;

    for (size_t index = 0; index < opaqueData.size(); ++index) {
        if (opaqueDataSizes[index] == 0) {
            // Not a POD value.
            copiedRangeAtHost[index] = std::bit_cast<size_t>(opaqueData[index]);
        } else {
            // A POD value.
            ::memcpy(destination, opaqueData[index], opaqueDataSizes[index]);
            uintptr_t offset = copyOffset + (destination - std::bit_cast<char*>(copiedRangeAtHost));
            copiedRangeAtHost[index] = offset;
            destination += opaqueDataSizes[index];
        }
    }

    return {compartment, copyOffset};
}

std::pair<TCopyGuard, std::vector<TPIValue*>> CopyRowRangeIntoCompartment(
    std::vector<const TValue*>& rows,
    i64 stringLikeColumnsDataWeight,
    const TRowSchemaInformation& rowSchemaInformation,
    IWebAssemblyCompartment* compartment)
{
    if (rows.empty()) {
        return {TCopyGuard{compartment, 0}, std::vector<TPIValue*>{}};
    }

    i64 singleRowByteLength = rowSchemaInformation.Length * sizeof(TPIValue);
    i64 allRowsByteLength = singleRowByteLength * rows.size();

    i64 batchByteLength = allRowsByteLength + stringLikeColumnsDataWeight;

    uintptr_t copyOffset = compartment->AllocateBytes(batchByteLength);
    uintptr_t rowOffset = copyOffset;

    auto* startPointer = ConvertPointerFromWasmToHost(std::bit_cast<char*>(copyOffset), batchByteLength);
    auto* destinationRow = std::bit_cast<TPIValue*>(startPointer);
    auto* destinationString = startPointer + allRowsByteLength;

    std::vector<TPIValue*> resultOffsets;
    resultOffsets.reserve(rows.size());

    for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
        ::memcpy(destinationRow, rows[rowIndex], singleRowByteLength);

        for (int index : rowSchemaInformation.StringLikeIndices) {
            TPIValue* value = &destinationRow[index];
            if (value->Type != EValueType::Null) {
                ::memcpy(destinationString, std::bit_cast<char*>(value->Data.Uint64), value->Length);
                value->SetStringPosition(destinationString);
                destinationString += value->Length;
            }
        }

        resultOffsets.push_back(std::bit_cast<TPIValue*>(rowOffset));
        rowOffset += singleRowByteLength;

        destinationRow += rowSchemaInformation.Length;
    }

    return {TCopyGuard{compartment, copyOffset}, std::move(resultOffsets)};
}

std::pair<TCopyGuard, std::vector<NQueryClient::TPIValue*>> CopyRowRangeIntoCompartment(
    TRange<TRange<TPIValue>> rows,
    IWebAssemblyCompartment* compartment)
{
    i64 allRowsByteLength = 0;
    i64 allStringsByteLength = 0;
    for (auto& row : rows) {
        allRowsByteLength += row.size() * sizeof(TPIValue);
        for (auto& item : row) {
            if (IsStringLikeType(item.Type)) {
                allStringsByteLength += item.Length;
            }
        }
    }

    i64 totalByteLength = allRowsByteLength + allStringsByteLength;
    uintptr_t copyOffset = compartment->AllocateBytes(totalByteLength);
    auto* startPointer = ConvertPointerFromWasmToHost(std::bit_cast<char*>(copyOffset), totalByteLength);
    auto* itemDestination = std::bit_cast<TPIValue*>(startPointer);
    uintptr_t currentRowOffset = copyOffset;
    auto* stringsDestination = startPointer + allRowsByteLength;
    auto resultOffsets = std::vector<TPIValue*>();
    resultOffsets.reserve(rows.size());

    for (size_t rowIndex = 0; rowIndex < rows.size(); ++rowIndex) {
        resultOffsets.push_back(std::bit_cast<TPIValue*>(currentRowOffset));

        for (size_t itemIndex = 0; itemIndex < rows[rowIndex].size(); ++itemIndex) {
            auto& item = rows[rowIndex][itemIndex];
            ::memcpy(itemDestination, &item, sizeof(TPIValue));

            if (IsStringLikeType(item.Type)) {
                ::memcpy(stringsDestination, item.AsStringBuf().data(), item.Length);
                itemDestination->SetStringPosition(stringsDestination);
                stringsDestination += item.Length;
            }

            ++itemDestination;
        }

        currentRowOffset += rows[rowIndex].size() * sizeof(TPIValue);
    }

    return {TCopyGuard{compartment, copyOffset}, std::move(resultOffsets)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
