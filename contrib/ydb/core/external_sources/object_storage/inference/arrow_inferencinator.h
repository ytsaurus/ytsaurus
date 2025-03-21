#pragma once

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NExternalSource::NObjectStorage::NInference {

enum class EFileFormat {
    Undefined,
    CsvWithNames,
    TsvWithNames,
    JsonEachRow,
    JsonList,
    Parquet,
};

constexpr EFileFormat ConvertFileFormat(TStringBuf format) {
    if (format == "csv_with_names") {
        return EFileFormat::CsvWithNames;
    }
    if (format == "tsv_with_names") {
        return EFileFormat::TsvWithNames;
    }
    if (format == "json_each_row") {
        return EFileFormat::JsonEachRow;
    }
    if (format == "json_list") {
        return EFileFormat::JsonList;
    }
    if (format == "parquet") {
        return EFileFormat::Parquet;
    }
    return EFileFormat::Undefined;
}

constexpr TStringBuf ConvertFileFormat(EFileFormat format) {
    switch (format) {

    case EFileFormat::CsvWithNames:
        return "csv_with_names";
    case EFileFormat::TsvWithNames:
        return "tsv_with_names";
    case EFileFormat::JsonEachRow:
        return "json_each_row";
    case EFileFormat::JsonList:
        return "json_list";
    case EFileFormat::Parquet:
      return "parquet";
    case EFileFormat::Undefined:
    default:
        return "UNSUPPORTED";
    }
}

constexpr bool IsArrowInferredFormat(EFileFormat format) {
    return format != EFileFormat::Undefined;
}

constexpr bool IsArrowInferredFormat(TStringBuf format) {
    return IsArrowInferredFormat(ConvertFileFormat(format));
}

NActors::IActor* CreateArrowInferencinator(NActors::TActorId arrowFetcher);
} // namespace NKikimr::NExternalSource::NObjectStorage::NInference
