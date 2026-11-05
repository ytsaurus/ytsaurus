#include "tskv.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr TStringBuf RecordDelimiter = "\t";

TStringBuf SkipTskvMarker(const TStringBuf rawRecord, const TStringBuf marker)
{
    return rawRecord.StartsWith(marker) ? rawRecord.Tail(marker.size()) : rawRecord;
}

THashMap<TStringBuf, TStringBuf> ParseTskv(
    TStringBuf rawData,
    const TStringBuf marker,
    const THashSet<TStringBuf>& requiredFields)
{
    auto source = rawData;
    rawData = SkipTskvMarker(rawData, marker);

    const bool selectiveParsing = !requiredFields.empty();

    THashMap<TStringBuf, TStringBuf> record;
    if (selectiveParsing) {
        record.reserve(requiredFields.size());
    }

    while (!rawData.empty()) {
        TStringBuf key, value;
        const TStringBuf kvToken = rawData.NextTok(RecordDelimiter);
        if (kvToken.TrySplit('=', key, value)) {
            if (key.empty()) {
                THROW_ERROR_EXCEPTION("An empty key with value %Qv was found in %Qv",
                    value,
                    source);
            }

            if (!selectiveParsing || requiredFields.contains(key)) {
                if (selectiveParsing) {
                    record.emplace_noresize(key, value);
                } else {
                    record[key] = value;
                }
            }
        } else {
            THROW_ERROR_EXCEPTION("split by '=' failed for %Qv in %Qv",
                kvToken,
                source);
        }
    }
    return record;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TTskvSourceComputationParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("data_column", &TThis::DataColumn)
        .Default("data");
    registrar.Parameter("tskv_marker", &TThis::TskvMarker)
        .Default("tskv\t");
}

void TTskvSourceComputationDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

TTskvSwiftSourceComputation::TTskvSwiftSourceComputation(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext,
    THashSet<TStringBuf> requiredFields)
    : TSwiftOrderedSourceComputation(std::move(context), std::move(dynamicContext))
    , RequiredFields_(std::move(requiredFields))
{ }

void TTskvSwiftSourceComputation::DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output)
{
    auto rawData = GetColumnValue<std::optional<TStringBuf>>(inputMessage, GetParameters()->DataColumn);
    if (!rawData) {
        DoProcessUnparsed(inputMessage, TError("empty data"), output);
        return;
    }

    try {
        auto parsed = ParseTskv(*rawData, GetParameters()->TskvMarker, RequiredFields_);

        DoProcessTskv(inputMessage, std::move(parsed), output);
    } catch (const std::exception& ex) {
        DoProcessUnparsed(inputMessage, TError(ex), output);
    }
}

void TTskvSwiftSourceComputation::DoProcessUnparsed(const TInputMessageConstPtr& /*inputMessage*/, TError error, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR error;
}

void TTskvSwiftSourceComputation::DoProcessTskv(
    const TInputMessageConstPtr& /*inputMessage*/,
    THashMap<TStringBuf, TStringBuf>&& inputTskv,
    IOutputCollectorPtr output)
{
    DoProcessTskv(std::move(inputTskv), std::move(output));
}

void TTskvSwiftSourceComputation::DoProcessTskv(
    THashMap<TStringBuf, TStringBuf>&& /*inputTskv*/,
    IOutputCollectorPtr /*output*/)
{
    THROW_ERROR_EXCEPTION("One of the overloads for DoProcessTskv must be implemented");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
