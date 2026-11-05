#include "json.h"

#include <library/cpp/json/json_reader.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TJsonSourceComputationParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("data_column", &TThis::DataColumn)
        .Default("data");
}

void TJsonSourceComputationDynamicParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TJsonSwiftSourceComputation::DoProcessMessage(const TInputMessageConstPtr& inputMessage, IOutputCollectorPtr output)
{
    auto rawData = GetColumnValue<std::optional<TStringBuf>>(inputMessage, GetParameters()->DataColumn);
    if (!rawData) {
        DoProcessUnparsed(inputMessage, TError("empty data"), output);
        return;
    }

    try {
        NJson::TJsonValue json;
        NJson::ReadJsonTree(*rawData, &json, true);

        DoProcessJson(inputMessage, std::move(json), output);
    } catch (const std::exception& ex) {
        DoProcessUnparsed(inputMessage, TError(ex), output);
    }
}

void TJsonSwiftSourceComputation::DoProcessJson(const TInputMessageConstPtr& /*inputMessage*/, NJson::TJsonValue&& inputJson, IOutputCollectorPtr output)
{
    DoProcessJson(std::move(inputJson), output);
}

void TJsonSwiftSourceComputation::DoProcessJson(NJson::TJsonValue&& /*inputJson*/, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR_EXCEPTION("One of the overloads for DoProcessJson must be implemented");
}

void TJsonSwiftSourceComputation::DoProcessUnparsed(const TInputMessageConstPtr& /*inputMessage*/, TError error, IOutputCollectorPtr /*output*/)
{
    THROW_ERROR error;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
