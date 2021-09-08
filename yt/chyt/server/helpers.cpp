#include "helpers.h"

#include "conversion.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/logging/log.h>

#include <Common/FieldVisitorToString.h>

#include <DataTypes/DataTypeNullable.h>

#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <Access/AccessControlManager.h>
#include <Access/User.h>

#include <util/string/escape.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TGuid ToGuid(DB::UUID uuid)
{
    TGuid result;
    memcpy(&result, &uuid, sizeof(uuid));
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void RegisterNewUser(DB::AccessControlManager& accessControlManager, TString userName)
{
    auto user = std::make_unique<DB::User>();
    user->setName(userName);
    user->access.grant(DB::AccessFlags::allFlags(), "YT" /* database */);
    user->access.grant(DB::AccessFlags::allFlags(), "system" /* database */);
    user->access.grant(DB::AccessType::CREATE_TEMPORARY_TABLE);
    user->access.grant(DB::AccessType::dictGet);

    accessControlManager.tryInsert(std::move(user));
}

////////////////////////////////////////////////////////////////////////////////

DB::Field GetMinimumTypeValue(const DB::DataTypePtr& dataType)
{
    switch (dataType->getTypeId()) {
        case DB::TypeIndex::Nullable:
            return DB::Field();

        case DB::TypeIndex::Int8:
            return DB::Field(std::numeric_limits<DB::Int8>::min());
        case DB::TypeIndex::Int16:
            return DB::Field(std::numeric_limits<DB::Int16>::min());
        case DB::TypeIndex::Int32:
            return DB::Field(std::numeric_limits<DB::Int32>::min());
        case DB::TypeIndex::Int64:
            return DB::Field(std::numeric_limits<DB::Int64>::min());

        case DB::TypeIndex::UInt8:
            return DB::Field(std::numeric_limits<DB::UInt8>::min());
        case DB::TypeIndex::UInt16:
            return DB::Field(std::numeric_limits<DB::UInt16>::min());
        case DB::TypeIndex::UInt32:
            return DB::Field(std::numeric_limits<DB::UInt32>::min());
        case DB::TypeIndex::UInt64:
            return DB::Field(std::numeric_limits<DB::UInt64>::min());

        case DB::TypeIndex::Float32:
            return DB::Field(-std::numeric_limits<DB::Float32>::infinity());
        case DB::TypeIndex::Float64:
            return DB::Field(-std::numeric_limits<DB::Float64>::infinity());

        case DB::TypeIndex::Date:
            return DB::Field(std::numeric_limits<DB::UInt16>::min());
        case DB::TypeIndex::DateTime:
            return DB::Field(std::numeric_limits<DB::UInt32>::min());
        // TODO(dakovalkov): Now timestamps is represented as UInt64, not DateTime64.
        // case DB::TypeIndex::DateTime64:
            // return DB::Field(std::numeric_limits<DB::Decimal64>::min());

        case DB::TypeIndex::String:
            return DB::Field("");

        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", dataType->getName());
    }
}

DB::Field GetMaximumTypeValue(const DB::DataTypePtr& dataType)
{
    switch (dataType->getTypeId()) {
        case DB::TypeIndex::Nullable:
            return GetMaximumTypeValue(DB::removeNullable(dataType));

        case DB::TypeIndex::Int8:
            return DB::Field(std::numeric_limits<DB::Int8>::max());
        case DB::TypeIndex::Int16:
            return DB::Field(std::numeric_limits<DB::Int16>::max());
        case DB::TypeIndex::Int32:
            return DB::Field(std::numeric_limits<DB::Int32>::max());
        case DB::TypeIndex::Int64:
            return DB::Field(std::numeric_limits<DB::Int64>::max());

        case DB::TypeIndex::UInt8:
            return DB::Field(std::numeric_limits<DB::UInt8>::max());
        case DB::TypeIndex::UInt16:
            return DB::Field(std::numeric_limits<DB::UInt16>::max());
        case DB::TypeIndex::UInt32:
            return DB::Field(std::numeric_limits<DB::UInt32>::max());
        case DB::TypeIndex::UInt64:
            return DB::Field(std::numeric_limits<DB::UInt64>::max());

        case DB::TypeIndex::Float32:
            return DB::Field(std::numeric_limits<DB::Float32>::infinity());
        case DB::TypeIndex::Float64:
            return DB::Field(std::numeric_limits<DB::Float64>::infinity());

        case DB::TypeIndex::Date:
            return DB::Field(std::numeric_limits<DB::UInt16>::max());
        case DB::TypeIndex::DateTime:
            return DB::Field(std::numeric_limits<DB::UInt32>::max());
        // TODO(dakovalkov): Now timestamps is represented as UInt64, not DateTime64.
        // case DB::TypeIndex::DateTime64:
            // return DB::Field(std::numeric_limits<DB::Decimal64>::max());

        case DB::TypeIndex::String:
            // The "maximum" string does not exist.
            // Set some big value instead of it.
            // NOTE: CH uses unsigned char comparison, so max char is '\xff', not '\xef'.
            return DB::Field(std::string(SentinelMaxStringLength, '\xff'));

        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", dataType->getName());
    }
}

std::optional<DB::Field> TryDecrementFieldValue(const DB::Field& field, const DB::DataTypePtr& dataType)
{
    if (field == GetMinimumTypeValue(dataType)) {
        return std::nullopt;
    }
    switch (dataType->getTypeId()) {
        case DB::TypeIndex::Nullable:
            // When the decremented value is unrepresented in removeNullable(dataType),
            // we theoreticly can represent it as Null, because Null is smaller than any value.
            // But we do not care since this function declared to help only in 'simple cases'.
            return TryDecrementFieldValue(field, DB::removeNullable(dataType));

        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
            return DB::Field(field.get<Int64>() - 1);

        case DB::TypeIndex::UInt8:
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt64:
            return DB::Field(field.get<UInt64>() - 1);

        case DB::TypeIndex::Date:
        case DB::TypeIndex::DateTime:
            return DB::Field(field.get<UInt64>() - 1);

        case DB::TypeIndex::String: {
            std::string value = field.get<std::string>();
            if (value.back() != '\0') {
                value.back() = static_cast<unsigned char>(value.back()) - 1;
            } else {
                value.pop_back();
            }
            return DB::Field(std::move(value));
        }

        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            // Not implemented yet.
            return std::nullopt;

        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", dataType->getName());
    }
}

std::optional<DB::Field> TryIncrementFieldValue(const DB::Field& field, const DB::DataTypePtr& dataType)
{
    if (field == GetMaximumTypeValue(dataType)) {
        return std::nullopt;
    }
    switch (dataType->getTypeId()) {
        case DB::TypeIndex::Nullable:
            return TryIncrementFieldValue(field, DB::removeNullable(dataType));

        case DB::TypeIndex::Int8:
        case DB::TypeIndex::Int16:
        case DB::TypeIndex::Int32:
        case DB::TypeIndex::Int64:
            return DB::Field(field.get<Int64>() + 1);

        case DB::TypeIndex::UInt8:
        case DB::TypeIndex::UInt16:
        case DB::TypeIndex::UInt32:
        case DB::TypeIndex::UInt64:
            return DB::Field(field.get<UInt64>() + 1);

        case DB::TypeIndex::Date:
        case DB::TypeIndex::DateTime:
            return DB::Field(field.get<UInt64>() + 1);

        case DB::TypeIndex::String: {
            std::string value = field.get<std::string>();
            if (value.back() != '\xff') {
                value.back() = static_cast<unsigned char>(value.back()) + 1;
            } else {
                value.push_back('\0');
            }
            return DB::Field(std::move(value));
        }

        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            // Not implemented yet.
            return std::nullopt;

        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %v", dataType->getName());
    }
}

////////////////////////////////////////////////////////////////////////////////

TQuerySettingsPtr ParseCustomSettings(
    const TQuerySettingsPtr baseSettings,
    const DB::Settings::Range& customSettings,
    const TLogger& logger)
{
    const auto& Logger = logger;

    auto node = ConvertToNode(baseSettings);
    for (const auto& setting : customSettings) {
        auto settingName = TString(setting.getName());
        YT_VERIFY(settingName.StartsWith("chyt"));
        if (!settingName.StartsWith("chyt.") && !settingName.StartsWith("chyt_")) {
            THROW_ERROR_EXCEPTION(
                "Invalid setting name %Qv; CHYT settings should start with \"chyt.\" prefix",
                settingName);
        }

        TYPath ypath = "/" + settingName.substr(/*strlen("chyt.")*/ 5);
        for (auto& character : ypath) {
            if (character == '.') {
                character = '/';
            }
        }

        auto field = setting.getValue();
        YT_LOG_TRACE("Parsing custom setting (YPath: %v, FieldValue: %v)", ypath, field.dump());

        auto modifiedNode = FindNodeByYPath(node, ypath);

        INodePtr patchNode;
        if (modifiedNode && modifiedNode->GetType() != ENodeType::String && field.getType() == DB::Field::Types::Which::String) {
            // All settings provided via http interface have a 'string' type.
            // To overcome this limitation, try to parse it as a YsonString if not a string value is expected.
            const auto& stringVal = field.get<std::string>();
            patchNode = ConvertToNode(TYsonStringBuf(stringVal));
        } else {
            TUnversionedValue unversionedValue;
            unversionedValue.Id = 0;
            ToUnversionedValue(field, &unversionedValue);
            patchNode = ConvertToNode(unversionedValue);
        }

        YT_LOG_TRACE("Patch node (Node: %v)", ConvertToYsonString(patchNode, EYsonFormat::Text));
        SetNodeByYPath(node, ypath, patchNode);
    }

    YT_LOG_TRACE("Resulting node (Node: %v)", ConvertToYsonString(node, EYsonFormat::Text));

    auto result = New<TQuerySettings>();
    result->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    result->Load(node);

    YT_LOG_DEBUG(
        "Custom settings parsed (Settings: %v, Unrecognized: %v)",
        ConvertToYsonString(result, EYsonFormat::Text),
        ConvertToYsonString(result->GetUnrecognizedRecursively(), EYsonFormat::Text));

    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! Leaves only some of the "significant" profile counters.
THashMap<TString, size_t> GetBriefProfileCounters(const ProfileEvents::Counters& profileCounters)
{
    static const std::vector<ProfileEvents::Event> SignificantEvents = {
        ProfileEvents::Query,
        ProfileEvents::SelectQuery,
        ProfileEvents::InsertQuery,
        ProfileEvents::InsertedRows,
        ProfileEvents::InsertedBytes,
        ProfileEvents::ContextLock,
        ProfileEvents::RealTimeMicroseconds,
        ProfileEvents::UserTimeMicroseconds,
        ProfileEvents::SystemTimeMicroseconds,
        ProfileEvents::SoftPageFaults,
        ProfileEvents::HardPageFaults,
        ProfileEvents::OSIOWaitMicroseconds,
        ProfileEvents::OSCPUWaitMicroseconds,
        ProfileEvents::OSCPUVirtualTimeMicroseconds,
        ProfileEvents::OSReadChars,
        ProfileEvents::OSWriteChars,
        ProfileEvents::OSReadBytes,
        ProfileEvents::OSWriteBytes,
    };

    THashMap<TString, size_t> result;

    for (const auto& event : SignificantEvents) {
        result[CamelCaseToUnderscoreCase(ProfileEvents::getName(event))] = profileCounters[event].load(std::memory_order_relaxed);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

int GetQueryProcessingStageRank(DB::QueryProcessingStage::Enum stage)
{
    switch (stage) {
        case DB::QueryProcessingStage::FetchColumns:
            return 0;
        case DB::QueryProcessingStage::WithMergeableState:
            return 1;
        case DB::QueryProcessingStage::WithMergeableStateAfterAggregation:
        case DB::QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit:
            return 2;
        case DB::QueryProcessingStage::Complete:
            return 3;

        default:
            THROW_ERROR_EXCEPTION("Unexpected query processing stage (Stage: %v)",
                toString(stage));
    }
}

int GetDistributedInsertStageRank(EDistributedInsertStage stage)
{
    switch (stage) {
        case EDistributedInsertStage::WithMergeableState:
            return 1;
        case EDistributedInsertStage::AfterAggregation:
            return 2;
        case EDistributedInsertStage::Complete:
            return 3;
        case EDistributedInsertStage::None:
            return 4;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

namespace DB {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const NameSet& nameSet)
{
    return NYT::Format("%v", std::vector<TString>(nameSet.begin(), nameSet.end()));
}

void Serialize(const QueryStatusInfo& query, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("query").Value(NYT::NClickHouseServer::MaybeTruncateSubquery(TString(query.query)))
            .Item("elapsed_seconds").Value(query.elapsed_seconds)
            .Item("read_rows").Value(query.read_rows)
            .Item("read_bytes").Value(query.read_bytes)
            .Item("total_rows").Value(query.total_rows)
            .Item("written_rows").Value(query.written_rows)
            .Item("written_bytes").Value(query.written_bytes)
            .Item("memory_usage").Value(query.memory_usage)
            .Item("peak_memory_usage").Value(query.peak_memory_usage)
        .EndMap();
}

void Serialize(const ProcessListForUserInfo& processListForUserInfo, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("memory_usage").Value(processListForUserInfo.memory_usage)
            .Item("peak_memory_usage").Value(processListForUserInfo.peak_memory_usage)
            .Item("brief_profile_counters").Value(NYT::NClickHouseServer::GetBriefProfileCounters(*processListForUserInfo.profile_counters))
        .EndMap();
}

TString ToString(const Field& field)
{
    return EscapeC(TString(field.dump()));
}

TString ToString(const Block& block)
{
    NYT::TStringBuilder content;
    const auto& columns = block.getColumns();
    content.AppendChar('{');
    for (size_t rowIndex = 0; rowIndex < block.rows(); ++rowIndex) {
        if (rowIndex != 0) {
            content.AppendString(", ");
        }
        content.AppendChar('{');
        for (size_t columnIndex = 0; columnIndex < block.columns(); ++columnIndex) {
            if (columnIndex != 0) {
                content.AppendString(", ");
            }
            const auto& field = (*columns[columnIndex])[rowIndex];
            content.AppendString(applyVisitor(FieldVisitorToString(), field));
        }
        content.AppendChar('}');
    }
    content.AppendChar('}');

    return NYT::Format(
        "{RowCount: %v, ColumnCount: %v, Structure: {%v}, Content: %v}",
        block.rows(),
        block.columns(),
        block.dumpStructure(),
        content.Flush());
}

void PrintTo(const Field& field, std::ostream* os)
{
    *os << ToString(field);
}


////////////////////////////////////////////////////////////////////////////////

} // namespace DB
