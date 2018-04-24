#pragma once

#include <mapreduce/yt/interface/fwd.h>
#include <mapreduce/yt/interface/common.h>

#include <mapreduce/yt/skiff/public.h>
#include <mapreduce/yt/skiff/wire_type.h>
#include <mapreduce/yt/skiff/skiff_schema.h>

#include <util/generic/vector.h>

namespace NYT {

struct IYsonConsumer;
struct TAuth;
enum class ENodeReaderFormat : int;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TCreateSkiffSchemaOptions
{
    using TSelf = TCreateSkiffSchemaOptions;

    FLUENT_FIELD_DEFAULT(bool, HasKeySwitch, true);
    FLUENT_FIELD_DEFAULT(bool, HasRangeIndex, false);
    FLUENT_FIELD_DEFAULT(bool, HasRowIndex, false);
};

////////////////////////////////////////////////////////////////////////////////

NSkiff::TSkiffSchemaPtr GetJobInputSkiffSchema();

NSkiff::EWireType ValueTypeToSkiffType(EValueType valueType);

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TTableSchema& schema,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

NSkiff::TSkiffSchemaPtr CreateSkiffSchema(
    const TNode& schemaNode,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

void Serialize(const NSkiff::TSkiffSchemaPtr& schema, IYsonConsumer* consumer);

void Deserialize(NSkiff::TSkiffSchemaPtr& schema, const TNode& node);

TFormat CreateSkiffFormat(const NSkiff::TSkiffSchemaPtr& schema);

NSkiff::TSkiffSchemaPtr CreateSkiffSchemaIfNecessary(
    ENodeReaderFormat nodeReaderFormat,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& tablePaths,
    const TCreateSkiffSchemaOptions& options = TCreateSkiffSchemaOptions());

} // namespace NDetail
} // namespace NYT
