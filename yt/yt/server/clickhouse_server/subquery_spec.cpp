#include "subquery_spec.h"

#include "schema.h"

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TSubquerySpec* protoSpec, const TSubquerySpec& spec)
{
    using NYT::ToProto;

    ToProto(protoSpec->mutable_data_source_directory(), spec.DataSourceDirectory);

    for (const auto& inputDataSliceDescriptors : spec.DataSliceDescriptors) {
        auto* inputSpec = protoSpec->add_input_specs();
        ToProto(
            inputSpec->mutable_chunk_specs(),
            inputSpec->mutable_chunk_spec_count_per_data_slice(),
            inputDataSliceDescriptors);
    }

    ToProto(protoSpec->mutable_initial_query_id(), spec.InitialQueryId);
    ToProto(protoSpec->mutable_read_schema(), spec.ReadSchema);

    protoSpec->set_membership_hint(spec.MembershipHint.GetData());
    protoSpec->set_subquery_index(spec.SubqueryIndex);
    protoSpec->set_table_index(spec.TableIndex);
    protoSpec->set_initial_query(spec.InitialQuery);
}

void FromProto(TSubquerySpec* spec, const NProto::TSubquerySpec& protoSpec)
{
    using NYT::FromProto;

    FromProto(&spec->DataSourceDirectory, protoSpec.data_source_directory());

    for (const auto& inputSpec : protoSpec.input_specs()) {
        FromProto(
            &spec->DataSliceDescriptors.emplace_back(),
            inputSpec.chunk_specs(),
            inputSpec.chunk_spec_count_per_data_slice());
    }

    FromProto(&spec->InitialQueryId, protoSpec.initial_query_id());
    FromProto(&spec->ReadSchema, protoSpec.read_schema());

    spec->MembershipHint = TYsonString(protoSpec.membership_hint());
    spec->SubqueryIndex = protoSpec.subquery_index();
    spec->TableIndex = protoSpec.table_index();
    spec->InitialQuery = protoSpec.initial_query();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
