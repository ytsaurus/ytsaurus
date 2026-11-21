#include "secondary_index.h"

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTableServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityServer;
using namespace NTableServer;
using namespace NTabletClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

struct TUnfoldedColumnsCompat
{
    std::string TableColumn;
    std::string IndexColumn;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TUnfoldedColumnsCompat, 0x0de599a9)
    {
        PHOENIX_REGISTER_FIELD(1, TableColumn);
        PHOENIX_REGISTER_FIELD(2, IndexColumn);
    }
};

PHOENIX_DEFINE_TYPE(TUnfoldedColumnsCompat);

////////////////////////////////////////////////////////////////////////////////

std::string TSecondaryIndex::GetLowercaseObjectName() const
{
    return Format("secondary index %v", GetId());
}

std::string TSecondaryIndex::GetCapitalizedObjectName() const
{
    return Format("Secondary index %v", GetId());
}

void TSecondaryIndex::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TObject>();

    PHOENIX_REGISTER_FIELD(1, TableId_);
    PHOENIX_REGISTER_FIELD(2, IndexTableId_);
    PHOENIX_REGISTER_FIELD(3, Kind_);
    // COMPAT(sabdenovch)
    registrar.template VirtualField<4>("ExternalCellTags_", [] (TThis* this_, auto& context) {
        if (context.GetVersion() >= EMasterReign::SequoiaSecondaryIndices) {
            NYT::Load(context, this_->ExternalCellTags_);
        } else if (auto cellTag = NYT::Load<TCellTag>(context); cellTag != NotReplicatedCellTagSentinel) {
            this_->ExternalCellTags_ = {cellTag};
        }
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->ExternalCellTags_);
    })();
    PHOENIX_REGISTER_FIELD(5, Predicate_);

    // COMPAT(sabdenovch)
    registrar.template VirtualField<6>("UnfoldedColumns_", [] (TThis* this_, auto& context) {
        if (context.GetVersion() >= EMasterReign::SecondaryIndexUnfoldedColumnsSerializationFix) {
            NYT::Load(context, this_->UnfoldedColumns_);
        } else if (context.GetVersion() >= EMasterReign::SecondaryIndexUnfoldedNames) {
            if (auto unfoldedColumnsCompat = NYT::Load<std::optional<TUnfoldedColumnsCompat>>(context)) {
                this_->UnfoldedColumns_ = TUnfoldedColumns(
                    unfoldedColumnsCompat->TableColumn,
                    unfoldedColumnsCompat->IndexColumn);
            }
        } else {
            if (auto unfoldedColumn = NYT::Load<std::optional<std::string>>(context)) {
                this_->UnfoldedColumns_ = TUnfoldedColumns(*unfoldedColumn, *unfoldedColumn);
            }
        }
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->UnfoldedColumns_);
    })();

    PHOENIX_REGISTER_FIELD(7, TableToIndexCorrespondence_);
    PHOENIX_REGISTER_FIELD(8, EvaluatedColumnsSchema_,
        .template Serializer<TNullableIntrusivePtrSerializer<>>());
}

PHOENIX_DEFINE_TYPE(TSecondaryIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
