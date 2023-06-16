
#include <yt/systest/table.h>

namespace NYT::NTest {

void ToProto(NProto::TDataColumn* proto, const TDataColumn& column)
{
    proto->set_name(column.Name);
}

void FromProto(TDataColumn* column, const NProto::TDataColumn& proto)
{
    column->Name = proto.name();
}

void FromProto(TTable* table, const NProto::TTable& proto)
{
  table->DataColumns.clear();
  for (const auto& protoColumn : proto.columns()) {
    TDataColumn column;
    FromProto(&column, protoColumn);
    table->DataColumns.push_back(std::move(column));
  }
}

void ToProto(NProto::TTable* proto, const TTable &table)
{
    for (const auto& column : table.DataColumns) {
        ToProto(proto->add_columns(), column);
    }
}

}  // namespace NYT::NTest
