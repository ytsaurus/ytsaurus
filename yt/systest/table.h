#pragma once

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/systest/proto/table.pb.h>

namespace NYT::NTest {

struct TDataColumn {
    TString Name;
};

struct TTable {
    std::vector<TDataColumn> DataColumns;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataColumn* proto, const TDataColumn& column);
void FromProto(TDataColumn* column, const NProto::TDataColumn& proto);

void ToProto(NProto::TTable* proto, const TTable& table);
void FromProto(TTable* table, const NProto::TTable& proto);


}  // namespace NYT::NTest
