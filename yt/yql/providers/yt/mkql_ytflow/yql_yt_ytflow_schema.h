#pragma once

#include <yt/yt/client/table_client/public.h>

#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>


namespace NKikimr::NMiniKQL {

class TType;
class TTypeEnvironment;

} // namespace NKikimr::NMiniKQL


namespace NYql {

NYT::NTableClient::TLogicalTypePtr ConvertType(const NKikimr::NMiniKQL::TType* type);

NYT::NTableClient::TLogicalTypePtr FilterFields(
    NYT::NTableClient::TLogicalTypePtr structType,
    TVector<TString> filter);

const NKikimr::NMiniKQL::TType* FilterFields(
    const NKikimr::NMiniKQL::TType* structType,
    TVector<TString> filter,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv);

NYT::NTableClient::TLogicalTypePtr PartiallyReorderFields(
    NYT::NTableClient::TLogicalTypePtr structType,
    TVector<TString> partialOrder);

NYT::NTableClient::TLogicalTypePtr RenameFields(
    NYT::NTableClient::TLogicalTypePtr structType,
    THashMap<TString, TString> renames);

NYT::NTableClient::TTableSchemaPtr BuildTableSchema(
    NYT::NTableClient::TLogicalTypePtr structType);

bool EqualTo(
    NYT::NTableClient::TNameTablePtr one,
    NYT::NTableClient::TNameTablePtr another);

} // namespace NYql
