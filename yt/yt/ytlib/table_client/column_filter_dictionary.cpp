#include "column_filter_dictionary.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/digest/multi.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TColumnName>
TGenericColumnFilterDictionary<TColumnName>::TGenericColumnFilterDictionary(bool sortColumns)
    : SortColumns_(sortColumns)
{ }

template <typename TColumnName>
int TGenericColumnFilterDictionary<TColumnName>::GetIdOrRegisterAdmittedColumns(std::vector<TColumnName> admittedColumns)
{
    if (SortColumns_) {
        std::sort(admittedColumns.begin(), admittedColumns.end());
    }
    auto admittedColumnsIterator = AdmittedColumnsToId_.find(admittedColumns);
    if (admittedColumnsIterator == AdmittedColumnsToId_.end()) {
        int id = IdToAdmittedColumns_.size();
        IdToAdmittedColumns_.push_back(admittedColumns);
        admittedColumnsIterator = AdmittedColumnsToId_.emplace(admittedColumns, id).first;
    }
    return admittedColumnsIterator->second;
}

template <typename TColumnName>
const std::vector<TColumnName>& TGenericColumnFilterDictionary<TColumnName>::GetAdmittedColumns(int id) const
{
    return IdToAdmittedColumns_[id];
}

////////////////////////////////////////////////////////////////////////////////

template <typename TColumnName>
void ToProto(NProto::TColumnFilterDictionary* protoDictionary, const TGenericColumnFilterDictionary<TColumnName>& dictionary)
{
    using NYT::ToProto;

    for (const auto& admittedColumns : dictionary.IdToAdmittedColumns_) {
        auto* protoColumnFilter = protoDictionary->add_column_filters();
        ToProto(protoColumnFilter->mutable_admitted_names(), admittedColumns);
    }
}

template <typename TColumnName>
void FromProto(TGenericColumnFilterDictionary<TColumnName>* dictionary, const NProto::TColumnFilterDictionary& protoDictionary)
{
    using NYT::FromProto;

    for (const auto& columnFilter : protoDictionary.column_filters()) {
        dictionary->GetIdOrRegisterAdmittedColumns(FromProto<std::vector<TColumnName>>(columnFilter.admitted_names()));
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TGenericColumnFilterDictionary<TString>;

template
void ToProto(NProto::TColumnFilterDictionary* protoDictionary, const TGenericColumnFilterDictionary<TString>& dictionary);

template
void FromProto(TGenericColumnFilterDictionary<TString>* dictionary, const NProto::TColumnFilterDictionary& protoDictionary);

template class TGenericColumnFilterDictionary<TColumnStableName>;

template
void ToProto(NProto::TColumnFilterDictionary* protoDictionary, const TGenericColumnFilterDictionary<TColumnStableName>& dictionary);

template
void FromProto(TGenericColumnFilterDictionary<TColumnStableName>* dictionary, const NProto::TColumnFilterDictionary& protoDictionary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
