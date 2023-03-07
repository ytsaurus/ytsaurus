#pragma once

#include "public.h"

#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <util/digest/sequence.h>

#include <string>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnFilterDictionary
{
public:
    TColumnFilterDictionary(bool sortColumns = true);

    int GetIdOrRegisterAdmittedColumns(std::vector<TString> admittedColumns);

    const std::vector<TString>& GetAdmittedColumns(int id) const;

private:
    const bool SortColumns_;

    THashMap<std::vector<TString>, int, TRangeHash<>> AdmittedColumnsToId_;
    std::vector<std::vector<TString>> IdToAdmittedColumns_;

    friend void ToProto(NProto::TColumnFilterDictionary* protoDictionary, const TColumnFilterDictionary& dictionary);
    friend void FromProto(TColumnFilterDictionary* dictionary, const NProto::TColumnFilterDictionary& protoDictionary);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
