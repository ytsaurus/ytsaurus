#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <util/digest/sequence.h>

#include <string>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TColumnName>
class TGenericColumnFilterDictionary
{
public:
    using TColumnNames = std::vector<TColumnName>;

public:
    TGenericColumnFilterDictionary(bool sortColumns = true);

    int GetIdOrRegisterAdmittedColumns(TColumnNames admittedColumns);

    const TColumnNames& GetAdmittedColumns(int id) const;

private:
    const bool SortColumns_;

    THashMap<TColumnNames, int, TRangeHash<>> AdmittedColumnsToId_;
    std::vector<TColumnNames> IdToAdmittedColumns_;

    template <typename T>
    friend void ToProto(NProto::TColumnFilterDictionary* protoDictionary, const TGenericColumnFilterDictionary<T>& dictionary);

    template <typename T>
    friend void FromProto(TGenericColumnFilterDictionary<T>* dictionary, const NProto::TColumnFilterDictionary& protoDictionary);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
