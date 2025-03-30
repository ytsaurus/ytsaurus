#pragma once

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/name_service.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

    class TRanking {
    public:
        explicit TRanking(TFrequencyData frequency);
        void PartialSort(TVector<TGenericName>& names, size_t limit);

    private:
        size_t Weight(const TGenericName& name) const;

        TFrequencyData Frequency_;
    };

    TRanking MakeDefaultRanking();

} // namespace NSQLComplete
