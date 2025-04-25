#pragma once

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/name_service.h>

#include <util/generic/hash.h>

namespace NSQLComplete {

    class IRanking {
    public:
        using TPtr = TAtomicSharedPtr<IRanking>;

        virtual void CropToSortedPrefix(TVector<TGenericName>& names, size_t limit) const = 0;
        virtual ~IRanking() = default;
    };

    THolder<IRanking> MakeDefaultRanking();

    THolder<IRanking> MakeDefaultRanking(TFrequencyData frequency);

} // namespace NSQLComplete
