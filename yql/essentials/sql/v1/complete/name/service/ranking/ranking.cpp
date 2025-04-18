#include "ranking.h"

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

#include <util/charset/utf8.h>
#include <util/string/cast.h>

namespace NSQLComplete {

    class TRanking: public IRanking {
    private:
        struct TRow {
            TGenericName Name;
            size_t Weight;
            TString Content;
        };

    public:
        TRanking(TFrequencyData frequency)
            : Frequency_(std::move(frequency))
        {
        }

        void CropToSortedPrefix(TVector<TGenericName>& names, size_t limit) override {
            limit = std::min(limit, names.size());

            TVector<TRow> rows;
            rows.reserve(names.size());
            for (TGenericName& name : names) {
                size_t weight = Weight(name);
                TString content = ToString(name);
                rows.emplace_back(std::move(name), weight, std::move(content));
            }

            ::PartialSort(
                std::begin(rows), std::begin(rows) + limit, std::end(rows),
                [](const TRow& lhs, const TRow& rhs) {
                    const size_t lhs_weight = ReversedWeight(lhs.Weight);
                    const auto lhs_content = lhs.Content;

                    const size_t rhs_weight = ReversedWeight(rhs.Weight);
                    const auto rhs_content = rhs.Content;

                    return std::tie(lhs_weight, lhs_content) <
                           std::tie(rhs_weight, rhs_content);
                });

            names.crop(limit);
            rows.crop(limit);

            for (size_t i = 0; i < limit; ++i) {
                names[i] = std::move(rows[i].Name);
            }
        }

    private:
        size_t Weight(const TGenericName& name) const {
            return std::visit([this](const auto& name) -> size_t {
                using T = std::decay_t<decltype(name)>;

                auto content = ToLowerUTF8(ToString(name));

                if constexpr (std::is_same_v<T, TKeyword>) {
                    if (auto weight = Frequency_.Keywords.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TPragmaName>) {
                    const THashMap<TString, size_t>* group;
                    const size_t* weight;
                    if (
                        (group = Frequency_.PragmasBySpace.FindPtr(ToLowerUTF8(name.Namespace))) &&
                        (weight = group->FindPtr(ToLowerUTF8(name.Indentifier)))) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TFunctionName>) {
                    const THashMap<TString, size_t>* group;
                    const size_t* weight;
                    if (
                        (group = Frequency_.FunctionsBySpace.FindPtr(ToLowerUTF8(name.Namespace))) &&
                        (weight = group->FindPtr(ToLowerUTF8(name.Indentifier)))) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TTypeName>) {
                    if (auto weight = Frequency_.Types.FindPtr(content)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, THintName>) {
                    if (auto weight = Frequency_.Hints.FindPtr(content)) {
                        return *weight;
                    }
                }

                return 0;
            }, name);
        }

        static size_t ReversedWeight(size_t weight) {
            return std::numeric_limits<size_t>::max() - weight;
        }

        TFrequencyData Frequency_;
    };

    IRanking::TPtr MakeDefaultRanking() {
        return IRanking::TPtr(new TRanking(LoadFrequencyData()));
    }

    IRanking::TPtr MakeDefaultRanking(TFrequencyData frequency) {
        return IRanking::TPtr(new TRanking(frequency));
    }

} // namespace NSQLComplete
