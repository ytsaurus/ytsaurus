#include "ranking.h"

#include "frequency.h"

#include <yql/essentials/sql/v1/complete/name/name_service.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    const TStringBuf ContentView(const TGenericName& name Y_LIFETIME_BOUND) {
        return std::visit([](const auto& name) -> TStringBuf {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_base_of_v<TIndentifier, T>) {
                return name.Indentifier;
            }
        }, name);
    }

    size_t ReversedWeight(size_t weight) {
        return std::numeric_limits<size_t>::max() - weight;
    }

    class TRanking: public IRanking {
    public:
        TRanking(TFrequencyData frequency)
            : Frequency_(std::move(frequency))
        {
        }

        void PartialSort(TVector<TGenericName>& names, size_t limit) override {
            limit = std::min(limit, names.size());

            ::PartialSort(
                std::begin(names), std::begin(names) + limit, std::end(names),
                [this](const TGenericName& lhs, const TGenericName& rhs) {
                    const size_t lhs_weight = ReversedWeight(Weight(lhs));
                    const auto lhs_content = ContentView(lhs);

                    const size_t rhs_weight = ReversedWeight(Weight(rhs));
                    const auto rhs_content = ContentView(rhs);

                    return std::tie(lhs_weight, lhs_content) <
                           std::tie(rhs_weight, rhs_content);
                });
        }

    private:
        size_t Weight(const TGenericName& name) const {
            return std::visit([this](const auto& name) -> size_t {
                using T = std::decay_t<decltype(name)>;

                auto identifier = ToLowerUTF8(name.Indentifier);

                if constexpr (std::is_same_v<T, TFunctionName>) {
                    if (auto weight = Frequency_.Functions.FindPtr(identifier)) {
                        return *weight;
                    }
                }

                if constexpr (std::is_same_v<T, TTypeName>) {
                    if (auto weight = Frequency_.Types.FindPtr(identifier)) {
                        return *weight;
                    }
                }

                return 0;
            }, name);
        }

        TFrequencyData Frequency_;
    };

    IRanking::TPtr MakeDefaultRanking(TFrequencyData frequency) {
        return IRanking::TPtr(new TRanking(frequency));
    }

} // namespace NSQLComplete
