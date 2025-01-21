#include <library/cpp/threading/future/future.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>

namespace NYql {

class TLocalTableDataService: public ITableDataService {
public:
    TLocalTableDataService(ui32 numParts);

    NThreading::TFuture<void> Put(const TString& key, const TString& value) override;

    NThreading::TFuture<TMaybe<TString>> Get(const TString& key) override;

    NThreading::TFuture<void> Delete(const TString& key) override;

private:
    std::vector<std::unordered_map<TString, TString>> Data_;
    const ui32 NumParts_;
};

} // namspace NYql
