#include <library/cpp/threading/future/core/future.h>

namespace NYql {

class ITableDataService {
public:
    virtual ~ITableDataService() = default;

    virtual NThreading::TFuture<void> Put(const TString& id, const TString& data) = 0;

    virtual NThreading::TFuture<TMaybe<TString>> Get(const TString& id) = 0;

    virtual NThreading::TFuture<void> Delete(const TString& id) = 0;
};

} // namspace NYql
