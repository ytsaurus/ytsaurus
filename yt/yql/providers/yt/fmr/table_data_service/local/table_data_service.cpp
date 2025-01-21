#include "table_data_service.h"

namespace NYql {

TLocalTableDataService::TLocalTableDataService(ui32 numParts): NumParts_(numParts) {
    Data_.resize(NumParts_);
};

NThreading::TFuture<void> TLocalTableDataService::Put(const TString& key, const TString& value) {
    auto& map = Data_[std::hash<TString>()(key) % NumParts_];
    auto it = map.find(key);
    if (it != map.end()) {
        return NThreading::MakeFuture();
    }
    map.insert({key, value});
    return NThreading::MakeFuture();
}

NThreading::TFuture<TMaybe<TString>> TLocalTableDataService::Get(const TString& key) {
    TMaybe<TString> value = Nothing();
    auto& map = Data_[std::hash<TString>()(key) % NumParts_];
    auto it = map.find(key);
    if (it != map.end()) {
        value = it->second;
    }
    return NThreading::MakeFuture(value);
}

NThreading::TFuture<void> TLocalTableDataService::Delete(const TString& key) {
    auto& map = Data_[std::hash<TString>()(key) % NumParts_];
    auto it = map.find(key);
    if (it == map.end()) {
        return NThreading::MakeFuture();
    }
    map.erase(key);
    return NThreading::MakeFuture();
}

} // namspace NYql
