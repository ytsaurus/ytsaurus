#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif
#undef HELPERS_INL_H_

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//template <class TTypedResponse>
//TIntrusivePtr<TTypedResponse> TMultiCellBatchResponse::GetResponse(int index) const
//{
//    int batchNumber = ResponseIndex_[index].first;
//    int batchIndex = ResponseIndex_[index].second;
//    return BatchResponses_[batchNumber]->GetResponse<TTypedResponse>(batchIndex);
//}
//
//template <class TTypedResponse>
//TIntrusivePtr<TTypedResponse> TMultiCellBatchResponse::FindResponse(const Stroka& key) const
//{
//    for (const auto& batchRsp : BatchResponses_) {
//        if (!batchRsp) {
//            continue;
//        }
//        auto rsp = batchRsp->FindResponse<TTypedResponse>(key);
//        if (rsp) {
//            return rsp;
//        }
//    }
//    return nullptr;
//}
//
//template <class TTypedResponse>
//TIntrusivePtr<TTypedResponse> TMultiCellBatchResponse::GetResponse(const Stroka& key) const
//{
//    auto rsp = FindResponse<TTypedResponse>(key);
//    YCHECK(rsp->IsOK());
//    return rsp;
//}
//
//template <class TTypedResponse>
//std::vector< TIntrusivePtr<TTypedResponse> > TMultiCellBatchResponse::GetResponses(const Stroka& key) const
//{
//    std::vector<TIntrusivePtr<TTypedResponse>> responses;
//    for (const auto& batchRsp : BatchResponses_) {
//        for (const auto& rsp : batchRsp->GetResponses<TTypedResponse>(key)) {
//            responses.push_back(rsp);
//        }
//    }
//    return responses;
//}
//
//template <class TTypedResponse>
//TNullable<std::vector<TIntrusivePtr<TTypedResponse>>> TMultiCellBatchResponse::FindResponses(const Stroka& key) const
//{
//    std::vector<TIntrusivePtr<TTypedResponse>> responses;
//    if (key.empty()) {
//        responses.reserve(GetSize());
//        for (int index = 0; index < GetSize(); ++index) {
//            auto rsp = GetResponse<TTypedResponse>(index);
//            if (!rsp) {
//                return Null;
//            }
//            responses.push_back(rsp);
//        }
//    } else {
//        auto range = KeyToIndexes_.equal_range(key);
//        for (auto it = range.first; it != range.second; ++it) {
//            auto rsp = GetResponse<TTypedResponse>(it->second);
//            if (!rsp) {
//                return Null;
//            }
//            responses.push_back(rsp);
//        }
//    }
//    return responses;
//}

////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode)
{
    auto spec = New<TSpec>();
    try {
        spec->Load(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
