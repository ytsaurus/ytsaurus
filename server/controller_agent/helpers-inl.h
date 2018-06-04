#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
#endif

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::INodePtr specNode)
{
    auto spec = New<TSpec>();
    try {
        spec->Load(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TAvgSummary<T>::TAvgSummary()
    : TAvgSummary(T(), 0)
{ }

template <class T>
TAvgSummary<T>::TAvgSummary(T sum, i64 count)
    : Sum_(sum)
    , Count_(count)
    , Avg_(CalcAvg())
{ }

template <class T>
TNullable<T> TAvgSummary<T>::CalcAvg()
{
    return Count_ == 0 ? TNullable<T>() : Sum_ / Count_;
}

template <class T>
void TAvgSummary<T>::AddSample(T sample)
{
    Sum_ += sample;
    ++Count_;
    Avg_ = CalcAvg();
}

template <class T>
void TAvgSummary<T>::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Sum_);
    Persist(context, Count_);
    if (context.IsLoad()) {
        Avg_ = CalcAvg();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
