#pragma once
#ifndef RESOURCE_TREE_ELEMENT_INL_H_
#error "Direct inclusion of this file is not allowed, include resource_tree_element.h"
// For the sake of sane code completion.
#include "resource_tree_element.h"
#endif

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobMetrics TResourceTreeElement::GetJobMetrics()
{
    NConcurrency::TReaderGuard guard(JobMetricsLock_);

    return JobMetrics_;
}

bool TResourceTreeElement::GetAlive() const
{
    return Alive_.load(std::memory_order_relaxed);
}

void TResourceTreeElement::SetNonAlive()
{
    // We need a barrier to be sure that nobody tries to change some usages.
    NConcurrency::TWriterGuard guard(ResourceUsageLock_);

    Alive_ = false;
}

TResourceVector TResourceTreeElement::GetFairShare() const
{
    return FairShare_.Load();
}

void TResourceTreeElement::SetFairShare(TResourceVector fairShare)
{
    FairShare_.Store(fairShare);
}

double TResourceTreeElement::GetFairShareRatio() const
{
    return MaxComponent(FairShare_.Load());
}

inline void TResourceTreeElement::SetFairShareRatio(double fairShare)
{
    FairShare_.Store(TResourceVector::FromDouble(fairShare));
}

const TString& TResourceTreeElement::GetId()
{
    return Id_;
}

void TResourceTreeElement::ApplyLocalJobMetricsDelta(const TJobMetrics& delta)
{
    NConcurrency::TWriterGuard guard(JobMetricsLock_);

    JobMetrics_ += delta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
