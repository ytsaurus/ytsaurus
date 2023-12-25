#ifndef COMPACTION_HINT_FETCHER_INL_H_
#error "Direct inclusion of this file is not allowed, include compaction_hint_fetcher.h"
// For the sake of sane code completion.
#include "compaction_hint_fetcher.h"
#endif

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
void TCompactionHintRequestWithResult<TResult>::SetRequestStatus(ECompactionHintRequestStatus status)
{
    Data_ = status;
}

template <class TResult>
void TCompactionHintRequestWithResult<TResult>::SetResult(TResult result)
{
    Data_ = std::move(result);
}

template <class TResult>
bool TCompactionHintRequestWithResult<TResult>::IsRequestStatus() const
{
    return Data_.index() == 0;
}

template <class TResult>
bool TCompactionHintRequestWithResult<TResult>::IsCertainRequestStatus(ECompactionHintRequestStatus status) const
{
    return IsRequestStatus() && AsRequestStatus() == status;
}

template <class TResult>
ECompactionHintRequestStatus TCompactionHintRequestWithResult<TResult>::AsRequestStatus() const
{
    return std::get<ECompactionHintRequestStatus>(Data_);
}

template <class TResult>
const TResult& TCompactionHintRequestWithResult<TResult>::AsResult() const
{
    return std::get<TResult>(Data_);
}

template <class TResult>
void Serialize(
    const TCompactionHintRequestWithResult<TResult>& hint,
    NYson::IYsonConsumer* consumer)
{
    auto fluent = NYTree::BuildYsonFluently(consumer);
    if (hint.IsRequestStatus()) {
        fluent.Value(hint.AsRequestStatus());
    } else {
        fluent.Value(hint.AsResult());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TProfiledQueue<T>::TProfiledQueue(NProfiling::TGauge sizeCounter)
    : SizeCounter_(std::move(sizeCounter))
{ }

template <class T>
void TProfiledQueue<T>::Push(T value)
{
    Queue_.push(std::move(value));
    UpdateSize();
}

template <class T>
template <class ...Args>
void TProfiledQueue<T>::Emplace(Args&& ...args)
{
    Queue_.emplace(std::forward<Args>(args)...);
    UpdateSize();
}

template <class T>
void TProfiledQueue<T>::Pop()
{
    Queue_.pop();
    UpdateSize();
}

template <class T>
T TProfiledQueue<T>::PopAndGet()
{
    T value = Queue_.front();
    Queue_.pop();
    UpdateSize();

    return value;
}

template <class T>
i64 TProfiledQueue<T>::Size() const
{
    return Queue_.size();
}

template <class T>
bool TProfiledQueue<T>::Empty() const
{
    return Queue_.empty();
}

template <class T>
void TProfiledQueue<T>::UpdateSize()
{
    SizeCounter_.Update(Queue_.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
