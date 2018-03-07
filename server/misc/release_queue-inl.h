#ifndef RELEASE_QUEUE_H_
#error "Direct inclusion of this file is not allowed, include release_queue.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TReleaseQueue<T>::Push(T&& value)
{
    Queue_.push(std::move(value));
}

template <class T>
void TReleaseQueue<T>::Push(const T& value)
{
    Queue_.push(value);
}

template <class T>
typename TReleaseQueue<T>::TCookie TReleaseQueue<T>::Checkpoint() const
{
    return HeadCookie_ + Queue_.size();
}

template<class T>
std::vector<T> TReleaseQueue<T>::Release(TCookie limit)
{
    auto count = ClampVal<i64>(limit - HeadCookie_, 0, Queue_.size());
    std::vector<T> released;
    released.reserve(count);

    while (HeadCookie_ < limit && !Queue_.empty()) {
        released.emplace_back(std::move(Queue_.front()));
        Queue_.pop();
        ++HeadCookie_;
    }

    YCHECK(released.size() == count);
    YCHECK(HeadCookie_ >= limit || Queue_.empty());

    return released;
}

template <class T>
typename TReleaseQueue<T>::TCookie TReleaseQueue<T>::GetHeadCookie() const
{
    return HeadCookie_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
