#ifndef FILE_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include file_helpers.h"
#endif
#undef FILE_HELPERS_INL_H_

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TCheckedReader<T>::TCheckedReader(T& underlying)
    : Underlying_(underlying)
    , CurrentOffset_(underlying.GetPosition())
    , FileLength_(underlying.GetLength())
    , Success_(true)
{ }

template <class T>
size_t TCheckedReader<T>::Load(void* buffer, size_t length)
{
    if (!Check(length)) {
        return 0;
    }

    size_t bytesRead = Underlying_.Load(buffer, length);
    CurrentOffset_ += bytesRead;
    return bytesRead;
}

template <class T>
void TCheckedReader<T>::Skip(size_t length)
{
    if (!Check(length))
        return;

    Underlying_.Skip(length);
    CurrentOffset_ += length;
}

template <class T>
bool TCheckedReader<T>::Success() const
{
    return Success_;
}

template <class T>
size_t TCheckedReader<T>::Avail() const
{
    return FileLength_ - CurrentOffset_;
}

template <class T>
bool TCheckedReader<T>::Check(size_t length)
{
    if (!Success_) {
        return false;
    }

    if (CurrentOffset_ + length > FileLength_) {
        Success_ = false;
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
