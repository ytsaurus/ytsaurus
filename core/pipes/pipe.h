#pragma once

#include "public.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TPipe
    : public TNonCopyable
{
public:
    static const int InvalidFD = -1;

    TPipe();
    TPipe(TPipe&& pipe);
    ~TPipe();

    void operator=(TPipe&& other);

    void CloseReadFD();
    void CloseWriteFD();

    TAsyncReaderPtr CreateAsyncReader();
    TAsyncWriterPtr CreateAsyncWriter();

    int ReleaseReadFD();
    int ReleaseWriteFD();

    int GetReadFD() const;
    int GetWriteFD() const;

private:
    int ReadFD_ = InvalidFD;
    int WriteFD_ = InvalidFD;

    TPipe(int fd[2]);
    void Init(TPipe&& other);

    friend class TPipeFactory;
};

Stroka ToString(const TPipe& pipe);

////////////////////////////////////////////////////////////////////////////////

class TPipeFactory
{
public:
    explicit TPipeFactory(int minFD = 0);
    ~TPipeFactory();

    TPipe Create();

    void Clear();

private:
    const int MinFD_;
    std::vector<int> ReservedFDs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
