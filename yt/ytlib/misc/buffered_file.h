#pragma once

#include "common.h"

#include <util/system/file.h>
#include <util/stream/file.h>

#include <iostream>

namespace NYT {
namespace NMetaState {

//! Wrapper on File and BufferedFileOutput.
class TBufferedFile
{
public:
    TBufferedFile(const Stroka& fName, ui32 oMode):
        File_(fName, oMode),
        FileOutput_(File_)
    { }

    i64 Seek(i64 offset, SeekDir origin)
    {
        FileOutput_.Flush();
        return File_.Seek(offset, origin);
    }

    void Flush()
    {
        FileOutput_.Flush();
        File_.Flush();
    }

    void Append(const void* buf, size_t len)
    {
        // You promise that write at the end of the file.
        FileOutput_.Write(buf, len);
    }
    
    void Write(const void* buf, size_t len)
    {
        FileOutput_.Flush();
        File_.Write(buf, len);
    }

    size_t Pread(void* buf, size_t len, i64 offset)
    {
        FileOutput_.Flush();
        return File_.Pread(buf, len, offset);
    }

    size_t Read(void* buf, size_t len)
    {
        FileOutput_.Flush();
        return File_.Read(buf, len);
    }

    void Skip(size_t len)
    {
        FileOutput_.Flush();
        File_.Seek(len, sCur);
    }

    size_t GetPosition()
    {
        FileOutput_.Flush();
        return File_.GetPosition();
    }
    
    size_t GetLength()
    {
        FileOutput_.Flush();
        return File_.GetLength();
    }
    
    void Resize(size_t len)
    {
        FileOutput_.Flush();
        File_.Resize(len);
    }

    void Close()
    {
        FileOutput_.Flush();
        File_.Close();
    }

private:
    TFile File_;
    TBufferedFileOutput FileOutput_;
};

//! Using this class you promise don't work with file externally.
template<class FileObject>
class TCheckableFileReader
{
public:
    TCheckableFileReader(FileObject& file):
        File_(file),
        CurrentOffset_(file.GetPosition()),
        FileLength_(file.GetLength()),
        Success_(true)
    { }
    
    size_t Read(void* buf, size_t len)
    {
        if (Check(len)) {
            size_t readLength = File_.Read(buf, len);
            CurrentOffset_ += readLength;
            return readLength;
        }
        return 0;
    }
    
    void Skip(size_t len)
    {
        if (Check(len)) {
            File_.Skip(len);
            CurrentOffset_ += len;
        }
    }

    bool Check(size_t len)
    {
        if (!Success_) {
            return false;
        }
        if (CurrentOffset_ + len > FileLength_) {
            Success_ = false;
            return false;
        }
        return true;
    }

    bool Success() const
    {
        return Success_;
    }

private:
    FileObject& File_;
    i64 CurrentOffset_;
    i64 FileLength_;
    bool Success_;
};

template <class FileObject>
TCheckableFileReader<FileObject> makeCheckableReader(FileObject& file) {
    return TCheckableFileReader<FileObject>(file);
}

} // namespace NMetaState
} // namespace NYT
