#pragma once

#include "public.h"

#include <util/system/file.h>
#include <util/stream/file.h>

#include <iostream>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Wrapper on TFile and TBufferedFileOutput.
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

    void Append(const void* buffer, size_t length)
    {
        FileOutput_.Write(buffer, length);
    }
    
    void Write(const void* buffer, size_t length)
    {
        FileOutput_.Flush();
        File_.Write(buffer, length);
    }

    size_t Pread(void* buffer, size_t length, i64 offset)
    {
        FileOutput_.Flush();
        return File_.Pread(buffer, length, offset);
    }

    size_t Read(void* buffer, size_t length)
    {
        FileOutput_.Flush();
        return File_.Read(buffer, length);
    }

    void Skip(size_t length)
    {
        FileOutput_.Flush();
        File_.Seek(length, sCur);
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
    
    void Resize(size_t length)
    {
        FileOutput_.Flush();
        File_.Resize(length);
    }

    void Close()
    {
        FileOutput_.Flush();
        File_.Close();
    }

    TBufferedFileOutput* GetOutputStream()
    {
        return &FileOutput_;
    }

private:
    TFile File_;
    TBufferedFileOutput FileOutput_;
};

//! This class forces you to work within reachable file content.
template <class FileObject>
class TCheckableFileReader
{
public:
    TCheckableFileReader(FileObject& file):
        File_(file),
        CurrentOffset_(file.GetPosition()),
        FileLength_(file.GetLength()),
        Success_(true)
    { }
    
    size_t Read(void* buffer, size_t length)
    {
        if (Check(length)) {
            size_t bytesRead = File_.Read(buffer, length);
            CurrentOffset_ += bytesRead;
            return bytesRead;
        }
        return 0;
    }
    
    void Skip(size_t length)
    {
        if (Check(length)) {
            File_.Skip(length);
            CurrentOffset_ += length;
        }
    }

    bool Check(size_t length)
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
TCheckableFileReader<FileObject> CreateCheckableReader(FileObject& file)
{
    return TCheckableFileReader<FileObject>(file);
}

void CheckedMoveFile(const Stroka& source, const Stroka& destination);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
