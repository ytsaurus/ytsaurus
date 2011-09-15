#include "fs.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>


// for GetAvaibaleSpace()
#if defined(_linux_)
#include <sys/vfs.h>
#elif defined(_freebsd_) || defined(_darwin_)
#include <sys/param.h>
#include <sys/mount.h>
#elif defined (_win_)
#include <windows.h>
#endif

namespace NYT {
namespace NFS {

//////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FS");

//////////////////////////////////////////////////////////////////////////////

bool Remove(Stroka name)
{
#if defined(_win_)
    return DeleteFile(~name);
#else
    struct stat sb;

    if (int result = lstat(~name, &sb))
        return result == 0;

    if (!S_ISDIR(sb.st_mode))
        return ::remove(~name) == 0;

    return ::rmdir(~name) == 0;
#endif
}

bool Rename(Stroka oldName, Stroka newName)
{
#if defined(_win_)
    return MoveFileEx(~oldName, ~newName, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return ::rename(~oldName, ~newName) == 0;
#endif
}

Stroka GetFileName(Stroka filePath)
{
    size_t delimPos = filePath.rfind('/');
#ifdef _win32_
    if (delimPos == Stroka::npos) {
        // There's a possibility of Windows-style path
        delimPos = filePath.rfind('\\');
    }
#endif
    return (delimPos == Stroka::npos) ? filePath : filePath.substr(delimPos+1);
}

Stroka GetFileExtension(Stroka filePath)
{
    i32 dotPosition = filePath.find_last_of('.');
    if (dotPosition < 0) {
        return "";
    }
    return filePath.substr(dotPosition + 1, filePath.size() - dotPosition - 1);
}

Stroka GetFileNameWithoutExtension(Stroka filePath)
{
    filePath = GetFileName(filePath);
    i32 dotPosition = filePath.find_last_of('.');
    if (dotPosition < 0) {
        return filePath;
    }
    return filePath.substr(0, dotPosition);
}

void CleanTempFiles(Stroka location)
{
    LOG_INFO("Cleaning temp files in %s", ~location);
    TFileList fileList;
    fileList.Fill(location);
    const char* fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka fullName = location + "/" + Stroka(fileName);
        if (fullName.has_suffix(TempFileSuffix)) {
            LOG_INFO("Removing file %s", ~fullName);
            if (!NFS::Remove(~fullName)) {
                LOG_ERROR("Error removing %s",  ~fullName);
            }
        }
    }
}

i64 GetAvailableSpace(const Stroka& path)
{
#if !defined( _win_)
    struct statfs fsData;
    int result = statfs(~path, &fsData);
    i64 availableSpace = fsData.f_bavail * fsData.f_bsize;
#else
    ui64 freeBytes;
    int result = GetDiskFreeSpaceExA(
        ~path,
        (PULARGE_INTEGER)&freeBytes,
        (PULARGE_INTEGER)NULL,
        (PULARGE_INTEGER)NULL);
    i64 availableSpace = freeBytes;
#endif

#if !defined(_win_)
    if (result != 0) {
#else
    if (result == 0) {
#endif
        throw yexception() <<
            Sprintf("Failed to get available disk space at %s.", ~path.Quote());
    }
    return availableSpace;
}

void ForcePath(Stroka path, int mode)
{
    MakePathIfNotExist(~path, mode);
}

i64 GetFileSize(const Stroka& filePath)
{
#if !defined(_win_)
    struct stat fData;
    int result = stat(~filePath, &fData);
#else
    WIN32_FIND_DATA findData;
    HANDLE handle = FindFirstFileA(~filePath, &findData);
#endif

#if !defined(_win_)
    if (result == -1) {
#else
    if (handle == INVALID_HANDLE_VALUE) {
#endif
        throw yexception() <<
            Sprintf("Failed to get size of a filea %s.", ~filePath.Quote());
    }

#if !defined(_win_)
    i64 fileSize = static_cast<i64>(fData.st_size);
#else
    FindClose(handle);
    i64 fileSize = static_cast<i64>(findData.nFileSizeHigh) * (MAXDWORD + 1) + findData.nFileSizeLow;
#endif

    return fileSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
