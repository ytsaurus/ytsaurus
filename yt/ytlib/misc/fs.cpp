#include "stdafx.h"
#include "fs.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

// For GetAvaibaleSpace().
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

bool Remove(const Stroka& name)
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

bool Rename(const Stroka& oldName, const Stroka& newName)
{
#if defined(_win_)
    return MoveFileEx(~oldName, ~newName, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return ::rename(~oldName, ~newName) == 0;
#endif
}

Stroka GetFileName(const Stroka& path)
{
    size_t delimPos = path.rfind('/');
#ifdef _win32_
    if (delimPos == Stroka::npos) {
        // There's a possibility of Windows-style path
        delimPos = path.rfind('\\');
    }
#endif
    return (delimPos == Stroka::npos) ? path : path.substr(delimPos+1);
}

Stroka GetFileExtension(const Stroka& path)
{
    i32 dotPosition = path.find_last_of('.');
    if (dotPosition < 0) {
        return "";
    }
    return path.substr(dotPosition + 1, path.size() - dotPosition - 1);
}

Stroka GetFileNameWithoutExtension(const Stroka& path)
{
    Stroka fileName = GetFileName(path);
    i32 dotPosition = fileName.find_last_of('.');
    if (dotPosition < 0) {
        return fileName;
    }
    return fileName.substr(0, dotPosition);
}

void CleanTempFiles(const Stroka& path)
{
    LOG_INFO("Cleaning temp files in %s",
        ~path.Quote());

    if (!isexist(~path))
        return;

    TFileList fileList;
    fileList.Fill(path);
    const char* fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka fullName = path + "/" + Stroka(fileName);
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
        ythrow yexception() <<
            Sprintf("Failed to get available disk space at %s.", ~path.Quote());
    }
    return availableSpace;
}

void ForcePath(const Stroka& path, int mode)
{
    MakePathIfNotExist(~path, mode);
}

i64 GetFileSize(const Stroka& path)
{
#if !defined(_win_)
    struct stat fileStat;
    int result = stat(~path, &fileStat);
#else
    WIN32_FIND_DATA findData;
    HANDLE handle = FindFirstFileA(~path, &findData);
#endif

#if !defined(_win_)
    if (result == -1) {
#else
    if (handle == INVALID_HANDLE_VALUE) {
#endif
        ythrow yexception() << Sprintf("Failed to get the size of file %s",
            ~path.Quote());
    }

#if !defined(_win_)
    i64 fileSize = static_cast<i64>(fileStat.st_size);
#else
    FindClose(handle);
    i64 fileSize =
        (static_cast<i64>(findData.nFileSizeHigh) << 32) +
        static_cast<i64>(findData.nFileSizeLow);
#endif

    return fileSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
