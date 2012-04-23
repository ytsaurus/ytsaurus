#include "stdafx.h"
#include "fs.h"

#include <ytlib/logging/log.h>

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

// For JoinPaths
#ifdef _win_
static const char PATH_DELIM = '\\';
static const char PATH_DELIM2 = '/';
#else
static const char PATH_DELIM = '/';
static const char PATH_DELIM2 = 0;
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

Stroka GetDirectoryName(const Stroka& path)
{
    auto absPath = CombinePaths(GetCwd(), path);
#ifdef _win_
    // May be mixed style of filename ('/' and '\')
    correctpath(absPath);
#endif
    return absPath.substr(0, absPath.find_last_of(LOCSLASH_C));
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
    LOG_INFO("Cleaning temp files in %s", ~path.Quote());

    if (!isexist(~path))
        return;

    TFileList fileList;
    fileList.Fill(path, TStringBuf(), TStringBuf(), Max<int>());
    i32 size = fileList.Size();
    for (i32 i = 0; i < size; ++i) {
        Stroka fileName = NFS::CombinePaths(path, fileList.Next());
        if (fileName.has_suffix(TempFileSuffix)) {
            LOG_INFO("Removing file %s", ~fileName);
            if (!NFS::Remove(~fileName)) {
                LOG_ERROR("Error removing %s",  ~fileName);
            }
        }
    }
}

i64 GetAvailableSpace(const Stroka& path)
{
#if !defined( _win_)
    struct statfs fsData;
    int result = statfs(~path, &fsData);
    i64 availableSpace = (i64) fsData.f_bavail * fsData.f_bsize;
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
            Sprintf("Failed to get available disk space at %s (error code: %d)", 
                ~path.Quote(),
                result);
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

static bool IsAbsolutePath(const Stroka& path)
{
    if (path.empty())
        return false;
    if (path[0] == PATH_DELIM)
        return true;
#ifdef _win_
    if (path[0] == PATH_DELIM2)
        return true;
    if (path[0] > 0 && isalpha(path[0]) && path[1] == ':')
        return true;
#endif // _win_
    return false;
}

static Stroka JoinPaths(const Stroka& path1, const Stroka& path2)
{
    if (path1.empty())
        return path2;
    if (path2.empty())
        return path1;

    Stroka path = path1;
    int delim = 0;
    if (path1.back() == PATH_DELIM || path1.back() == PATH_DELIM2)
        ++delim;
    if (path2[0] == PATH_DELIM || path2[0] == PATH_DELIM2)
        ++delim;
    if (delim == 0)
        path.append(1, PATH_DELIM);
    path.append(path2, delim == 2 ? 1 : 0, Stroka::npos);
    return path;
}

Stroka CombinePaths(const Stroka& path1, const Stroka& path2)
{
    if (IsAbsolutePath(path2))
        return path2;
    return JoinPaths(path1, path2);
}

Stroka NormalizePathSeparators(const Stroka& path)
{
    Stroka result;
    result.reserve(path.length());
    for (int i = 0; i < path.length(); ++i) {
        if (path[i] == '\\') {
            result.append('/');
        } else {
            result.append(path[i]);
        }
    }
    return result;
}

void SetExecutableMode(const Stroka& path, bool executable)
{
    UNUSED(path);
    UNUSED(executable);
    // TODO(babenko): implement this
}

void MakeSymbolicLink(const Stroka& filePath, const Stroka& linkPath)
{
#ifdef _win_
    // From MSDN: If the function succeeds, the return value is nonzero.
    // If the function fails, the return value is zero. To get extended error information, call GetLastError.
    auto res = !CreateSymbolicLink(~linkPath, ~filePath, (DWORD)0);
    if (res == 0) {
        ythrow yexception() << Sprintf(
            "Failed to link %s to %s (Error: %d)",
            ~filePath.Quote(),
            ~linkPath.Quote(),
            GetLastError());
    }
#else
    auto res = symlink(~filePath, ~linkPath);
    if (res != 0) {
        ythrow yexception() << Sprintf(
            "Failed to link %s to %s (Error: %d)",
            ~filePath.Quote(),
            ~linkPath.Quote(),
            errno);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
