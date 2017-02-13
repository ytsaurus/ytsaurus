#pragma once

/*!
 * \file fs.h
 * \brief File system functions
 */

#include "common.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

//! File suffix for temporary files.
const char* const TempFileSuffix = "~";

//! Returns |true| if a given path points to an existing file or directory.
bool Exists(const Stroka& path);

//! Removes a given file or directory.
void Remove(const Stroka& path);

//! Removes #destination if it exists. Then renames #destination into #source.
void Replace(const Stroka& source, const Stroka& destination);

//! Removes a given directory recursively.
void RemoveRecursive(const Stroka& path);

//! Renames a given file or directory.
void Rename(const Stroka& source, const Stroka& destination);

//! Returns name of file.
Stroka GetFileName(const Stroka& path);

//! Returns extension of file.
Stroka GetFileExtension(const Stroka& path);

//! Returns name of file without extension.
Stroka GetFileNameWithoutExtension(const Stroka& path);

//! Returns path of directory containing the file.
Stroka GetDirectoryName(const Stroka& path);

//! Returns the absolute path for the given (possibly relative) path.
Stroka GetRealPath(const Stroka& path);

//! Combines two strings into a path.
Stroka CombinePaths(const Stroka& path1, const Stroka& path2);

//! Combines a bunch of strings into a path.
Stroka CombinePaths(const std::vector<Stroka>& paths);

//! Deletes all files with extension #TempFileSuffix in a given directory.
void CleanTempFiles(const Stroka& path);

//! Returns all files in a given directory.
std::vector<Stroka> EnumerateFiles(const Stroka& path, int depth = 1);

//! Returns all directories in a given directory.
std::vector<Stroka> EnumerateDirectories(const Stroka& path, int depth = 1);

//! Describes total, free, and available space on a disk drive.
struct TDiskSpaceStatistics
{
    i64 TotalSpace;
    i64 FreeSpace;
    i64 AvailableSpace;
};

//! Computes the space statistics for disk drive containing #path.
TDiskSpaceStatistics GetDiskSpaceStatistics(const Stroka& path);

//! Creates the #path and parent directories if they don't exists.
void ForcePath(const Stroka& path, int mode = 0777);

struct TFileStatistics
{
    i64 Size = -1;
    TInstant ModificationTime;
    TInstant AccessTime;
};

//! Returns the file statistics.
TFileStatistics GetFileStatistics(const Stroka& path);

//! Sets the access and modification times to now.
void Touch(const Stroka& path);

//! Converts all back slashes to forward slashes.
Stroka NormalizePathSeparators(const Stroka& path);

//! Sets 'executable' mode.
void SetExecutableMode(const Stroka& path, bool executable);

//! Makes a symbolic link on file #fileName with #linkName.
void MakeSymbolicLink(const Stroka& filePath, const Stroka& linkPath);

//! Returns |true| if given paths refer to the same inode.
//! Always returns |false| under Windows.
bool AreInodesIdentical(const Stroka& lhsPath, const Stroka& rhsPath);

//! Returns the home directory of the current user.
//! Interestingly, implemented for both Windows and *nix.
Stroka GetHomePath();

//! Flushes the directory's metadata. Useful for, e.g., committing renames happened in #path.
void FlushDirectory(const Stroka& path);

struct TMountPoint
{
    Stroka Name;
    Stroka Path;
};

std::vector<TMountPoint> GetMountPoints(const Stroka& mountsFile = "/proc/mounts");

//! Mount tmpfs at given path.
void MountTmpfs(const Stroka& path, int userId, i64 size);

//! Unmount given path.
void Umount(const Stroka& path, bool detach);

//! Wraps a given #func in with try/catch; makes sure that only IO-related
//! exceptions are being thrown. For all other exceptions, immediately terminates
//! with fatal error.
void ExpectIOErrors(std::function<void()> func);

//! Sets a given mode on the path.
void Chmod(const Stroka& path, int mode);

//! Copies file chunk after chunk, releasing thread between chunks.
void ChunkedCopy(
    const Stroka& existingPath, 
    const Stroka& newPath, 
    i64 chunkSize);

TError AttachLsofOutput(TError error, const Stroka& path);
TError AttachFindOutput(TError error, const Stroka& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
