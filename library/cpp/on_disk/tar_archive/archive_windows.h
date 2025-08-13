#pragma once

#ifndef S_IFLNK
#define S_IFLNK 0120000
#endif

#ifndef S_ISDIR
#define S_ISDIR(m)  (((m) & S_IFMT) == S_IFDIR) /* directory */
#define S_ISREG(m)  (((m) & S_IFMT) == S_IFREG) /* regular file */
#define S_ISLNK(m)  (((m) & S_IFMT) == S_IFLNK) /* Symbolic link */
#endif
