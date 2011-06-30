SVN_REVISION_LINE = 4
SVN_PATH = 5
SVN_LAST_AUTHOR = 12

INDENT = "    "

function file_exists(filename)
    local f = io.open(filename, "r")
    if (f == nil) then
        return 0
    else
        io.close(f)
        return 1
    end
end

function preadline(command)
    local h = io.popen(command, 'r')
    if (h ~= nil) then
        local data = h:read()
        io.close(h)
        return data
    end
    return nil
end

function get_svn_data(fname)
    local out_data = ""
    local f = io.open(fname, "r")

    if (f == nil) then
        return nil
    end

    local lines = {}
    for line in f:lines() do
        table.insert(lines, line)
    end

    out_data = "Svn info:\\n"
    out_data = out_data..INDENT.."URL: "..lines[SVN_PATH].."\\n"
    out_data = out_data..INDENT.."Revision: "..lines[SVN_REVISION_LINE].."\\n"
    out_data = out_data..INDENT.."Last Changed Author: "..lines[SVN_LAST_AUTHOR].."\\n"

    io.close(f)
    return out_data
end

function get_git_data(src_dir)
    if (not file_exists(src_dir..".git/config")) then
        return nil
    end

    if (not file_exists("/bin/sh")) then  -- don't care about windows
        return nil
    end

    local commit = preadline('git --git-dir "'..src_dir..'/.git" rev-parse HEAD')
    if (commit == nil or string.len(commit) ~= 40) then
        return nil
    end

    local out_data = "git info:\\n"
    out_data = out_data..INDENT.."Commit: "..commit.."\\n"

    local author = preadline('git --git-dir "'..src_dir..'/.git" log -1 "--format=format:%an <%ae>" '..commit)
    if (author ~= nil) then
        out_data = out_data..INDENT.."Author: "..author.."\\n"
    end

    local subj = preadline('git --git-dir "'..src_dir..'/.git" log -1 "--format=format:%s" '..commit)
    if (subj ~= nil) then
        out_data = out_data..INDENT.."Subject: "..subj.."\\n"
    end

    local gitsvnid = preadline('git --git-dir "'..src_dir..'/.git" log -1 --grep="^git-svn-id: " | grep "^    git-svn-id: .*@.*"')
    if (gitsvnid ~= nil) then
        gitsvnid = string.sub(gitsvnid, 17, -1)
        local at = string.find(gitsvnid, '@')
        local sp = string.find(gitsvnid, ' ', at)
        out_data = out_data.."\\ngit-svn info:\\n"
        out_data = out_data..INDENT.."URL: "..string.sub(gitsvnid, 0, at-1).."\\n"
        out_data = out_data..INDENT.."Revision: "..string.sub(gitsvnid, at+1, sp-1).."\\n"
    end

    return out_data
end

function get_scm_data(fname)
    local src_dir = get_build_dir(fname)
    local result = ""

    result = get_svn_data(fname)
    if (result ~= nil) then
        return result
    end

    result = get_git_data(src_dir)
    if (result ~= nil) then
        return result
    end

    return "Svn info:\\n"..INDENT.."no svn info\\n"
end

function get_other_data(almost_top_dir, build_dir)
    local out_data = "Other info:\\n"

    local user = os.getenv("USER")
    if (user == nil) then
        user = "Unknown user"
    end

    local hostname = os.hostname()
    if (hostname == nil) then
        hostname = "No host information"
    end

    local src_dir = string.sub(almost_top_dir, 0, -13)

    local build_date = os.date()

    out_data = out_data..INDENT.."Build by: "..user.."\\n"
    out_data = out_data..INDENT.."Top src dir: "..src_dir.."\\n"
    if (build_dir ~= nil) then
        out_data = out_data..INDENT.."Top build dir: "..build_dir.."\\n"
    end
    out_data = out_data..INDENT.."Build date: "..build_date.."\\n"
    out_data = out_data..INDENT.."Hostname: "..hostname.."\\n"

    return out_data
end

function get_build_info(compiler, fname)
    local out_data = "Build info:\\n"
    out_data = out_data..INDENT.."Compiler: "..compiler.."\\n"

    local flags_info = INDENT.."Compile flags:"..fname.." no flags info\\n"
    local f = io.open(fname, "r")
    if (f ~= nil) then
        for line in f:lines() do
            local a, b = line:find("CXX_FLAGS")
            if a ~= nil and a == 1 then
                flags_info = INDENT.."Compile flags: "..string.gsub(line:sub(13), '"', '\\"')
                break
            end
        end
        io.close(f)
    end

    return out_data..flags_info
end

function get_build_dir(almost_top_dir)
    return string.sub(almost_top_dir, 0, -13);
end

print([[#ifndef _svnversion_h_
#define _svnversion_h_

#define PROGRAM_VERSION "]]..get_scm_data(arg[1]).."\\n"..get_other_data(arg[1], arg[4]).."\\n"..get_build_info(arg[2], arg[3])..[["
#define ARCADIA_SOURCE_PATH "]]..get_build_dir(arg[1])..[["

#endif]])

