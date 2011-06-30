require "stdlib"

function get_input_file_name()
    if arg[1] == nul then
        error("Input file argument is not specified")
    end

    return arg[1]
end

function get_output_file_name()
    if arg[2] == nul then
        error("Output file argument is not specified")
    end

    return arg[2]
end

function strip(s)
    return string.gsub(s,"^%s*(.-)%s*$", "%1")
end

function is_empty(s)
    return not s.find(s, "%S")
end

function find_last_of(s, subStr)
    local lastPos = -1

    while true do
        local pos = string.find(s, subStr, lastPos + 1)

        if not (pos == nil) then
            lastPos = pos
        elseif lastPos == -1 then
            return nil
        else
            return lastPos
        end
    end
end

function get_file_name(path)
    local pos = find_last_of(path, "/")

    if pos == nil then
        return path
    end

    return string.sub(path, pos + 1)
end

function check_name_line(line)
    line = strip(line)

    local commentPos = string.find(line, "//")

    if commentPos == 1 then
        return ""
    end

    if not (commentPos == nil) then
        line = string.sub(line, 1, commentPos - 1)
        line = strip(line)
    end

    if is_empty(line) then
        return line
    end
    
    local spacePos = string.find(line, " ")
    if spacePos == nil then
        return ""
    end

    local enum = string.sub(line, 1, spacePos - 1)
    if (enum ~= "enum") then
        return ""
    end

    line = string.sub(line, spacePos + 1, #line)
    line = strip(line)
    spacePos = string.find(line, " ")
    if spacePos == nil then
        return line
    end

    line = string.sub(line, 1, spacePos - 1)
    return line
end

function check_field_line(line)
    line = strip(line)

    local endPos = string.find(line, "//")
    if endPos == 1 then
        return ""
    end
    if not (endPos == nil) then
        line = string.sub(line, 1, endPos - 1)
        line = strip(line)
    end
    if is_empty(line) then
        return ""
    end
    endPos = string.find(line, ",")
    if not (endPos == nil) then
        line = string.sub(line, 1, endPos - 1)
        line = strip(line)
    end
    return line
end

function write_header(outFile, name)
    outFile:write("// This file was auto-generated. Do not edit!!!\n")
    outFile:write("#include \"" .. name .. "\"\n")
    outFile:write("#include <util/generic/typetraits.h>\n")
    outFile:write("#include <util/generic/singleton.h>\n")
    outFile:write("#include <util/generic/stroka.h>\n")
    outFile:write("#include <util/generic/map.h>\n")
    outFile:write("#include <util/stream/output.h>\n\n")
end

function write_enum(outFile, name, fields, hasEq)
    local count = #fields + 1
    local nsname = "N" .. string.sub(name, 2, #name) .. "Private"
    outFile:write("namespace " .. nsname .. " {\n")
    outFile:write("    class TNameBufs {\n")
    outFile:write("    private:\n")
    if hasEq == true then
        outFile:write("        ymap<int, Stroka> Names;\n")
    else
        outFile:write("        Stroka Names[" .. count .. "];\n")
    end
    outFile:write("    public:\n")
    outFile:write("        TNameBufs() {\n")
    for k, v in pairs(fields) do
        outFile:write("            Names[" .. k .. "] = \"" .. v .. "\";\n")
    end
    outFile:write("        }\n\n")
    outFile:write("        const Stroka& ToString(" .. name .. " i) const {\n")
    if hasEq == true then
        outFile:write("            ymap<int, Stroka>::const_iterator j = Names.find(i);\n")
        outFile:write("            YASSERT(j != Names.end());\n")
        outFile:write("            return j->second;\n")
    else
        outFile:write("            YASSERT(i >= 0 && i < " .. count .. ");\n")
        outFile:write("            return Names[i];\n")
    end
    outFile:write("        }\n\n")
    outFile:write("        bool FromString(const Stroka& name, " .. name .. "& ret) const {\n")
    if hasEq == true then
        outFile:write("            for (ymap<int, Stroka>::const_iterator i = Names.begin(); i != Names.end(); ++i) {\n")
        outFile:write("                if (i->second == name) {\n")
        outFile:write("                    ret = (" .. name .. ")i->first;\n")
    else
        outFile:write("            for (int i = 0; i < " .. count .. "; ++i) {\n")
        outFile:write("                if (Names[i] == name) {\n")
        outFile:write("                    ret = (" .. name .. ")i;\n")
    end
    outFile:write("                    return true;\n")
    outFile:write("                }\n")
    outFile:write("            }\n")
    outFile:write("            return false;\n")
    outFile:write("        }\n\n")
    outFile:write("        static inline const TNameBufs& Instance() {\n")
    outFile:write("            return *Singleton<TNameBufs>();\n")
    outFile:write("        }\n")
    outFile:write("    };\n")
    outFile:write("}\n\n")
    outFile:write("const Stroka& ToString(" .. name .. " x) {\n")
    outFile:write("    const " .. nsname .. "::TNameBufs& names = " .. nsname .. "::TNameBufs::Instance();\n")
    outFile:write("    return names.ToString(x);\n")
    outFile:write("}\n\n")
    outFile:write("bool FromString(const Stroka& name, " .. name .. "& ret) {\n")
    outFile:write("    const " .. nsname .. "::TNameBufs& names = " .. nsname .. "::TNameBufs::Instance();\n")
    outFile:write("    return names.FromString(name, ret);\n")
    outFile:write("}\n\n")
    outFile:write("template<>\n")
    outFile:write("void Out<" .. name .. ">(TOutputStream& os, TTypeTraits<" .. name .. ">::TFuncParam n) {\n")
    outFile:write("    os << ToString(n);\n")
    outFile:write("}\n\n")
end

function parse(inputFileName, outputFileName, ns)
    local endMarker = "};"

    local inFile = assert(io.open(inputFileName, "r"))
    local outFile = assert(io.open(outputFileName, "wb"))
    
    local fname = get_file_name(inputFileName)
    write_header(outFile, fname)
    if not (ns == nil) then
        outFile:write("using namespace " .. ns .. ";\n\n")
    end

    local fields = {}
    local enumname
    local hasEq = false
    local value = 0

    local inEnum = false

    for line in inFile:lines() do
        if inEnum == false then
            enumname = check_name_line(line)
            if not is_empty(enumname) then
                inEnum = true
                print("Process " .. enumname .. "...")
            end
        else
            line = check_field_line(line)
            if line == endMarker then
                write_enum(outFile, enumname, fields, hasEq)
                fields = {}
                enumname = ""
                hasEq = false
                value = 0
                inEnum = false
            elseif not is_empty(line) then
                local eqPos = string.find(line, "=")
                if eqPos == nil then
                    fields[value] = line
                else
                    local name = string.sub(line, 1, eqPos - 1)
                    name = strip(name)
                    value = string.sub(line, eqPos + 1, #line)
                    value = strip(value)
                    fields[value] = name
                    hasEq = true
                end
                value = value + 1
            end
        end
    end

    io.close(inFile)
    io.close(outFile)
end


function main()
    local inputFileName  = get_input_file_name()
    local outputFileName = get_output_file_name()
    parse(inputFileName, outputFileName, arg[3])
    print("OK")
end

main()
