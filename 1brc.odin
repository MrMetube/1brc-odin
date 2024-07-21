package main

import "core:fmt"
import "core:hash"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sys/windows"
import "core:thread"
import pt "perftime"

DATA_PATH :: "./data/measurements_1B.txt"

Entry :: struct {
    sum:      i32, // probably from -10M to 10M
    count:    u32, // at most 1 billion but probably at most 100k
    min, max: i16, // fixed point numbers from -999 to 999
}

Result_Entry :: struct {
    name:           string,
    min, mean, max: f32,
}
Mapping :: map[u32]Entry

ParseArgs :: struct {
    data:    []u8,
    entries: Mapping,
    names:   map[u32]string,
}

main :: proc(){
    one_billion_row_challenge()
}

one_billion_row_challenge :: proc() {
    pt.begin_profiling()
    defer pt.end_profiling()
    
    data, file_mapping_handle := load_data()
    
    core_count := os.processor_core_count()
    parts := split_data(&data, core_count)
    
    pt.start("parsing")
    threads  := make([]^thread.Thread, core_count)
    arg_list := make([]ParseArgs, core_count)
    
    parse_entries_args :: proc(a: ^ParseArgs) { a.entries, a.names = parse_entries(a.data) }

    for _, i in threads {
        args := &arg_list[i]
        args.data = parts[i]
        
        threads[i] = thread.create_and_start_with_poly_data(args, parse_entries_args)
    }
    thread.join_multiple(..threads)
    entries: Mapping
    names: map[u32]string
    for a in arg_list {
        for hash, entry in a.entries {
            if hash not_in entries {
                entries[hash] = entry
            } else {
                e := &entries[hash]
                e.count += entry.count
                e.sum += entry.sum
                e.min = min(entry.min, e.min)
                e.max = max(entry.max, e.max)
            }
        }
        for hash, name in a.names {
            names[hash] = name
        }
    }
    pt.stop()
    windows.UnmapViewOfFile(file_mapping_handle)
    
    pt.start("other")
    list := make([]Result_Entry, len(entries))
    index: int
    for hash in entries {
        defer index += 1
        e := &entries[hash]
        value := Result_Entry {
            mean = f32(e.sum) / f32(e.count) * .1,
            min  = f32(e.min) * .1,
            max  = f32(e.max) * .1,
            name = names[hash],
        }
        list[index] = value
    }
    pt.stop()
    
    pt.start("other")
    lexical :: proc(a, b: Result_Entry) -> bool { return a.name > b.name }
    slice.sort_by(list, lexical)
    pt.stop()
    
    pt.start("print")
    builder, err := strings.builder_make()
    assert(err == nil, "Failed to make a string builder")
    for entry in list {
        fmt.sbprintfln(
            &builder,
            "%20s; %2.1f; %+2.1f; %2.1f", entry.name, entry.min, entry.mean, entry.max,
        )
    }
    output := string(builder.buf[:])
    fmt.print(output)
    
    pt.stop()
}

parse_entries :: proc(data: []u8) -> (entries: Mapping, names: map[u32]string) {
    skip_to_value :: proc(index:^int, target: u8, data: []u8) {
        // based on https://graphics.stanford.edu/~seander/bithacks.html#ZeroInWord
        has_zero_byte :: proc (v:u32) -> b8 {
            MASK:u32: 0x7F7F7F7F
            return ~((((v & MASK) + MASK) | v) | MASK) != 0
        }
        
        has_value :: proc (v:u32, n:u32) -> b8 {
            return has_zero_byte( v ~ (~u32(0) / 255 * n) )    
        }
        for {
			STRIDE :: size_of(u32)
            if index^+STRIDE >= len(data) do break
            x := transmute(^u32) &data[index^]
            if has_value(x^, u32(target)) do break
            index^ += STRIDE
        }
        for index^ < len(data) && data[index^] != target do index^ += 1
    }
    
    last, index: int
    for index < len(data) {
        skip_to_value(&index, ';', data)
        if index >= len(data) do break
        
        colon := index
        skip_to_value(&index, '\r', data)
        
        name  := data[last:colon-1]
        temp := data[colon+1:index]
        last = index + len("\r\n") // dont include the newline
        
        temperature := parse_temperature(temp)
        
        h := hash.fnv32a(name)
        if e, ok := &entries[h]; !ok {
            entries[h] = Entry {
                min   = temperature,
                max   = temperature,
                sum   = i32(temperature),
                count = 1,
            }
            names[h] = string(name)
        } else {
            e.count += 1
            e.sum += i32(temperature)
            if temperature < e.min {
                e.min = temperature
            } else if temperature > e.max {
                e.max = temperature
            }
        }
    }
    return entries, names
}

parse_temperature :: proc(s: []u8) -> (temperature: i16) {
    // the length of the temperature only varies by sign and <10 or >=10
    make_num_hundreds :: proc(hundreds, teens, units: u8) -> i16 {
        return i16(hundreds) * 100 + make_num_teens(teens, units)
    }
    make_num_teens :: proc(teens, units: u8) -> i16 {
        return i16(teens) * 10 + i16(units)
    }
    
    switch len(s) {
    case 3:
        // positive and < 10
        temperature = make_num_teens(s[0] - '0', s[2] - '0')
    case 4:
        // negative and < 10 or positive and > 11
        temperature = s[0] == '-' ? -make_num_teens(s[1] - '0', s[3] - '0') : make_num_hundreds(s[0] - '0', s[1] - '0', s[3] - '0')
    case 5:
        // negative and > 10
        temperature = -make_num_hundreds(s[1] - '0', s[2] - '0', s[4] - '0')
    case: unreachable()
    }
    return
}

split_data :: proc(data: ^[]u8, count := 2) -> [][]u8 {
	pt.start_scope("other")

    result := make([][]u8, count)
    splits := make([]int, count, context.temp_allocator)
    stride := len(data) / count
    
    for i in 1 ..< count {
        middle := i * stride
        // fix to end of line
        for data[middle] != '\n' do middle += 1
        middle += 1 // after the \n
        splits[i] = middle
    }
    for i in 1 ..< count {
        result[i - 1] = data[splits[i - 1]:splits[i]]
    }
    result[count - 1] = data[splits[count - 1]:]
    return result
}

load_data :: proc() -> (data: []u8, file_mapping_handle:windows.HANDLE) {
	pt.start_scope("other")
    
    win_path := windows.utf8_to_utf16(DATA_PATH)
    file_handle := windows.CreateFileW(
        &win_path[0],
        windows.GENERIC_READ,
        windows.FILE_SHARE_READ,
        nil,
        windows.OPEN_EXISTING,
        windows.FILE_ATTRIBUTE_NORMAL,
        nil,
    )
    if file_handle == nil do print_error_and_panic()
    
    file_mapping_handle = windows.CreateFileMappingW(file_handle, nil, 2, 0, 0, nil)
    if file_mapping_handle == nil do print_error_and_panic()
    
    file_size: windows.LARGE_INTEGER
    windows.GetFileSizeEx(file_handle, &file_size)
    starting_address: ^u8 = auto_cast windows.MapViewOfFile(
        file_mapping_handle,
        windows.FILE_MAP_READ ,
        0,
        0,
        0,
    )
    if starting_address == nil do print_error_and_panic()
    
    windows.CloseHandle(file_handle)
    
    return mem.ptr_to_bytes(starting_address, int(file_size)), file_mapping_handle
}

print_error_and_panic :: proc(loc := #caller_location) {
    error_code := windows.GetLastError()
    buffer: [1024]u16
    sl := buffer[:]
    length := windows.FormatMessageW(
        windows.FORMAT_MESSAGE_FROM_SYSTEM,
        nil,
        error_code,
        0,
        raw_data(sl),
        1024,
        nil,
    )
    message, _ := windows.utf16_to_utf8(buffer[:length])
    fmt.panicf("\nERROR at %v : %s\n", loc, string(message))
}
