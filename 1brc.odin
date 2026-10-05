package main

import "base:intrinsics"

import "core:fmt"
import "core:io"
import "core:mem"
import "core:os"
import info "core:sys/info"
import "core:slice"
import "core:strings"
import "core:strconv"
import "core:sys/windows"
import "core:thread"

import pt "perftime"
import "profiler"

Multithreaded :: true

DATA_PATH :: "./data/measurements_10M.txt"

Entry :: struct {
    name:  string,
    sum:      i32, // probably from -10M to 10M
    count:    u32, // at most 1 billion but probably at most 100k
    min, max: i16, // fixed point numbers from -999 to 999
}

Result_Entry :: struct {
    name:           string,
    min, mean, max: f32,
}
Mapping :: map[u32] Entry

ParseArgs :: struct {
    thread_index: u32,
    data:    [] u8,
    entries: Mapping,
}

main :: proc() {
    spall_buffer_size :: 1000 * Megabyte
    init_spall(spall_buffer_size)
    spall_proc()
    
    pt.begin_profiling()
    defer pt.end_profiling()
    
    data, file_mapping_handle := load_data()
    
    cpu_core_count, _, _ := info.cpu_core_count()
    core_count := Multithreaded ? cpu_core_count : 1
    parts := split_data(&data, core_count)
    
    spall_begin("preparation")
    threads  := make([] ^thread.Thread, core_count)
    arg_list := make([] ParseArgs,      core_count)
    
    worker_thread :: proc (a: ^ParseArgs) {
        init_spall_thread(a.thread_index, spall_buffer_size)
        parse_entries(&a.entries, a.data)
    }
    
    spall_begin("reserve entries")
    for &arg in arg_list {
        reserve(&arg.entries, 10000)
    }
    spall_end()
    spall_end()
    
    spall_begin("parsing")
    for _, i in threads {
        args := &arg_list[i]
        args.data = parts[i]
        args.thread_index = cast(u32) i + 1
        threads[i] = thread.create_and_start_with_poly_data(args, worker_thread)
    }
    thread.join_multiple(..threads)
    spall_end()
    
    spall_begin("merging")
    entries: Mapping
    reserve(&entries, 10000)
    for a in arg_list {
        for name, entry in a.entries {
            if name not_in entries {
                entries[name] = entry
            } else {
                e := &entries[name]
                e.count += entry.count
                e.sum += entry.sum
                e.min = min(entry.min, e.min)
                e.max = max(entry.max, e.max)
            }
        }
    }
    spall_end()
    windows.UnmapViewOfFile(file_mapping_handle)
    
    spall_begin("prepare results")
    list := make([]Result_Entry, len(entries))
    index: int
    for _, &e in entries {
        defer index += 1
        value := Result_Entry {
            mean = f32(e.sum) / f32(e.count) * .1,
            min  = f32(e.min) * .1,
            max  = f32(e.max) * .1,
            name = e.name,
        }
        list[index] = value
    }
    spall_end()
    
    spall_begin("sort")
    lexical :: proc(a, b: Result_Entry) -> bool { return a.name < b.name }
    slice.sort_by(list, lexical)
    spall_end()
    
    spall_begin("format")
    builder_buffer := make([] u8, 1*Megabyte)
    builder := strings.builder_from_slice(builder_buffer)
    
    writer := strings.to_writer(&builder)
    float_buffer: [384] u8
    for entry in list {
        if true {
            // 2.0ms
            min  := strconv.write_float(float_buffer[:], cast(f64) entry.min,  'f', 1, 32)
            mean := strconv.write_float(float_buffer[:], cast(f64) entry.mean, 'f', 1, 32)
            max  := strconv.write_float(float_buffer[:], cast(f64) entry.max,  'f', 1, 32)
            
            for i in strings.rune_count(entry.name) ..< 20 { append(&builder.buf, ' ') }
            append(&builder.buf, entry.name)
            append(&builder.buf, "; ")
            append(&builder.buf, min[1:]) // skip sign
            append(&builder.buf, "; ")
            append(&builder.buf, mean)
            append(&builder.buf, "; ")
            append(&builder.buf, max[1:]) // skip sign
            append(&builder.buf, '\n')
        } else {
            // 3.4ms
            fmt.sbprintf(&builder, "%20s; %.1f; %+.1f; %.1f\n", entry.name, entry.min, entry.mean, entry.max)
        }
    }
    spall_end()
    
    spall_begin("print")
    output := strings.to_string(builder)
    io.write_string(os.to_writer(os.stdout), output)
    spall_end()
}

parse_entries :: proc (entries: ^Mapping, data: [] u8) {
    spall_proc()
    
    last, index: int
    #no_bounds_check for {
        skip_to_value(&index, ';', data)
        if index >= len(data) do break
        colon := index
        skip_to_value(&index, '\r', data)
        
        name        := cast(string) data[last:colon]
        temperature := parse_temperature(&data[colon+1], cast(u32) (index - colon - 1))
        
        last = index + len("\r\n") // dont include the newline
        
        spall_scope("insert entry")
        hash_name :: proc (data: [] u8, seed: u32 = 5381) -> u32 #no_bounds_check {
            spall_proc()
            
            hash := seed
            for b in data {
                hash = hash * 33 + cast(u32) b
            }
            
            return hash
        }
        
        hash := hash_name(transmute([] u8) name)
        
        spall_begin("map entry")
        _, e, just_inserted, _ := map_entry(entries, hash)
        spall_end()
        
        e.sum   += cast(i32) temperature
        e.count += 1
        if just_inserted {
            e.min  = temperature
            e.max  = temperature
            e.name = name
        } else {
            if temperature < e.min {
                e.min = temperature
            } else if temperature > e.max {
                e.max = temperature
            }
        }
    }
}

skip_to_value :: proc(index: ^int, $target: u32, data: []u8) #no_bounds_check {
    local_index := index^
    for local_index < len(data) && cast(u32) data[local_index] != target do local_index += 1
    index ^= local_index
}

parse_temperature :: proc (s: pmm, count: u32) -> i16 #no_bounds_check {
    spall_proc()
    
    // the length of the temperature only varies by sign and <10 or >=10
    // 3 -> positive and <10
    // 4 -> negative and <10 or positive and >11
    // 5 -> negative and >10
    bytes := (cast(^i64) s)^
    bytes &= 0x000000_0f_0f_0f_0f_0f
    //              |          x1 x0 - hundreds
    //              |       t0 t1 t1 - tens
    //              | u0 u1 u2       - units

    // hundreds_shift 8 * { -1, 0, 1 }
    // minus | count = 5 | -minus + count=5
    // false |   false   | -0+0 =  0
    // false |    true   | -0+1 = -1
    //  true |   false   | -1+0 = -1
    //  true |    true   | -1+1 =  0
    minus := (bytes & 0xff) == ('-' & 0x0f)
    xx := count - cast(u32) (minus ~ (count == 5)) 
    
    temperature := 100 * ((bytes >> (8 * xx    - 32)) & 0xff)
    temperature +=  10 * ((bytes >> (8 * count - 24)) & 0xff)
    temperature +=       ((bytes >> (8 * count -  8)) & 0xff)
    temperature *= cast(i64) (minus) * -2 + 1
    
    return cast(i16) temperature
}

////////////////////////////////////////////////

split_data :: proc(data: ^[]u8, count := 2) -> [][]u8 {
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
    file_handle := windows.CreateFileW(
        DATA_PATH,
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

print_error_and_panic :: proc (loc := #caller_location) {
    fmt.panicf("\nERROR at %v\n", loc)
}
