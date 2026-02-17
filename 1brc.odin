package main

import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sys/windows"
import "core:thread"
import pt "perftime"

Multithreaded :: true

DATA_PATH :: "./data/measurements_100k.txt"

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
    spall_buffer_size :: 10 * Megabyte
    init_spall(spall_buffer_size)
    spall_proc()
    
    pt.begin_profiling()
    defer pt.end_profiling()
    
    data, file_mapping_handle := load_data()
    
    core_count := Multithreaded ? os.processor_core_count() : 1
    parts := split_data(&data, core_count)
    
    spall_begin("parsing")
    threads  := make([]^thread.Thread, core_count)
    arg_list := make([]ParseArgs, core_count)
    
    parse_entries_args :: proc(a: ^ParseArgs) {
        init_spall_thread(a.thread_index, spall_buffer_size)
        a.entries = parse_entries(a.data)
    }

    for _, i in threads {
        args := &arg_list[i]
        args.data = parts[i]
        args.thread_index = cast(u32) i + 1
        threads[i] = thread.create_and_start_with_poly_data(args, parse_entries_args)
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
    
    spall_begin("print")
    builder, _ := strings.builder_make()
    for entry in list {
        fmt.sbprintfln(
            &builder,
            "%20s; %2.1f; %+2.1f; %2.1f", entry.name, entry.min, entry.mean, entry.max,
        )
    }
    
    output := strings.to_string(builder)
    fmt.print(output)
    fmt.println(len(list))
    spall_end()
}

parse_entries :: proc(data: []u8) -> (entries: Mapping) {
    reserve(&entries, 10000)
    spall_proc()
    
    last, index: int
    for {
        skip_to_value(&index, ';', data)
        if index >= len(data) do break
        colon := index
        skip_to_value(&index, '\r', data)
        
        name        := cast(string) data[last:colon]
        temperature := parse_temperature(data[colon+1:index])
        insert_entry(&entries, temperature, name)
        
        last = index + len("\r\n") // dont include the newline
    }
    
    return entries
}

skip_to_value :: proc(index: ^int, $target: u32, data: []u8) #no_bounds_check {
    local_index := index^
    for local_index < len(data) && cast(u32) data[local_index] != target do local_index += 1
    index ^= local_index
}

// Usage: 
// small : []u8 = ...
// big, mask, mask_end := masked_view(small, u32)
// for value, index in big {
//     if index == len(big)-1 do mask = mask_end
//     masked := value & mask
//     // only use the masked value
// }
masked_view :: proc (small_data: [] $S, $L: typeid) -> (big_data: [] L, mask_begin, mask_end: L) {
    size_factor := size_of(L) / size_of(S)
    
    big_len := len(small_data) / size_factor
    big_rest := len(small_data) % size_factor
    if big_rest != 0 do big_len += 1
    big_data = slice_from_parts(L, raw_data(small_data), big_len)
    
    _all_bits: [size_of(L)] u8 = ~ cast(u8) 0
    all_bits := transmute(L) _all_bits
    mask_begin = all_bits
    mask_end   = all_bits
    XL :: u128be when intrinsics.type_is_endian_big(L) else u128le
    if big_rest != 0 {
        s_bits :: size_of(S) * 8
        shift := cast(XL) (size_factor - big_rest) * s_bits
        when intrinsics.type_is_endian_big(L) {
            mask_end = cast(L) (all_bits << shift)
        } else {
            mask_end = cast(L) (all_bits >> shift)
        }
    }
    
    return big_data, mask_begin, mask_end
}

insert_entry :: proc(entries: ^Mapping, temperature: i16, name: string){
    hash_name :: proc (data: [] u8, seed: u32 = 5381) -> (hash: u32) {
        spall_proc()
        when !true {
            for b in data {
                hash = hash * 33 + cast(u32) b
            }
        } else {
            small : []u8 = data
            big, mask, mask_end := masked_view(small, u32be)
            for value, index in big {
                if index == len(big)-1 do mask = mask_end
                masked := value & mask
                
                hash = hash * 33 + transmute(u32) masked
            }
        }
        
        return hash
    }
    
    hashed := hash_name(transmute([] u8) name)
    
    spall_begin("map entry")
    _, e, just_inserted, _ := map_entry(entries, hashed)
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
	
parse_temperature :: proc(s: [] u8) -> (temperature: i16) #no_bounds_check {
    spall_proc()
    count := cast(i16) len(s)
    sign  := s[0] == '-' ? cast(i16) -1 : 1
    
    
    // hi := 1 * count + 0.5 * sign - 4.5   for all cases but count = 5
    hi := count == 5 ? 1 : (2 * count + sign - 9) / 2
    
    // the length of the temperature only varies by sign and <10 or >=10
    // 3 -> positive and <10
    // 4 -> negative and <10 or positive and >11
    // 5 -> negative and >10
    
    hundreds := hi == -1 ? 0 : cast(i16) s[hi] - '0'
    tens     := cast(i16) s[count - 3] - '0'
    units    := cast(i16) s[count - 1] - '0'
    temperature = sign * (hundreds * 100 + tens * 10 + units)
    
    return temperature
}

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

print_error_and_panic :: proc(loc := #caller_location) {
    message := os.error_string(os.get_last_error())
    fmt.panicf("\nERROR at %v : %s\n", loc, message)
}
