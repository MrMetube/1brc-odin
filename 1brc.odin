#+feature using-stmt
package main

import "base:intrinsics"

import "core:fmt"
import "core:slice"
import "core:strings"
import "core:strconv"
import "core:sys/info"
import "core:sys/windows"
import "core:thread"
import "core:time"

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
    entries: [LaneWidth] Mapping,
}

////////////////////////////////////////////////

main :: proc() {
    spall_buffer_size :: 40 * Kilobyte
    init_spall(spall_buffer_size)
    spall_proc()
    
    work_start := time.now()
    data, file_mapping_handle := load_data()
    
    read_start := time.now()
    sum : u8 = 123
    for v in data {
        sum ~= v
    }
    fmt.printf("%v\r                  \n", sum)
    read_duration := time.since(read_start)
    
    cpu_core_count, _, _ := info.cpu_core_count()
    core_count := Multithreaded ? cpu_core_count : 1
    parts := split_data(&data, core_count)
    
    threads  := make([] ^thread.Thread, core_count-1)
    arg_list := make([] ParseArgs,      core_count)
    
    spall_begin("reserve entries")
    for &arg in arg_list {
        for lane in 0..<LaneWidth {
            reserve(&arg.entries[lane], 10000)
        }
    }
    spall_end()
    
    worker_thread :: proc (a: ^ParseArgs) {
        init_spall_thread(a.thread_index, spall_buffer_size)
        parse_entries(&a.entries, a.data)
        if a.thread_index != 0 { spall_flush() }
    }
    
    for &args, i in arg_list {
        args.data = parts[i]
        args.thread_index = cast(u32) i
    }
    spall_begin("spawn threads")
    for &t, i in threads {
        args := &arg_list[i+1]
        t = thread.create_and_start_with_poly_data(args, worker_thread)
    }
    spall_end()
    
    worker_thread(&arg_list[0])
    
    for t in threads {
        for !thread.is_done(t) {
            intrinsics.cpu_relax()
        }
    }
    // thread.join_multiple(..threads)
    
    spall_begin("scalar")
    
    spall_begin("merging")
    entries: Mapping
    reserve(&entries, 10000)
    for a in arg_list {
        for lane in 0..<LaneWidth {
            for name, entry in a.entries[lane] {
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
    }
    spall_end()
    windows.UnmapViewOfFile(file_mapping_handle)
    
    spall_begin("prepare results")
    list := make([]Result_Entry, len(entries))
    index: int
    for _, &e in entries {
        defer index += 1
        value := Result_Entry {
            mean = cast(f32) (cast(f64) e.sum / cast(f64) e.count) * .1,
            min  = cast(f32) e.min * .1,
            max  = cast(f32) e.max * .1,
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
    
    float_buffer: [384] u8
    for entry in list {
        if !true {
            // 2.0ms
            min  := strconv.write_float(float_buffer[:], cast(f64) entry.min,  'f', 1, 32)
            mean := strconv.write_float(float_buffer[:], cast(f64) entry.mean, 'f', 1, 32)
            max  := strconv.write_float(float_buffer[:], cast(f64) entry.max,  'f', 1, 32)
            
            for _ in strings.rune_count(entry.name) ..< 20 { append(&builder.buf, ' ') }
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
    fmt.print(output)
    spall_end()
    spall_end()
    
    gigabytes := cast(f64) len(data) / cast(f64) 1 * Gigabyte
    
    total_duration := time.since(work_start) - read_duration
    fmt.printf("\n")
    fmt.printf("Read: %v - %.3f Gb/s\n", read_duration,  gigabytes / (cast(f64) read_duration)  / (cast(f64) time.Second))
    fmt.printf("Work: %v - %.3f Gb/s\n", total_duration, gigabytes / (cast(f64) total_duration) / (cast(f64) time.Second))
}

////////////////////////////////////////////////

load_data :: proc() -> (data: []u8, file_mapping_handle:windows.HANDLE) {
    file_handle := windows.CreateFileW(DATA_PATH, windows.GENERIC_READ, windows.FILE_SHARE_READ, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, nil)
    if file_handle == nil do print_error_and_panic()
    defer windows.CloseHandle(file_handle)
    
    print_error_and_panic :: proc (loc := #caller_location) {
        fmt.panicf("\nERROR at %v\n", loc)
    }
    
    file_mapping_handle = windows.CreateFileMappingW(file_handle, nil, 2, 0, 0, nil)
    if file_mapping_handle == nil do print_error_and_panic()
    
    file_size: windows.LARGE_INTEGER
    windows.GetFileSizeEx(file_handle, &file_size)
    
    starting_address := cast([^] u8) windows.MapViewOfFile(file_mapping_handle, windows.FILE_MAP_READ , 0, 0, 0)
    if starting_address == nil do print_error_and_panic()
    
    result := starting_address[:file_size]
    return result, file_mapping_handle
}

split_data :: proc(data: ^[]u8, count := 2) -> [][]u8 {
    spall_proc()
    
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

////////////////////////////////////////////////

parse_entries :: proc (entries: ^[LaneWidth] Mapping, data: [] u8) {
    N :: LaneWidth
    names:  [dynamic; N] string
    bytes:  lane_i64
    counts: lane_u64

    Scan_Width :: 64
    scan_lane :: #simd [Scan_Width] u8
    scan_mask :: #simd [Scan_Width] u32
    semicolon_chunk, line_chunk: int
    Bits :: bit_set[0..<Scan_Width]
    semicolon_bits, line_bits: Bits
    
    last, index: int
    loop: for {
        should_break: bool
        
        spall_begin("collect lines")
        #no_bounds_check for !(should_break || len(names) == cap(names)) {
            for semicolon_bits == {} {
                valid_lanes     := lanes_less(cast(scan_mask) semicolon_chunk + lanes_indices(scan_mask), cast(scan_mask) len(data))
                chunk           := lanes_masked_load(&data[semicolon_chunk], cast(scan_lane) 0, valid_lanes)
                semicolon_bits   = lanes_extract_most_significant_bits(lanes_equal(chunk, cast(scan_lane) ';'))
                semicolon_chunk += Scan_Width
            }
            semicolon_offset := cast(int) count_trailing_zeros(transmute(u64) semicolon_bits)
            semicolon_bits   -= { semicolon_offset }
            semicolon        := semicolon_chunk - Scan_Width + semicolon_offset
            
            for line_bits == {} {
                valid_lanes := lanes_less(cast(scan_mask) line_chunk + lanes_indices(scan_mask), cast(scan_mask) len(data))
                chunk       := lanes_masked_load(&data[line_chunk], cast(scan_lane) 0, valid_lanes)
                line_bits    = lanes_extract_most_significant_bits(lanes_equal(chunk, cast(scan_lane) '\r'))
                line_chunk  += Scan_Width
            }
            line_offset := cast(int) count_trailing_zeros(transmute(u64) line_bits)
            line_bits   -= { line_offset }
            line_end    := line_chunk - Scan_Width + line_offset
            
            text  := (cast(^i64) &data[semicolon+1])^
            count := cast(u64) (line_end - semicolon - 1)
            name  := cast(string) data[last:semicolon]
            
            bytes  = lanes_replace(bytes,  len(names), text)
            counts = lanes_replace(counts, len(names), count)
            append(&names, name)
            
            index = line_end + len("\r\n")
            should_break = index >= len(data)
            last = index
        }
        spall_end()
        
        {
            temperatures := parse_temperatures(bytes, counts)
            
            es: Lane(Entry)
            just_inserteds: lane_u32
            
            spall_begin("hash and map insertion")
            for name, lane in names {
                seed: u32 : 5381
                name_bytes := transmute([] u8) name
                hash := seed
                #no_bounds_check for b in name_bytes {
                    hash = hash * 33 + cast(u32) b
                }
                
                _, e, just_inserted, _ := map_entry(&entries[lane], hash)
                
                es.p           = lanes_replace(es.p,           lane, cast(umm) e)
                just_inserteds = lanes_replace(just_inserteds, lane, just_inserted ? true_lane : 0)
            }
            spall_end()
            
            spall_begin("entry update")
            sum   := lane_member(es, "sum")
            count := lane_member(es, "count")
            min   := lane_member(es, "min")
            max   := lane_member(es, "max")
            
            write_mask := lanes_less(lane_offset, cast(lane_u32) len(names))
            lane_scatter(sum,   lane_gather(sum,   write_mask, 0) + cast(lane_i32) temperatures, write_mask)
            lane_scatter(count, lane_gather(count, write_mask, 0) + 1,                           write_mask)
            
            old_min := lane_gather(min, write_mask & ~just_inserteds, temperatures)
            old_max := lane_gather(max, write_mask & ~just_inserteds, temperatures)
            
            lane_scatter(min, lanes_min(temperatures, old_min), write_mask)
            lane_scatter(max, lanes_max(temperatures, old_max), write_mask)
            
            // @todo
            for lane in 0..<len(names) {
                if lanes_extract(just_inserteds, lane) != 0 {
                    e := lane_extract(es, lane)
                    e.name = names[lane]
                }
            }
            spall_end()
            
            clear(&names)
        }
        
        if should_break { break loop }
    }
}

parse_temperatures :: proc (bytes: lane_i64, counts: lane_u64) -> lane_i16 #no_bounds_check {
    spall_proc()
    // the length of the temperature only varies by sign and <10 or >=10
    // 3 -> positive and <10
    // 4 -> negative and <10 or positive and >11
    // 5 -> negative and >10
    bytes := bytes
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
    minus := lanes_equal(bytes & 0xff, '-' & 0x0f)
    shifts := counts - cast(lane_u64) (minus ~ lanes_equal(counts, 5)) & 1
    
    temperature := 100 * (shift_right(bytes, (8 * shifts - 32)) & 0xff)
    temperature +=  10 * (shift_right(bytes, (8 * counts - 24)) & 0xff)
    temperature +=       (shift_right(bytes, (8 * counts -  8)) & 0xff)
    temperature *= (cast(lane_i64) minus & 1) * -2 + 1
    
    return cast(lane_i16) temperature
}

////////////////////////////////////////////////

LaneWidth :: 8

lane_f32 :: #simd [LaneWidth] f32
lane_u32 :: #simd [LaneWidth] u32
lane_i32 :: #simd [LaneWidth] i32

lane_v2 :: [2] lane_f32
lane_v3 :: [3] lane_f32
lane_v4 :: [4] lane_f32

lane_iv2 :: [2] lane_i32
lane_uv3 :: [3] lane_u32

lane_pmm :: #simd [LaneWidth] pmm
lane_umm :: #simd [LaneWidth] umm
lane_int :: #simd [LaneWidth] int
lane_f64 :: #simd [LaneWidth] f64
lane_u64 :: #simd [LaneWidth] u64
lane_i64 :: #simd [LaneWidth] i64
lane_i16 :: #simd [LaneWidth] i16

lane_u8  :: #simd [LaneWidth] u8

lane_false :: cast(lane_u32) 0
true_lane  :: cast(u32) 0xffff_ffff
lane_true  :: cast(lane_u32) true_lane

lane_offset :: lane_u32{0, 1, 2, 3, 4, 5, 6, 7} when LaneWidth == 8 else ( lane_u32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15} when LaneWidth == 16 else ( lane_u32{0, 1, 2, 3} when LaneWidth == 4 else lane_u32 { 0, 1 }))

shift_right                         :: intrinsics.simd_shr_masked
lanes_extract                       :: intrinsics.simd_extract
lanes_replace                       :: intrinsics.simd_replace
lanes_equal                         :: intrinsics.simd_lanes_eq
lanes_less                          :: intrinsics.simd_lanes_lt
lanes_greater                       :: intrinsics.simd_lanes_gt
lanes_rotate                        :: intrinsics.simd_lanes_rotate_right
lanes_any                           :: intrinsics.simd_reduce_any
lanes_all                           :: intrinsics.simd_reduce_all
lanes_select                        :: intrinsics.simd_select
lanes_min                           :: intrinsics.simd_min
lanes_max                           :: intrinsics.simd_max
lanes_indices                       :: intrinsics.simd_indices
lanes_masked_load                   :: intrinsics.simd_masked_load
lanes_extract_most_significant_bits :: intrinsics.simd_extract_msbs

count_trailing_zeros :: intrinsics.count_trailing_zeros
