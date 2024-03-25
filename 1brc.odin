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

DATA_PATH ::
	"C:/1brc/data/measurements_10M.txt" when ODIN_DEBUG else "C:/1brc/data/measurements.txt"

Entry :: struct {
	sum:      i32,
	count:    u32,
	min, max: i16,
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

	data := load_data()
	when false && ODIN_DEBUG {
		pt.start("parsing")
		entries, names := parse_entries(data)
		defer delete(entries)
		pt.stop()
	} else {
		pt.start("parsing")
		core_count := os.processor_core_count()
		parts := split_data(&data, core_count)

		threads := make([]^thread.Thread, core_count)
		arg_list := make([]ParseArgs, core_count)

		parse_entries_args :: proc(a: ^ParseArgs) {
			a.entries, a.names = parse_entries(a.data)
		}
		for _, i in threads {
			args := &arg_list[i]
			args.data = parts[i]
			args.entries = make(Mapping)

			t := thread.create_and_start_with_poly_data(args, parse_entries_args)
			threads[i] = t
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
	}
	pt.start("find mean")
	list := make([]Result_Entry, len(entries))
	defer delete(list)
	index: int
	for hash in entries {
		defer index += 1
		e: ^Entry = &entries[hash]
		value := Result_Entry {
			mean = f32(e.sum) / f32(e.count) * .1,
			min  = f32(e.min) * .1,
			max  = f32(e.max) * .1,
			name = names[hash],
		}
		list[index] = value
	}
	pt.stop()

	pt.start("sort")
	
	lexical :: proc(a, b: Result_Entry) -> bool {
		return strings.compare(a.name, b.name) < 0
	}
	slice.sort_by(list, lexical)
	pt.stop()

	pt.start("print")
	builder, err := strings.builder_make()
	defer strings.builder_destroy(&builder)
	assert(err == nil, "Failed to make a string builder")
	for entry in list {
		fmt.sbprint(
			&builder,
			fmt.tprintf("%v;%2.1f;%2.1f;%2.1f\n", entry.name, entry.min, entry.mean, entry.max),
			sep = "",
		)
	}
	output := string(builder.buf[:])
	fmt.print(output)

	pt.stop()
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

parse_entries :: proc(data: []u8) -> (entries: Mapping, names: map[u32]string) {
	last: int
	for r, data_index in data {
		if r == '\n' {
			colon: int
			for i := last; i < data_index; i+= 1 {
				if data[i] == ';'{
					colon = i
					break
				}
			}
			name := data[last:colon-1]
			temp := data[colon+1:data_index-1]
			last = data_index + 1 // dont include the \n
 			
			temperature: i16 = parse_temperature(temp)

			h := hash.fnv32a(name)
			e, ok := &entries[h]
			if !ok {
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

	}
	return entries, names
}

parse_temperature :: proc "contextless" (s: []u8) -> (temperature: i16) {
	// the length of the temperature only varies by sign and <10 or >=10
	num :: #force_inline proc "contextless" (u: u8) -> i16 {return i16(u - '0')}
	make_num :: #force_inline proc "contextless" (hundreds, teens, units: u8) -> i16 {
		return num(hundreds) * 100 + num(teens) * 10 + num(units)
	}

	length := len(s)
	switch {
	case length == 3:
		// positive and < 10
		temperature = make_num('0', s[0], s[2])
	case length == 4 && s[0] == '-':
		// negative and < 10 or
		temperature = -make_num('0', s[1], s[3])
	case length == 4:
		// positive and > 10
		temperature = make_num(s[0], s[1], s[3])
	case length == 5:
		// negative and > 10
		temperature = -make_num(s[1], s[2], s[4])
	}
	return
}

load_data :: proc() -> (data: []u8) {
	pt.start_scope(#procedure)

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

	file_mapping_handle := windows.CreateFileMappingW(file_handle, nil, 2, 0, 0, nil)
	if file_mapping_handle == nil do print_error_and_panic()

	file_size: windows.LARGE_INTEGER
	windows.GetFileSizeEx(file_handle, &file_size)
	starting_address: ^u8 = auto_cast windows.MapViewOfFile(
		file_mapping_handle,
		windows.FILE_MAP_READ,
		0,
		0,
		0,
	)
	if starting_address == nil do print_error_and_panic()

	return mem.ptr_to_bytes(starting_address, int(file_size))
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
