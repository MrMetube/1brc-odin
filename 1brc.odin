package main

import pq "core:container/priority_queue"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sys/windows"
import "core:thread"
import pt "perftime"

DATA_PATH ::
	"C:/1brc/data/measurements_10k.txt" when ODIN_DEBUG else "C:/1brc/data/measurements_10M.txt"

Entry :: struct {
	// TODO reduce sizes, pack efficiently
	sum:      i64,
	count:    u32,
	min, max: i16,
}

Result_Entry :: struct {
	name:           string,
	min, mean, max: f32,
}

ParseArgs :: struct {
	data:    []u8,
	entries: map[string]Entry,
}

main :: proc() {
	pt.begin_profiling()
	defer pt.end_profiling()

	data := load_data()
	// entries := parse_entries(data)
	pt.start("parsing")
	pt.start("split")
	core_count := os.processor_core_count()
	parts := split_data(&data, core_count)

	threads := make([]^thread.Thread, core_count)
	arg_list := make([]ParseArgs, core_count)

	parse_entries_args :: proc(a: ^ParseArgs) {
		result := parse_entries(a.data)
		a.entries = result
	}
	pt.stop()
	pt.start("run")
	for _, i in threads {
		args := &arg_list[i]
		args.data = parts[i]
		args.entries = make(map[string]Entry)

		t := thread.create_and_start_with_poly_data(args, parse_entries_args)
		threads[i] = t
	}
	thread.join_multiple(..threads)
	pt.stop()

	pt.start("join")
	entries: map[string]Entry
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
	pt.stop()
	pt.stop()

	pt.start("sort")
	lexical :: proc(a, b: Result_Entry) -> bool {
		return strings.compare(a.name, b.name) < 0
	}

	list := make([]Result_Entry, len(entries))
	index: int
	for name in entries {
		defer index += 1
		e: ^Entry = &entries[name]
		value := Result_Entry {
			mean = f32(e.sum) / f32(e.count) * .1,
			min  = f32(e.min) * .1,
			max  = f32(e.max) * .1,
			name = name,
		}
		list[index] = value
	}
	slice.sort_by(list, lexical)
	pt.stop()

	pt.start("print")

	builder, err := strings.builder_make()
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
	splits := make([]int, count)
	stride := len(data) / count

	for i in 1..<count {
		middle := i * stride
		// fix to end of line
		for data[middle] != '\n' do middle += 1
		middle += 1 // after the \n
		splits[i] = middle
	}
	for i in 1..<count{
		result[i-1] = data[splits[i-1]:splits[i]]
	}
	result[count-1] = data[splits[count-1]:]
	return result
}

parse_entries :: proc(data: []u8) -> (entries: map[string]Entry) {
	last: int
	line: []u8
	for r, data_index in data {
		if r == '\n' {
			line = data[last:data_index - 1] // dont include the \r
			last = data_index + 1 // dont include the \n
			colon: int
			for c, index in line {
				if c == ';' {
					colon = index
					break
				}
			}
			name := line[:colon - 1]
			str := line[colon + 1:]

			measurement: i16
			// the length of the measurement only varies by sign and <10 or >=10
			num :: #force_inline proc(u: u8) -> i16 {return i16(u - '0')}
			switch len(str) {
			case 3:
				// positive and < 10
				measurement = num(str[0]) * 10 + num(str[2])
			case 4:
				// negative and < 10 or positive and > 10
				if str[0] == '-' {
					measurement = -(num(str[1]) * 10 + num(str[3]))
				} else {
					measurement = num(str[0]) * 100 + num(str[1]) * 10 + num(str[3])
				}
			case 5:
				// negative and > 10
				measurement = -(num(str[1]) * 100 + num(str[2]) * 10 + num(str[4]))
			}

			name_str := string(name)
			if name_str not_in entries {
				entries[name_str] = Entry {
					min   = measurement,
					max   = measurement,
					sum   = i64(measurement),
					count = 1,
				}
			} else {
				e := &entries[name_str]
				e.count += 1
				e.sum += i64(measurement)
				if measurement < e.min {
					e.min = measurement
				} else if measurement > e.max {
					e.max = measurement
				}
			}
		}

	}
	return entries
}

load_data :: proc() -> (data: []u8) {
	pt.start("read file")
	defer pt.stop()
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
