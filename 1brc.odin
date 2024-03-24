package main

import "core:fmt"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sys/windows"
import pt "perftime"

DATA_PATH ::
	"C:/1brc/data/measurements_10k.txt" when ODIN_DEBUG else "C:/1brc/data/measurements_1M.txt"

Entry :: struct {
	// TODO reduce sizes, pack efficiently
	sum:      i64,
	count:    u32,
	min, max: i16,
}

Entry_With_Name :: struct {
	e:              ^Entry,
	name:           string,
	min, mean, max: f32,
}

// TODO threading?
main :: proc() {
	pt.begin_profiling()
	defer pt.end_profiling()

	data: []byte
	{
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

		data = mem.ptr_to_bytes(starting_address, int(file_size))
	}
	pt.start("parsing")
	entries:  map[string]Entry
	// line_count: u32
	last: int
	line: []u8
	for r, data_index in data {
		if r == '\n' {
			line = data[last:data_index - 1] // dont include the \r
			last = data_index + 1 // dont include the \n
			// line_count += 1
		}
		for c, index in line {
			if c == ';' {
				name := line[:index - 1]
				measurement_str := line[index + 1:]
				pt.start("parse measurem.")
				FACTOR :: 10
				measurement: i16
				is_negative := measurement_str[0] == '-'
				start := is_negative ? 1 : 0
				for r in measurement_str[start:] {
					if r != '.' {
						measurement = FACTOR * measurement + i16(r - '0')
					}
				}
				if is_negative do measurement *= -1
				pt.stop()

				pt.start("update entries")
				pt.start("string")
				name_str := string(name)
				pt.stop()
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
				pt.stop()
			}
		}
		// if line_count % 10_000 == 0 {
		// 	fmt.printf("\t\r%v", line_count)
		// }
	}
	// fmt.println()
	pt.stop()
	pt.start("sort")
	list := make([]Entry_With_Name, len(entries))
	index: int
	for name, e in entries {
		defer index += 1

		e := e
		list[index] = Entry_With_Name {
			e    = &e,
			name = name,
		}
	}
	lexical :: proc(a, b: Entry_With_Name) -> bool {
		return strings.compare(a.name, b.name) < 0
	}
	slice.sort_by(list, lexical)
	pt.stop()
	pt.start("calculate mean")
	for entry in list {
		entry := entry
		entry.mean = f32(entry.e.sum) / f32(entry.e.count) * .1
		entry.min = f32(entry.e.min) * .1
		entry.max = f32(entry.e.max) * .1
	}
	pt.stop()
	pt.start("print")
	for entry in list {
		fmt.printf("%v;%2.1f;%2.1f;%2.1f\n", entry.name, entry.min, entry.mean, entry.max)
	}
	pt.stop()
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
