package main

import "core:fmt"
import "core:mem"
import "core:os"
import "core:slice"
import "core:strings"
import "core:sys/windows"
import pt "perftime"

DATA_PATH ::
	"C:/1brc/data/measurements_10000.txt" when ODIN_DEBUG else "C:/1brc/data/measurements.txt"

Entry :: struct {
	name:                 string,
	// TODO reduce sizes, pack efficiently
	min, max, sum, count: f64,
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
	entries: map[string]Entry
	last: int
	line: []u8
	for r, data_index in data {
		if r == '\n' {
			line = data[last:data_index - 1] // dont include the \r
			last = data_index + 1 // dont include the \n
		}
		for c, index in line {
			if c == ';' {
				name := line[:index - 1]
				measurement_str := line[index + 1:]
				pt.start("parse measurem.")
				// TODO use fixed point numbers
				measurement: f64
				is_negative: b32
				FACTOR :: 10
				DIVISOR :: .1
				for r in measurement_str {
					switch r {
					case '-':
						is_negative = true
					case '0' ..= '9':
						measurement *= FACTOR
						measurement += f64(u8(r) - u8('0'))
					}
				}
				measurement *= DIVISOR
				if is_negative do measurement *= -1
				pt.stop()

				pt.start("update entries")
				name_str := string(name)
				if name_str not_in entries {
					entries[name_str] = Entry {
						name  = name_str,
						min   = measurement,
						max   = measurement,
						sum   = measurement,
						count = 1,
					}
				} else {
					e := &entries[name_str]
					e.count += 1
					e.sum += measurement
					// TODO unify, cant be both
					e.min = min(e.min, measurement)
					e.max = max(e.max, measurement)
				}
				pt.stop()
			}
		}
		if data_index % 100_000 == 0 {
			fmt.printf("\t\r%v",data_index)
		}
	}
	pt.stop()
	// TODO sort and calculate mean
	pt.start("sort")
	list := make([]Entry, len(entries))
	index: int
	for _, e in entries {
		list[index] = e
		index += 1
	}
	lexical :: proc(a, b: Entry) -> bool {
		return strings.compare(a.name, b.name) < 0
	}
	slice.sort_by(list, lexical)
	pt.stop()
	pt.start("calculate mean")
	for entry in list {
		mean := entry.sum / entry.count
		fmt.printf("%v;%2.1f;%2.1f;%2.1f\n", entry.name, entry.min, mean, entry.max)
	}
	pt.stop()
}

print_error_and_panic :: proc() {
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
	fmt.panicf("ERROR: %s\n", string(message))
}
