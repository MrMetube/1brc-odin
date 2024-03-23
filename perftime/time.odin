package perftime

import "core:fmt"
import "core:time"

DO_PROFILE :: true
DO_TIMING :: !true


Timing :: struct {
	start:                                              time.Time,
	exclusive_time, inclusive_time, old_inclusive_time: time.Duration,
	call_depth, hit_count, proccessed_byte_count:       i64,
	parent:                                             ^Timing,
}

MAX_BLOCK_DEPTH :: 1024 * 1024

Timer :: struct {
	current:                ^Timing,
	timings:                map[string]^Timing,
	total_start, total_end: time.Time,
	timeblock_keys:         [MAX_BLOCK_DEPTH]string,
	timeblock_cursor:       int,
}

when !DO_TIMING{
	start :: proc(key: string, byte_count: i64 = 0) {}
	stop :: proc() {}
	_make_timing :: proc(byte_count: i64) -> (t: ^Timing) {return nil}
	_start_timing :: proc(using t: ^Timing, byte_count: i64) {}
	_stop_timing :: proc(using t: ^Timing) {}
}else{
	start :: proc(key: string, byte_count: i64 = 0) {
		using the_timer
		timeblock_keys[timeblock_cursor] = key
		timeblock_cursor += 1

		previous := timings[key]
		if previous == nil {
			timings[key] = _make_timing(byte_count)
		} else {
			_start_timing(previous, byte_count)
		}
	}

	stop :: proc() {
		using the_timer
		timeblock_cursor -= 1
		last_key := timeblock_keys[timeblock_cursor]
		t := timings[last_key]

		_stop_timing(t)
	}

	_make_timing :: proc(byte_count: i64) -> (t: ^Timing) {
		t = new(Timing)
		t.parent = the_timer.current
		_start_timing(t, byte_count)
		return
	}

	_start_timing :: proc(using t: ^Timing, byte_count: i64) {
		proccessed_byte_count += byte_count
		if call_depth == 0 do old_inclusive_time = inclusive_time
		call_depth += 1

		the_timer.current = t
		start = time.now()

	}

	_stop_timing :: proc(using t: ^Timing) {
		elapsed_time := time.diff(start, time.now())
		the_timer.current = parent

		exclusive_time += elapsed_time
		if parent != nil do parent.exclusive_time -= elapsed_time

		call_depth -= 1
		if call_depth == 0 do inclusive_time = old_inclusive_time + elapsed_time

		hit_count += 1
	}
} // when DO_TIMING

when !DO_PROFILE {
	begin_profiling :: proc() {}
	end_profiling :: proc() {}
} else {

	the_timer: Timer

	begin_profiling :: proc() {
		using the_timer
		total_start = time.now()
	}

	end_profiling :: proc() {
		using the_timer
		total_end = time.now()

		total_time := time.diff(total_start, total_end)
		fmt.printf("%16s[% 9s]: %v \n", "Total", "hit count", total_time)

		DELTA :: 100

		for key, value in timings {
			using value
			percent_ex := 100 * f64(exclusive_time) / f64(total_time)
			fmt.printf("%16s[% 9d]: %v (%.2f%%", key, hit_count, exclusive_time, percent_ex)
			if inclusive_time - exclusive_time > DELTA {
				percent_in := 100 * f64(inclusive_time) / f64(total_time)
				fmt.printf(", %v %.2f%% w/children", inclusive_time, percent_in)
			}
			fmt.print(')')
			if call_depth != 0 {
				fmt.printf(
					"\n    ERROR: Call depth is %v \n    Check symmetry of start and stop calls for '%s'",
					call_depth,
					key,
				)
			}

			// TODO print Bandwidth

			fmt.println()
		}
		timings = {}
	}

	
} // when DO_PROFILE