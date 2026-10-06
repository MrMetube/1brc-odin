#+no-instrumentation
#+vet !unused-procedures
package main

import "base:intrinsics"
import "base:runtime"

import "core:mem"
import "core:fmt"

pmm :: rawptr
umm :: uintptr

////////////////////////////////////////////////

Byte     :: 1
Kilobyte :: 1024 * Byte
Megabyte :: 1024 * Kilobyte
Gigabyte :: 1024 * Megabyte
Terabyte :: 1024 * Gigabyte
Petabyte :: 1024 * Terabyte
Exabyte  :: 1024 * Petabyte

absolute_difference :: proc (a, b: $T) -> (result: T) {
    result = abs(a - b)
    return result
}

swap :: proc (a, b: ^$T) { a^, b^ = b^, a^ }

@(disabled=ODIN_DISABLE_ASSERT)
assert :: proc(condition: $B, message := #caller_expression(condition), loc := #caller_location, prefix:= "Assertion failed") where intrinsics.type_is_boolean(B) {
    if !condition {
        fmt.printf("%v %v", loc, prefix)
        if len(message) > 0 {
            fmt.printf(": %v", message)
        }
        fmt.printf("\n")
        
        when ODIN_DEBUG {
             runtime.debug_trap()
        } else {
            runtime.trap()
        }
    }
}

slice_from_parts :: proc { slice_from_parts_cast, slice_from_parts_direct }
slice_from_parts_cast :: proc "contextless" ($T: typeid, data: pmm, #any_int count: i64) -> []T {
    // :PointerArithmetic
    return (cast([^]T)data)[:count]
}
slice_from_parts_direct :: proc "contextless" (data: ^$T, #any_int count: i64) -> []T {
    // :PointerArithmetic
    return (cast([^]T)data)[:count]
}

make :: proc {
    make_slice,
    make_dynamic_array,
    make_dynamic_array_len,
    make_dynamic_array_len_cap,
    make_map,
    make_map_cap,
    make_multi_pointer,
    make_soa_slice,
    make_soa_dynamic_array,
    make_soa_dynamic_array_len,
    make_soa_dynamic_array_len_cap,
    
    make_by_pointer_slice,
    make_by_pointer_dynamic_array,
    make_by_pointer_dynamic_array_len,
    make_by_pointer_dynamic_array_len_cap,
    make_by_pointer_map,
    make_by_pointer_map_cap,
    make_by_pointer_multi_pointer,
    make_by_pointer_soa_slice,
    make_by_pointer_soa_dynamic_array,
    make_by_pointer_soa_dynamic_array_len,
    make_by_pointer_soa_dynamic_array_len_cap,
}

make_by_pointer_slice :: proc(pointer: ^$T/[]$E, #any_int len: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_slice(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_dynamic_array :: proc(pointer: ^$T/[dynamic]$E, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_dynamic_array(T, allocator, loc) or_return
    return nil
}
make_by_pointer_dynamic_array_len :: proc(pointer: ^$T/[dynamic]$E, #any_int len: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_dynamic_array_len(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_dynamic_array_len_cap :: proc(pointer: ^$T/[dynamic]$E, #any_int len: int, cap: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_dynamic_array_len_cap(T, len, cap, allocator, loc) or_return
    return nil
}
make_by_pointer_map :: proc(pointer: ^$T/map[$K]$E, allocator := context.allocator, loc := #caller_location) {
    pointer ^= make_map(T, allocator, loc)
}
make_by_pointer_map_cap :: proc(pointer: ^$T/map[$K]$E, #any_int capacity: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_map_cap(T, capacity, allocator, loc) or_return
    return nil
}
make_by_pointer_multi_pointer :: proc(pointer: ^$T/[^]$E, #any_int len: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_multi_pointer(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_soa_slice :: proc(pointer: ^$T/#soa []$E, #any_int len: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_soa_slice(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_soa_dynamic_array :: proc(pointer: ^$T/#soa [dynamic]$E, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_soa_dynamic_array(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_soa_dynamic_array_len :: proc(pointer: ^$T/#soa [dynamic]$E, #any_int len: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_soa_dynamic_array_len(T, len, allocator, loc) or_return
    return nil
}
make_by_pointer_soa_dynamic_array_len_cap :: proc(pointer: ^$T/#soa [dynamic]$E, #any_int len, capacity: int, allocator := context.allocator, loc := #caller_location) -> mem.Allocator_Error {
    pointer ^= make_soa_dynamic_array_len_cap(T, len, allocator, loc) or_return
    return nil
}

////////////////////////////////////////////////

Raw_Dynamic_Array :: struct {
    data: rawptr,
    len:  int,
    cap:  int,
    allocator: mem.Allocator,
}
RawSlice :: struct {
    data: rawptr,
    len:  int,
}
RawAny :: struct {
    data: rawptr,
	id:   typeid,
}

////////////////////////////////////////////////

zero :: proc (s: [] $T) {
    for &it in s do it = {}
}