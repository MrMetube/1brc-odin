#+vet !unused-procedures
#+no-instrumentation
package main

import "core:prof/spall"
import "core:time"

SpallDisabled :: !true

spall_ctx: spall.Context
@(thread_local) spall_buffer: spall.Buffer
@(private="file", thread_local) backing_buffer: [] u8

////////////////////////////////////////////////

when !true {
    @(instrumentation_enter)
    @(disabled=SpallDisabled)
    spall_enter :: proc "contextless" (proc_address, call_site_return_address: rawptr, loc := #caller_location) {
        spall._buffer_begin(&spall_ctx, &spall_buffer, "", "", loc)
    }

    @(instrumentation_exit)
    @(disabled=SpallDisabled)
    spall_exit :: proc "contextless" (proc_address, call_site_return_address: rawptr, loc := #caller_location) {
        spall._buffer_end(&spall_ctx, &spall_buffer)
    }
}

////////////////////////////////////////////////

@(deferred_none = delete_spall)
@(disabled=SpallDisabled)
init_spall :: proc (backing_buffer_size := 10 * Megabyte, location := #caller_location) {
    spall_ctx = spall.context_create("trace.spall", 10 * time.Millisecond)
    make(&backing_buffer, backing_buffer_size)
    spall_buffer = spall.buffer_create(backing_buffer, auto_cast context.user_index)
}
@(deferred_out = delete_spall_thread)
init_spall_thread :: proc (thread_index := cast(u32) context.user_index, backing_buffer_size := 10 * Megabyte, location := #caller_location) -> u32 {
    when SpallDisabled { return 0 }
    if thread_index != 0 {
        make(&backing_buffer, backing_buffer_size)
        spall_buffer = spall.buffer_create(backing_buffer, thread_index)
        spall_begin(location.procedure)
    }
    return thread_index
}

@(disabled=SpallDisabled)
delete_spall :: proc () {
    defer spall.context_destroy(&spall_ctx)
    defer delete(backing_buffer)
    defer spall.buffer_destroy(&spall_ctx, &spall_buffer)
}
@(disabled=SpallDisabled)
delete_spall_thread :: proc (thread_index: u32) {
    if thread_index == 0 { return }
    defer delete(backing_buffer)
    defer spall.buffer_destroy(&spall_ctx, &spall_buffer)
    defer spall_end()
}

////////////////////////////////////////////////

@(deferred_none = spall_end)
@(disabled=SpallDisabled) spall_proc :: proc (name: string = "", location := #caller_location) {
    spall_begin(name == "" ? location.procedure : name, location)
}

@(deferred_none = spall_end)
@(disabled=SpallDisabled) spall_scope :: proc (name: string, location := #caller_location) {
    spall_begin(name, location)
}
@(disabled=SpallDisabled) spall_hit :: proc (name: string, location := #caller_location) {
    spall_begin(name)
    spall_end()
}
@(disabled=SpallDisabled) spall_begin :: proc (name: string, location := #caller_location) {
	spall._buffer_begin(&spall_ctx, &spall_buffer, name, "", location)
}
@(disabled=SpallDisabled) spall_end :: proc () { spall._buffer_end(&spall_ctx, &spall_buffer) }
@(disabled=SpallDisabled) spall_flush :: proc () { spall.buffer_flush(&spall_ctx, &spall_buffer) }
