#+vet !unused-procedures
#+no-instrumentation
package main

import "core:prof/spall"
import "core:time"

SpallEnabled :: !false

spall_ctx: spall.Context
@(thread_local) spall_buffer: spall.Buffer
@(private="file", thread_local) backing_buffer: [] u8

////////////////////////////////////////////////

when true {
    @(instrumentation_enter)
    @(disabled=!SpallEnabled)
    spall_enter :: proc "contextless" (proc_address, call_site_return_address: rawptr, loc := #caller_location) {
        spall._buffer_begin(&spall_ctx, &spall_buffer, "", "", loc)
    }

    @(instrumentation_exit)
    @(disabled=!SpallEnabled)
    spall_exit :: proc "contextless" (proc_address, call_site_return_address: rawptr, loc := #caller_location) {
        spall._buffer_end(&spall_ctx, &spall_buffer)
    }
}

////////////////////////////////////////////////

@(deferred_none = delete_spall)
@(disabled=!SpallEnabled)
init_spall :: proc (backing_buffer_size := 10 * Megabyte, location := #caller_location) {
    spall_ctx = spall.context_create("trace.spall", 10 * time.Millisecond)
    make(&backing_buffer, backing_buffer_size)
    spall_buffer = spall.buffer_create(backing_buffer, auto_cast context.user_index)
}
@(deferred_none = delete_spall_thread)
@(disabled=!SpallEnabled)
init_spall_thread :: proc (thread_index := cast(u32) context.user_index, backing_buffer_size := 10 * Megabyte, location := #caller_location, ) {
    make(&backing_buffer, backing_buffer_size)
    spall_buffer = spall.buffer_create(backing_buffer, thread_index)
    spall_begin(location.procedure)
}

@(disabled=!SpallEnabled)
delete_spall :: proc () {
    defer spall.context_destroy(&spall_ctx)
    defer delete(backing_buffer)
    defer spall.buffer_destroy(&spall_ctx, &spall_buffer)
}
@(disabled=!SpallEnabled)
delete_spall_thread :: proc () {
    defer delete(backing_buffer)
    defer spall.buffer_destroy(&spall_ctx, &spall_buffer)
    defer spall_end()
}

////////////////////////////////////////////////

@(deferred_none = spall_end)
@(disabled=!SpallEnabled) spall_proc :: proc (name: string = "", location := #caller_location) {
    spall_begin(name == "" ? location.procedure : name, location)
}

@(deferred_none = spall_end)
@(disabled=!SpallEnabled) spall_scope :: proc (name: string, location := #caller_location) {
    spall_begin(name, location)
}
@(disabled=!SpallEnabled) spall_hit :: proc (name: string, location := #caller_location) {
    spall_begin(name)
    spall_end()
}
@(disabled=!SpallEnabled) spall_begin :: proc (name: string, location := #caller_location) {
	spall._buffer_begin(&spall_ctx, &spall_buffer, name, "", location)
}
@(disabled=!SpallEnabled) spall_end :: proc () { spall._buffer_end(&spall_ctx, &spall_buffer) }
@(disabled=!SpallEnabled) spall_flush :: proc () { spall.buffer_flush(&spall_ctx, &spall_buffer) }
