#+vet !unused-procedures
#+no-instrumentation
package main

import "base:intrinsics"
import "core:simd"

Lane_Slice_Checked :: !false

////////////////////////////////////////////////
// @note A typed wrapper on wide pointers and slices

Lane :: struct ($T: typeid) {
    p: lane_umm,
}

Lane_Slice :: struct ($E: typeid) {
    p:   lane_umm,
    len: lane_u32,
}

Lane_String :: Lane_Slice(u8)

////////////////////////////////////////////////

to_lane :: proc { to_lane_slice, to_lane_array, to_lane_wide }
to_lane_slice :: proc (slice: $S/ [] $T) -> Lane_Slice(T) {
    result: Lane_Slice(T)
    result.p   = cast(lane_umm) raw_data(slice)
    result.len = cast(lane_u32) len(slice)
    return result
}
to_lane_array :: proc (array: $S/ [dynamic ]$T) -> Lane_Slice(T) {
    result := to_lane(array[:])
    return result
}
to_lane_wide :: proc (array: ^[LaneWidth] $T) -> Lane(T) {
    slice  := to_lane(array[:])
    result := lane_index_scalar(slice, lane_offset)
    return result
}

////////////////////////////////////////////////

lane_extract :: proc (lane: Lane($T), #any_int lane_index: u32) -> ^T {
    ts := transmute([LaneWidth] ^T) lane.p
    result := ts[lane_index]
    return result
}

////////////////////////////////////////////////

lane_index_offset :: proc (slice: Lane_Slice($T), offset: lane_u32, caller_location := #caller_location) -> Lane_Slice(T) {
    result := slice
    result.p += cast(lane_umm) offset * size_of(T)
    
    return result
}

lane_index :: proc { lane_index_scalar, lane_index_array }
lane_index_scalar :: proc (slice: Lane_Slice($T), index: $I, caller_location := #caller_location) -> Lane(T) {
    when Lane_Slice_Checked {
        assert(less_than(cast(lane_u32) index, slice.len) == lane_true, loc = caller_location)
    }
    result: Lane(T)
    result.p = slice.p + cast(lane_umm) index * size_of(T)
    return result
}
lane_index_array :: proc (array: Lane([$N] $T), index: $I, caller_location := #caller_location) -> Lane(T) {
    when Lane_Slice_Checked {
        assert(less_than(cast(lane_u32) index, N) == lane_true, loc = caller_location)
    }
    base   := array.p
    offset := cast(lane_umm) index * size_of(T)
    result := Lane(T) { base + offset }
    return result
} 

lane_slice :: proc { lane_slice_start, lane_slice_start_end }
lane_slice_start :: proc (slice: Lane_Slice($T), start: lane_u32, caller_location := #caller_location) -> Lane_Slice(T) {
    result := lane_slice(slice, start, slice.len, caller_location = caller_location)
    return result
}
lane_slice_start_end :: proc (slice: Lane_Slice($T), start, end: lane_u32, caller_location := #caller_location) -> Lane_Slice(T) {
    when Lane_Slice_Checked {
        assert(less_than(start, slice.len) == lane_true, loc = caller_location)
        assert(less_equal(start, end)      == lane_true, loc = caller_location)
        assert(less_equal(end, slice.len)  == lane_true, loc = caller_location)
    }
    result: Lane_Slice(T)
    result.p = slice.p + cast(lane_umm) start * size_of(T)
    result.len = end - start
    return result
}

////////////////////////////////////////////////

@(private="file") Has    :: intrinsics.type_has_field
@(private="file") Field  :: intrinsics.type_field_type
@(private="file") Offset :: offset_of_by_string

// @todo(viktor): once OLS doesn't crash anymore we can remove the parameter
lane_member :: proc { lane_member_1, lane_member_2 }
lane_member_1 :: proc (lane: Lane($T), $member: string) -> Lane(Field(T, member)) where Has(T, member) {
    offset :: Offset(T, member)
    result := Lane(Field(T, member)) { lane.p + offset }
    return result
}

lane_member_2 :: proc (lane: Lane($T), $first_member: string, $member_of_first: string) -> Lane(Field(Field(T, first_member), member_of_first)) where Has(T, first_member), Has(Field(T, first_member), member_of_first) {
    first := lane_member(lane, first_member)
    result := lane_member(first, member_of_first)
    
    return result
}

lane_member_slice :: proc (lane: Lane($T), $member: string, $member_type: typeid / [] $E) -> Lane_Slice(E) {
    slice := cast(Lane(RawSlice)) lane_member(lane, member)
    
    result: Lane_Slice(E)
    result.p   = cast(lane_umm) lane_gather(lane_member(slice, "data"))
    result.len = cast(lane_u32) lane_gather(lane_member(slice, "len"))
    
    return result
}

lane_member_string :: proc (lane: Lane($T), $member: string) -> Lane_String {
    result := lane_member_slice(lane, member, [] u8)
    return result
}

////////////////////////////////////////////////

lane_gather       :: proc { lane_gather_no_mask,       lane_gather_mask       }
lane_gather_index :: proc { lane_gather_index_no_mask, lane_gather_index_mask }
lane_gather_v     :: proc { lane_gather_v_no_mask,     lane_gather_v_mask     }

lane_gather_no_mask :: proc (lane: Lane($T)) -> #simd [LaneWidth] T {
    result := lane_gather_mask(lane, lane_true, T{})
    return result
}
lane_gather_mask :: proc (lane: Lane($T), mask: lane_u32, default: #simd [LaneWidth] T) -> #simd [LaneWidth] T {
    result := simd.gather(cast(lane_pmm) lane.p, default, mask)
    return result
}

lane_gather_index_no_mask :: proc (lane: Lane_Slice($T), index: lane_u32) -> #simd [LaneWidth] T {
    result := lane_gather_index_mask(lane, index, lane_true, T{})
    return result
}
lane_gather_index_mask :: proc (lane: Lane_Slice($T), index: lane_u32, mask: lane_u32, default: #simd [LaneWidth] T) -> #simd [LaneWidth] T {
    gather_mask := mask
    when Lane_Slice_Checked {
        gather_mask &= less_than(index, lane.len)
    }
    
    element := lane_index(lane, index)
    result  := lane_gather_mask(element, gather_mask, default)
    return result
}

lane_gather_v_no_mask :: proc (lane: Lane($T/ [$N] $E)) -> [N] #simd [LaneWidth] E {
    result: [N] #simd [LaneWidth] E
    #no_bounds_check #unroll for channel_index in cast(u32) 0..<N {
        index := lane_index(lane, channel_index)
        result[channel_index] = lane_gather(index)
    }
    return result
}
lane_gather_v_mask :: proc (lane: Lane($T/ [$N] $E), mask: lane_u32, default: [N] #simd [LaneWidth] E) -> [N] #simd [LaneWidth] E {
    result: [N] #simd [LaneWidth] E
    #no_bounds_check #unroll for channel_index in cast(u32) 0..<N {
        index := lane_index(lane, channel_index)
        result[channel_index] = lane_gather(index, mask, default[channel_index])
    }
    return result
}

////////////////////////////////////////////////

lane_scatter :: proc { 
    lane_scatter_mask,
    lane_scatter_index,
    lane_scatter_index_array,
}

lane_scatter_mask :: proc (lane: Lane($T), value: #simd [LaneWidth] T, mask := lane_true) {
    simd.scatter(cast(lane_pmm) lane.p, value, mask)
}
lane_scatter_index :: proc (lane: Lane_Slice($T), index: lane_u32, value: #simd [LaneWidth] T, mask: lane_u32) {
    element := lane_index(lane, index)
    lane_scatter(element, value, mask)
}
lane_scatter_index_array :: proc (lane: Lane($A/[$N] $T), index: lane_u32, value: #simd [LaneWidth] T, mask: lane_u32) {
    element := lane_index(lane, index)
    lane_scatter(element, value, mask)
}

lane_scatter_v :: proc (lane: Lane($T / [$N] $E), value: [N] #simd [LaneWidth] E, mask: lane_u32) {
    #no_bounds_check #unroll for channel_index in cast(u32) 0..<N {
        index := lane_index(lane, channel_index)
        lane_scatter(index, value[channel_index], mask)
    }
}