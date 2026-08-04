package flow

// Heap profiles ignore pprof labels. The six selected noinline frames below
// encode a 24-bit job slot in their function names; memorySlotFromProfileSample
// decodes those names. The repetition is intentional: function identity is the payload.
type memoryMarker func(uint32, func())

var memoryMarkers5 = [16]memoryMarker{
	memoryMarker50,
	memoryMarker51,
	memoryMarker52,
	memoryMarker53,
	memoryMarker54,
	memoryMarker55,
	memoryMarker56,
	memoryMarker57,
	memoryMarker58,
	memoryMarker59,
	memoryMarker5a,
	memoryMarker5b,
	memoryMarker5c,
	memoryMarker5d,
	memoryMarker5e,
	memoryMarker5f,
}

var memoryMarkers4 = [16]memoryMarker{
	memoryMarker40,
	memoryMarker41,
	memoryMarker42,
	memoryMarker43,
	memoryMarker44,
	memoryMarker45,
	memoryMarker46,
	memoryMarker47,
	memoryMarker48,
	memoryMarker49,
	memoryMarker4a,
	memoryMarker4b,
	memoryMarker4c,
	memoryMarker4d,
	memoryMarker4e,
	memoryMarker4f,
}

var memoryMarkers3 = [16]memoryMarker{
	memoryMarker30,
	memoryMarker31,
	memoryMarker32,
	memoryMarker33,
	memoryMarker34,
	memoryMarker35,
	memoryMarker36,
	memoryMarker37,
	memoryMarker38,
	memoryMarker39,
	memoryMarker3a,
	memoryMarker3b,
	memoryMarker3c,
	memoryMarker3d,
	memoryMarker3e,
	memoryMarker3f,
}

var memoryMarkers2 = [16]memoryMarker{
	memoryMarker20,
	memoryMarker21,
	memoryMarker22,
	memoryMarker23,
	memoryMarker24,
	memoryMarker25,
	memoryMarker26,
	memoryMarker27,
	memoryMarker28,
	memoryMarker29,
	memoryMarker2a,
	memoryMarker2b,
	memoryMarker2c,
	memoryMarker2d,
	memoryMarker2e,
	memoryMarker2f,
}

var memoryMarkers1 = [16]memoryMarker{
	memoryMarker10,
	memoryMarker11,
	memoryMarker12,
	memoryMarker13,
	memoryMarker14,
	memoryMarker15,
	memoryMarker16,
	memoryMarker17,
	memoryMarker18,
	memoryMarker19,
	memoryMarker1a,
	memoryMarker1b,
	memoryMarker1c,
	memoryMarker1d,
	memoryMarker1e,
	memoryMarker1f,
}

var memoryMarkers0 = [16]memoryMarker{
	memoryMarker00,
	memoryMarker01,
	memoryMarker02,
	memoryMarker03,
	memoryMarker04,
	memoryMarker05,
	memoryMarker06,
	memoryMarker07,
	memoryMarker08,
	memoryMarker09,
	memoryMarker0a,
	memoryMarker0b,
	memoryMarker0c,
	memoryMarker0d,
	memoryMarker0e,
	memoryMarker0f,
}

//go:noinline
func memoryMarker50(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker51(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker52(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker53(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker54(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker55(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker56(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker57(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker58(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker59(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5a(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5b(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5c(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5d(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5e(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker5f(slot uint32, fn func()) {
	memoryMarkers4[(slot>>16)&0xf](slot, fn)
}

//go:noinline
func memoryMarker40(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker41(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker42(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker43(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker44(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker45(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker46(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker47(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker48(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker49(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4a(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4b(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4c(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4d(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4e(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker4f(slot uint32, fn func()) {
	memoryMarkers3[(slot>>12)&0xf](slot, fn)
}

//go:noinline
func memoryMarker30(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker31(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker32(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker33(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker34(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker35(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker36(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker37(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker38(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker39(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3a(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3b(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3c(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3d(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3e(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker3f(slot uint32, fn func()) {
	memoryMarkers2[(slot>>8)&0xf](slot, fn)
}

//go:noinline
func memoryMarker20(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker21(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker22(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker23(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker24(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker25(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker26(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker27(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker28(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker29(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2a(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2b(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2c(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2d(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2e(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker2f(slot uint32, fn func()) {
	memoryMarkers1[(slot>>4)&0xf](slot, fn)
}

//go:noinline
func memoryMarker10(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker11(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker12(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker13(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker14(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker15(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker16(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker17(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker18(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker19(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1a(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1b(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1c(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1d(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1e(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker1f(slot uint32, fn func()) {
	memoryMarkers0[(slot>>0)&0xf](slot, fn)
}

//go:noinline
func memoryMarker00(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker01(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker02(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker03(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker04(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker05(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker06(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker07(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker08(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker09(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0a(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0b(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0c(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0d(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0e(slot uint32, fn func()) {
	fn()
}

//go:noinline
func memoryMarker0f(slot uint32, fn func()) {
	fn()
}
