package driver

import (
	"unsafe"
	"reflect"
	"crypto/sha1"
	"encoding/binary"
	"io"
	"fmt"
	"math"
	"strings"
	"strconv"
	"github.com/juju/errors"
)

// https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
// returns the number read, whether the value is NULL and the number of bytes read
func readLengthEncodedInteger(b []byte) (num uint64, isNull bool, n int) {
	// See issue #349
	if len(b) == 0 {
		return 0, true, 1
	}
	switch b[0] {

	// 251: NULL
	case 0xfb:
		n = 1
		isNull = true
		return

		// 252: value of following 2
	case 0xfc:
		num = uint64(b[1]) | uint64(b[2])<<8
		n = 3
		return

		// 253: value of following 3
	case 0xfd:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
		n = 4
		return

		// 254: value of following 8
	case 0xfe:
		num = uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
			uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
			uint64(b[7])<<48 | uint64(b[8])<<56
		n = 9
		return
	}

	// 0-250: value of first byte
	num = uint64(b[0])
	n = 1
	return
}

// https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
// returns the string read as a bytes slice, wheter the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func readLengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := readLengthEncodedInteger(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

// scramble41() returns a scramble buffer based on the following formula:
// SHA1(password) XOR SHA1(20-byte public seed from server CONCAT SHA1(SHA1(password)))
func scramble41(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	hash := sha1.New()

	// Stage 1 hash: SHA1(password)
	hash.Write(password)
	stage1 := hash.Sum(nil)

	// Stage 2 hash: SHA1(SHA1(password))
	hash.Reset()
	hash.Write(stage1)
	stage2 := hash.Sum(nil)

	// Scramble hash
	hash.Reset()
	hash.Write(scramble)
	hash.Write(stage2)
	result := hash.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range result {
		result[i] ^= stage1[i]
	}
	return result
}

func byteCountFromBitCount(n int) int {
	return (n + 7) / 8
}

func readBinaryInt8(b []byte) int8 {
	return int8(b[0])
}

func readBinaryUint8(b []byte) uint8 {
	return b[0]
}

func readBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}

func readBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func readBinaryInt24(data []byte) int32 {
	u32 := uint32(readBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}

func readBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func readBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}

func readBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func readBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}

func readBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func readBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func readBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

// little-endian
func readLittleEndianFixedLengthInteger(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

// big-endian
func readBigEndianFixedLengthInteger(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

func putLengthEncodedInteger(n uint64) []byte {
	switch {
	case n <= 250:
		return []byte{byte(n)}

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	case n <= 0xffffffffffffffff:
		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
	}
	return nil
}

func skipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := readLengthEncodedInteger(b)
	if num < 1 {
		return n, nil
	}

	n = n + int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

var bitCountInByte = [256]uint8{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

// Calculate total bit counts in a bitmap
func bitCount(bitmap []byte) int {
	var n uint32 = 0

	for _, bit := range bitmap {
		n = n + uint32(bitCountInByte[bit])
	}

	return int(n)
}

// Get the bit set at offset position in bitmap
func getBit(bitmap []byte, off int) byte {
	bit := bitmap[off/8]
	return bit & (1 << (uint(off) & 7))
}

func formatBinaryDate(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	default:
		return nil, fmt.Errorf("invalid date packet length %d", n)
	}
}

func formatBinaryDateTime(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00 00:00:00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	case 7:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6])), nil
	case 11:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d.%06d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6],
			binary.LittleEndian.Uint32(data[7:11]))), nil
	default:
		return nil, fmt.Errorf("invalid datetime packet length %d", n)
	}
}

func formatBinaryTime(n int, data []byte) ([]byte, error) {
	if n == 0 {
		return []byte("0000-00-00"), nil
	}

	var sign byte
	if data[0] == 1 {
		sign = byte('-')
	}

	switch n {
	case 8:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
		)), nil
	case 12:
		return []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d.%06d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
			binary.LittleEndian.Uint32(data[8:12]),
		)), nil
	default:
		return nil, fmt.Errorf("invalid time packet length %d", n)
	}
}

func writeBinaryUint16(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func writeBinaryUint32(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func writeBinaryUint64(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

// vitess:hack string 和 slice no-copy转换,string和slice的转换只需要拷贝底层的指针，而不是内存拷贝
func unsafeGetString(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// vitess:hack string 和 slice no-copy转换,string和slice的转换只需要拷贝底层的指针，而不是内存拷贝
func unsafeGetSlice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

// parses the provided character set.
// Returns default charset(utf8) if it can't.
func parseCharset(cs string) uint8 {
	// Check if it's empty, return utf8. This is a reasonable default.
	if cs == "" {
		return CharacterSetUtf8
	}

	// Check if it's in our map.
	charset, ok := CharacterSetMap[strings.ToLower(cs)]
	if ok {
		return charset
	}

	// As a fallback, try to readFieldPacket a number. So we support more values.
	if i, err := strconv.ParseInt(cs, 10, 8); err == nil {
		return uint8(i)
	}
	errLog.Print(errors.Errorf("failed to interpret character set '%v'. Try using an integer value if needed", cs))
	// No luck. return utf8
	return CharacterSetUtf8
}
