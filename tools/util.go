package tools

import (
	"crypto/sha1"
	"encoding/binary"
	"io"
	"fmt"
	"math"
	"reflect"
	"unsafe"
	"runtime"
	"github.com/shopspring/decimal"
	"strconv"
	"bytes"
)

// https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
// returns the number read, whether the value is NULL and the number of bytes read
func ReadLengthEncodedInteger(b []byte) (num uint64, isNull bool, n int) {
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
func ReadLengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := ReadLengthEncodedInteger(b)
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
func Scramble41(scramble, password []byte) []byte {
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

func ByteCountFromBitCount(n int) int {
	return (n + 7) / 8
}

func ReadBinaryInt8(b []byte) int8 {
	return int8(b[0])
}

func ReadBinaryUint8(b []byte) uint8 {
	return b[0]
}

func ReadBinaryInt16(data []byte) int16 {
	return int16(binary.LittleEndian.Uint16(data))
}

func ReadBinaryUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func ReadBinaryInt24(data []byte) int32 {
	u32 := uint32(ReadBinaryUint24(data))
	if u32&0x00800000 != 0 {
		u32 |= 0xFF000000
	}
	return int32(u32)
}

func ReadBinaryUint24(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
}

func ReadBinaryInt32(data []byte) int32 {
	return int32(binary.LittleEndian.Uint32(data))
}

func ReadBinaryUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func ReadBinaryInt64(data []byte) int64 {
	return int64(binary.LittleEndian.Uint64(data))
}

func ReadBinaryUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func ReadBinaryFloat32(data []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(data))
}

func ReadBinaryFloat64(data []byte) float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(data))
}

// little-endian
func ReadLittleEndianFixedLengthInteger(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

// big-endian
func ReadBigEndianFixedLengthInteger(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

//func PutLengthEncodedInteger(n uint64) []byte {
//	switch {
//	case n <= 250:
//		return []byte{byte(n)}
//
//	case n <= 0xffff:
//		return []byte{0xfc, byte(n), byte(n >> 8)}
//
//	case n <= 0xffffff:
//		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
//
//	case n <= 0xffffffffffffffff:
//		return []byte{0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
//			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56)}
//	}
//	return nil
//}

func SkipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := ReadLengthEncodedInteger(b)
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
func BitCount(bitmap []byte) int {
	var n uint32 = 0

	for _, bit := range bitmap {
		n = n + uint32(bitCountInByte[bit])
	}

	return int(n)
}

// Get the bit set at offset position in bitmap
func GetBit(bitmap []byte, off int) byte {
	bit := bitmap[off/8]
	return bit & (1 << (uint(off) & 7))
}

func FormatBinaryDate(n int, data []byte) ([]byte, error) {
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

func FormatBinaryDateTime(n int, data []byte) ([]byte, error) {
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

func FormatBinaryTime(n int, data []byte) ([]byte, error) {
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

// vitess:hack string 和 slice no-copy转换,string和slice的转换只需要拷贝底层的指针，而不是内存拷贝
func UnsafeGetString(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

func PStack() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[0:n])
}

const digitsPerInteger = 9

func ParseDecimalType(data []byte, precision, decimals int, useDecimal bool) (interface{}, int, error) {
	integral := precision - decimals
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	// Going to destroy data
	data = buf

	// Support negative decimals:
	// The sign is encoded in the high bit of the the byte, but this bit may also be used in the value
	value := uint32(data[0])
	var res bytes.Buffer
	var mask uint32 = 0
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	// Clear sign
	data[0] ^= 0x80

	pos, value := parseDecimalDecompressValue(compIntegral, data, uint8(mask))
	res.WriteString(fmt.Sprintf("%d", value))

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos = pos + 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos = pos + 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := parseDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		pos = pos + size
	}
	if useDecimal {
		f, err := decimal.NewFromString(UnsafeGetString(res.Bytes()))
		return f, pos, err
	}

	f, err := strconv.ParseFloat(UnsafeGetString(res.Bytes()), 64)
	return f, pos, err
}

var compressedBytes = []int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

func parseDecimalDecompressValue(compIndx int, data []byte, mask uint8) (size int, value uint32) {
	size = compressedBytes[compIndx]
	buff := make([]byte, size)
	for i := 0; i < size; i++ {
		buff[i] = data[i] ^ mask
	}
	value = uint32(ReadBigEndianFixedLengthInteger(buff))
	return
}

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}
