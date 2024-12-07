package avro2avro

import (
	"iter"
)

type AvroSchema string

type InputRecords iter.Seq2[map[string]any, error]

type Input struct {
	AvroSchema
	InputRecords
}

type CodecName string

const (
	CodecNull    CodecName = "null"
	CodecDeflate CodecName = "deflate"
	CodecSnappy  CodecName = "snappy"
	CodecZstd    CodecName = "zstandard"
	CodecBzip2   CodecName = "bzip2"
	CodecXz      CodecName = "xz"
)

const (
	BlockLengthDefault int = 1000
)

type SimpleOutputConfig struct {
	blockLength int
	codec       CodecName
}

func (s SimpleOutputConfig) WithBlockLength(l int) SimpleOutputConfig {
	s.blockLength = l
	return s
}

func (s SimpleOutputConfig) WithCodec(c CodecName) SimpleOutputConfig {
	s.codec = c
	return s
}

func (s SimpleOutputConfig) Codec() CodecName { return s.codec }
func (s SimpleOutputConfig) BlockLength() int { return s.blockLength }

var SimpleOutputConfigDefault SimpleOutputConfig = SimpleOutputConfig{}.
	WithBlockLength(1000).
	WithCodec(CodecNull)
