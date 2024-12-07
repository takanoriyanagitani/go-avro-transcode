package avro2writer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	at "github.com/takanoriyanagitani/go-avro-transcode"
	util "github.com/takanoriyanagitani/go-avro-transcode/util"
)

var (
	ErrInvalidCodec error = errors.New("invalid codec")
)

func InputToHambaAvroWriter(
	ctx context.Context,
	i at.Input,
	w io.Writer,
	options []ho.EncoderFunc,
) error {
	var schema string = string(i.AvroSchema)

	s, e := ha.Parse(schema)
	if nil != e {
		return e
	}

	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
		options...,
	)

	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range i.InputRecords {
		if nil != e {
			return e
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		e = enc.Encode(row)
		if nil != e {
			return e
		}
	}

	return enc.Flush()
}

var codecMap map[at.CodecName]ho.CodecName = map[at.CodecName]ho.CodecName{
	"null":      ho.Null,
	"deflate":   ho.Deflate,
	"snappy":    ho.Snappy,
	"zstandard": ho.ZStandard,
}

func ToHambaCodec(c at.CodecName) (ho.CodecName, error) {
	cn, found := codecMap[c]
	if !found {
		return cn, fmt.Errorf("%w: %v", ErrInvalidCodec, c)
	}
	return cn, nil
}

func InputToAvroToWriter(
	ctx context.Context,
	i at.Input,
	w io.Writer,
	cfg at.SimpleOutputConfig,
) error {
	hcodec, e := ToHambaCodec(cfg.Codec())
	if nil != e {
		return e
	}

	options := []ho.EncoderFunc{
		ho.WithCodec(hcodec),
	}

	return InputToHambaAvroWriter(ctx, i, w, options)
}

func InputToAvroToStdout(
	ctx context.Context,
	i at.Input,
	cfg at.SimpleOutputConfig,
) error {
	return InputToAvroToWriter(ctx, i, os.Stdout, cfg)
}

func CodecToInputToAvroToStdout(
	cfg at.SimpleOutputConfig,
) func(at.Input) util.IO[util.Void] {
	return func(i at.Input) util.IO[util.Void] {
		return func(ctx context.Context) (util.Void, error) {
			return util.Empty, InputToAvroToStdout(ctx, i, cfg)
		}
	}
}
