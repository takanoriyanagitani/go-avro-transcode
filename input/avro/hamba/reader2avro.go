package reader2avro

import (
	"bufio"
	"context"
	"io"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	at "github.com/takanoriyanagitani/go-avro-transcode"
	util "github.com/takanoriyanagitani/go-avro-transcode/util"
)

func ConfigToOpts(c at.InputConfig) []ho.DecoderFunc {
	var hcfg ha.Config = ha.Config{}
	hcfg.MaxByteSliceSize = c.BlobSizeMax()
	var hapi ha.API = hcfg.Freeze()
	return []ho.DecoderFunc{
		ho.WithDecoderConfig(hapi),
	}
}

func ReaderToMapsWithOptionsHamba(
	r io.Reader,
	opts ...ho.DecoderFunc,
) (at.Input, error) {
	var br io.Reader = bufio.NewReader(r)

	dec, e := ho.NewDecoder(br, opts...)
	if nil != e {
		return at.Input{}, e
	}

	rows := func(yield func(map[string]any, error) bool) {
		var err error = nil
		var buf map[string]any

		for dec.HasNext() {
			err = dec.Decode(&buf)
			if !yield(buf, err) {
				return
			}
		}
	}

	var s at.AvroSchema = at.AvroSchema(dec.Schema().String())

	return at.Input{
		AvroSchema:   s,
		InputRecords: rows,
	}, nil
}

func ReaderToMapsWithConfig(
	r io.Reader,
	cfg at.InputConfig,
) (at.Input, error) {
	var opts []ho.DecoderFunc = ConfigToOpts(cfg)
	return ReaderToMapsWithOptionsHamba(r, opts...)
}

func StdinToMapsWithConfig(
	cfg at.InputConfig,
) (at.Input, error) {
	return ReaderToMapsWithConfig(os.Stdin, cfg)
}

func ConfigToStdinToMaps(cfg at.InputConfig) util.IO[at.Input] {
	return func(_ context.Context) (at.Input, error) {
		return StdinToMapsWithConfig(cfg)
	}
}

func ReaderToMaps(r io.Reader) (at.Input, error) {
	return ReaderToMapsWithOptionsHamba(r)
}

func StdinToAvroToMaps() (at.Input, error) {
	return ReaderToMaps(os.Stdin)
}

var StdinToMaps util.IO[at.Input] = util.Bind(
	util.Of(io.Reader(os.Stdin)),
	util.Lift(ReaderToMaps),
)
