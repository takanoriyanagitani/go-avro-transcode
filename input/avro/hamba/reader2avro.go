package reader2avro

import (
	"bufio"
	"io"
	"os"

	ho "github.com/hamba/avro/v2/ocf"

	at "github.com/takanoriyanagitani/go-avro-transcode"
	util "github.com/takanoriyanagitani/go-avro-transcode/util"
)

func ReaderToMaps(r io.Reader) (at.Input, error) {
	var br io.Reader = bufio.NewReader(r)

	dec, e := ho.NewDecoder(br)
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

func StdinToAvroToMaps() (at.Input, error) {
	return ReaderToMaps(os.Stdin)
}

var StdinToMaps util.IO[at.Input] = util.Bind(
	util.Of(io.Reader(os.Stdin)),
	util.Lift(ReaderToMaps),
)
