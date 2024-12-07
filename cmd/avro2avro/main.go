package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	at "github.com/takanoriyanagitani/go-avro-transcode"
	util "github.com/takanoriyanagitani/go-avro-transcode/util"

	ah "github.com/takanoriyanagitani/go-avro-transcode/input/avro/hamba"
	oh "github.com/takanoriyanagitani/go-avro-transcode/output/avro/hamba"
)

func GetEnvByKey(key string) util.IO[string] {
	return func(_ context.Context) (string, error) {
		val, found := os.LookupEnv(key)
		if !found {
			return "", fmt.Errorf("env var %s missing", key)
		}
		return val, nil
	}
}

var inputs util.IO[at.Input] = ah.StdinToMaps

var codec util.IO[at.CodecName] = util.Bind(
	GetEnvByKey("ENV_CODEC_NAME"),
	util.Lift(func(s string) (at.CodecName, error) {
		return at.CodecName(s), nil
	}),
)

var blockSize util.IO[int] = util.Bind(
	GetEnvByKey("ENV_BLOCK_SIZE"),
	util.Lift(func(s string) (int, error) {
		return strconv.Atoi(s)
	}),
).OrElse(util.Of(at.BlockLengthDefault))

var cfg util.IO[at.SimpleOutputConfig] = util.Bind(
	codec,
	func(c at.CodecName) util.IO[at.SimpleOutputConfig] {
		return util.Bind(
			blockSize,
			util.Lift(func(i int) (at.SimpleOutputConfig, error) {
				return at.SimpleOutputConfigDefault.
					WithCodec(c).
					WithBlockLength(i), nil
			}),
		)
	},
)

var stdin2avro2stdout util.IO[util.Void] = util.Bind(
	cfg,
	func(c at.SimpleOutputConfig) util.IO[util.Void] {
		var input2stdout func(at.Input) util.IO[util.Void] = oh.
			CodecToInputToAvroToStdout(c)
		return util.Bind(
			inputs,
			input2stdout,
		)
	},
)

func sub(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	_, e := stdin2avro2stdout(ctx)
	return e
}

func main() {
	e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
