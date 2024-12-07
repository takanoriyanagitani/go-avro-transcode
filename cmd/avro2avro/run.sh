#!/bin/sh

#export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
#cat sample.d/sample.jsonl | json2avrows > ./sample.d/sample.avro

export ENV_CODEC_NAME=zstandard
export ENV_CODEC_NAME=snappy
export ENV_CODEC_NAME=null
export ENV_CODEC_NAME=deflate

cat sample.d/sample.avro |
  ./avro2avro |
  rq \
  	-a \
	-J
