'use strict';

const PARQUET_COMPRESSION_METHODS = {
  'UNCOMPRESSED': {
    deflate: deflate_identity,
    inflate: inflate_identity
  },
  'GZIP': {
    deflate: deflate_gzip,
    inflate: inflate_gzip
  },
  'SNAPPY': {
    deflate: deflate_snappy,
    inflate: inflate_snappy
  },
  'LZO': {
    deflate: deflate_lzo,
    inflate: inflate_lzo
  },
  'BROTLI': {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  }
};

/**
 * Deflate a value using compression method `method`
 */
function deflate(method, value) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value) {
  return value;
}

function deflate_gzip(value) {
  const zlib = require('zlib');
  return zlib.gzipSync(value);
}

function deflate_snappy(value) {
  const snappy = require('snappyjs');
  return snappy.compress(value);
}

function deflate_lzo(value) {
  const lzo = require('lzo');
  return lzo.compress(value);
}

function deflate_brotli(value) {
  const brotli = require('brotli');
  return new Buffer(brotli.compress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  }));
}

/**
 * Inflate a value using compression method `method`
 */
function inflate(method, value) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

function inflate_identity(value) {
  return value;
}

function inflate_gzip(value) {
  const zlib = require('zlib');
  return zlib.gunzipSync(value);
}

function inflate_snappy(value) {
  const snappy = require('snappyjs');
  return snappy.uncompress(value);
}

function inflate_lzo(value) {
  const lzo = require('lzo');
  return lzo.decompress(value);
}

function inflate_brotli(value) {
  const brotli = require('brotli');
  return new Buffer(brotli.decompress(value));
}

module.exports = { PARQUET_COMPRESSION_METHODS, deflate, inflate };

