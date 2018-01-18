module.exports = function (bucket, opts) {
  if (!opts) opts = {}

  if (/^fs:/.test(bucket) || opts.type === 'fs') {
    return require('./fs')(bucket, opts)
  }

  return require('./s3')(bucket, opts)
}
