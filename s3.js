var S3 = require('aws-sdk/clients/s3')
var thunky = require('thunky')
var from = require('from2')
var mime = require('mime')
var each = require('stream-each')

module.exports = S3Storage

function S3Storage (bucket, opts) {
  if (!(this instanceof S3Storage)) return new S3Storage(bucket, opts)
  if (!opts) opts = {}

  this.bucket = bucket.replace(/^s3:\/\//, '')
  this.region = opts.region || 'us-west-2'
  this.s3 = new S3(opts)
  this.ready = thunky(this._open.bind(this))
}

S3Storage.prototype.list =
S3Storage.prototype.createListStream = function (opts) {
  if (!opts) opts = {}

  var self = this
  var marker = opts.marker || ''
  var limit = opts.limit || Infinity
  var stream = from.obj(read)
  var open = false

  return stream

  function openAndRead (size, cb) {
    open = true
    self.ready(function (err) {
      if (err) return cb(err)
      read(size, cb)
    })
  }

  function read (size, cb) {
    if (!open) return openAndRead(size, cb)
    self.s3.listObjects({
      Bucket: self.bucket,
      Prefix: opts.prefix,
      Marker: marker
    }, function (err, res) {
      if (err) return cb(err)

      var contents = res.Contents
      var len = Math.min(contents.length, limit)
      if (!len) return cb(null, null)

      for (var i = 0; i < len; i++) {
        var c = contents[i]
        var next = {key: c.Key, size: c.Size, modified: c.LastModified}
        limit--
        marker = c.Key
        if (i < contents.length - 1) stream.push(next)
        else cb(null, next)
      }
    })
  }
}

S3Storage.prototype.rename = function (from, to, cb) {
  var self = this
  var stream = this.createKeyStream({prefix: from})

  each(stream, ondata, cb)

  function ondata (key, next) {
    self.s3.copyObject({
      Bucket: self.bucket,
      CopySource: self.bucket + '/' + key,
      Key: key.replace(from, to) // from is *always* the first part of key
    }, function (err) {
      if (err) return next(err)
      self.del(key, next)
    })
  }
}

S3Storage.prototype.del = function (key, cb) {
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    self.s3.deleteObject({
      Bucket: self.bucket,
      Key: key
    }, cb)
  })
}

S3Storage.prototype.put = function (key, buf, cb) {
  if (!cb) cb = noop

  var self = this
  var type = mime.getType(key)

  this.ready(function (err) {
    if (err) return cb(err)
    self.s3.putObject({
      Bucket: self.bucket,
      ContentType: type,
      Key: key,
      Body: buf
    }, cb)
  })
}

S3Storage.prototype.get = function (key, cb) {
  if (!cb) cb = noop

  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.s3.getObject({
      Bucket: self.bucket,
      Key: key
    }, function (err, data) {
      if (err) return cb(err)
      cb(null, data.Body)
    })
  })
}

S3Storage.prototype._open = function (cb) {
  this.s3.createBucket({
    Bucket: this.bucket,
    CreateBucketConfiguration: {
      LocationConstraint: this.region
    }
  }, function (err) {
    // if we get a 409 it simply means we already created the bucket
    if (err && err.statusCode !== 409) return cb(err)
    cb(null)
  })
}

function noop () {}
