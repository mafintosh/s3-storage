var S3 = require('aws-sdk/clients/s3')
var thunky = require('thunky')
var from = require('from2')
var mime = require('mime')
var each = require('stream-each')
var stream = require('readable-stream')
var duplexify = require('duplexify')
var bulk = require('bulk-write-stream')

module.exports = S3Storage

function S3Storage (bucket, opts) {
  if (!(this instanceof S3Storage)) return new S3Storage(bucket, opts)
  if (!opts) opts = {}

  this.bucket = bucket.replace(/^s3:\/\//, '')
  this.region = opts.region || 'us-east-1' // default aws region
  this.s3 = new S3(opts)
  this.ready = thunky(this._open.bind(this))
  this.prefix = opts.prefix || null
}

S3Storage.prototype.list =
S3Storage.prototype.createListStream = function (opts) {
  if (!opts) opts = {}

  var self = this
  var marker = join(self.prefix, opts.marker || null)
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
      Prefix: join(self.prefix, opts.prefix || null),
      Marker: marker
    }, function (err, res) {
      if (err) return cb(err)

      var contents = res.Contents
      var len = Math.min(contents.length, limit)
      if (!len) return cb(null, null)

      for (var i = 0; i < len; i++) {
        var c = contents[i]
        var next = {key: c.Key.replace(self.prefix + '/', ''), size: c.Size, modified: c.LastModified}
        limit--
        marker = c.Key
        if (i < len - 1) stream.push(next)
        else cb(null, next)
      }
    })
  }
}

S3Storage.prototype.rename = function (from, to, cb) {
  var self = this
  var stream = this.list({prefix: from})

  each(stream, ondata, cb)

  function ondata (data, next) {
    var key = data.key

    self.s3.copyObject({
      Bucket: self.bucket,
      CopySource: self.bucket + '/' + join(self.prefix, key),
      Key: join(self.prefix, key.replace(from, to)) // from is *always* in the beginning of the key
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
      Key: join(self.prefix, key)
    }, cb)
  })
}

S3Storage.prototype.stat = function (key, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.s3.headObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key)
    }, function (err, data) {
      if (err) return cb(err)
      cb(null, {
        size: data.ContentLength,
        modified: new Date(data.LastModified)
      })
    })
  })
}

S3Storage.prototype.createReadStream = function (key) {
  var proxy = duplexify()
  var self = this

  proxy.setWritable(false)
  this.ready(function (err) {
    if (err) return proxy.destroy(err)
    proxy.setReadable(self.s3.getObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key)
    }).createReadStream())
  })

  return proxy
}

S3Storage.prototype.createWriteStream = function (key, opts) {
  if (typeof opts === 'number') opts = {length: opts}
  if (!opts) opts = {}

  var self = this
  var ondrain = null
  var onflush = null
  var flushed = false
  var proxy = new stream.Readable({read: read})
  var ws = bulk(write, flush)

  this.ready(function (err) {
    if (err) {
      proxy.push(null)
      ws.destroy(err)
      return
    }

    self.s3.putObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key),
      ContentLength: opts.length,
      Body: proxy
    }, function (err) {
      if (err) return ws.destroy(err)
      flushed = true
      if (onflush) onflush()
    })
  })

  return ws

  function read () {
    if (!ondrain) return
    var cb = ondrain
    ondrain = null
    cb()
  }

  function write (data, cb) {
    var drained = true
    for (var i = 0; i < data.length; i++) drained = proxy.push(data[i])
    if (!drained) ondrain = cb
    else cb()
  }

  function flush (cb) {
    if (flushed) return cb()
    onflush = cb
  }
}

S3Storage.prototype.put = function (key, buf, meta, cb) {
  if (typeof meta === 'function') {
    cb = meta
    meta = undefined
  }
  if (!cb) cb = noop

  var self = this
  var type = mime.getType(key)

  this.ready(function (err) {
    if (err) return cb(err)

    self.s3.putObject({
      Bucket: self.bucket,
      ContentType: type,
      Key: join(self.prefix, key),
      Body: buf,
      Metadata: meta
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
      Key: join(self.prefix, key)
    }, function (err, data) {
      if (err) return cb(err)
      cb(null, data.Body, data.Metadata)
    })
  })
}

S3Storage.prototype._open = function (cb) {
  var opts = this.region === 'us-east-1'
    ? {Bucket: this.bucket}
    : {Bucket: this.bucket, CreateBucketConfiguration: {LocationConstraint: this.region}}

  this.s3.createBucket(opts, function (err) {
    // if we get a 409 it simply means we already created the bucket
    // if we get a 403 it is access denied, but thats prob because the AWS user does not have that perm
    if (err && err.statusCode !== 409 && err.statusCode !== 403) return cb(err)
    cb(null)
  })
}

function noop () {}

function join (...args) {
  return args.filter(s => s != null).join('/').replace(/\/{2,}/g, '/')
}
