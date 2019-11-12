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

S3Storage.prototype.versions = function (key, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.s3.listObjectVersions({
      Bucket: self.bucket,
      Prefix: join(self.prefix, key)
    }, function (err, res) {
      if (err) return cb(err)

      var v = res.Versions.map(function (v) {
        return {
          key: v.Key.replace(self.prefix + '/', ''),
          latest: v.IsLatest,
          version: v.VersionId,
          modified: v.LastModified
        }
      })

      cb(null, v)
    })
  })
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

S3Storage.prototype.del = function (key, opts, cb) {
  if (typeof opts === 'function') return this.del(key, null, opts)

  var v = opts && opts.version
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)

    self.s3.deleteObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key),
      VersionId: v
    }, cb)
  })
}

S3Storage.prototype.delBatch = function (objects, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)

    self.s3.deleteObjects({
      Bucket: self.bucket,
      Delete: {
        Objects: objects.map(function (obj) {
          return {
            Key: join(self.prefix, obj.key),
            VersionId: obj.version
          }
        })
      }
    }, cb)
  })
}

S3Storage.prototype.exists = function (key, opts, cb) {
  if (typeof opts === 'function') return this.exists(key, null, opts)

  this.stat(key, opts, function (err, st) {
    if (err && err.code === 'NotFound') return cb(null, false)
    if (err) return cb(err, false)
    cb(null, true)
  })
}

S3Storage.prototype.stat = function (key, opts, cb) {
  if (typeof opts === 'function') return this.stat(key, null, opts)

  var v = opts && opts.version
  var self = this

  this.ready(function (err) {
    if (err) return cb(err)
    self.s3.headObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key),
      VersionId: v
    }, function (err, data) {
      if (err) return cb(err)
      cb(null, {
        size: data.ContentLength,
        modified: new Date(data.LastModified)
      })
    })
  })
}

S3Storage.prototype.createReadStream = function (key, opts) {
  var proxy = duplexify()
  var self = this
  var v = opts && opts.version

  var Range = (opts && (typeof opts.start === 'number' || typeof opts.end === 'number'))
    ? 'bytes=' + (opts.start || 0) + '-' + (typeof opts.end === 'number' ? opts.end : '')
    : undefined

  proxy.setWritable(false)
  this.ready(function (err) {
    if (err) return proxy.destroy(err)
    proxy.setReadable(self.s3.getObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key),
      VersionId: v,
      Range
    }).createReadStream())
  })

  return proxy
}

S3Storage.prototype.createMultipartStream = function (key, opts) {
  if (typeof opts === 'number') opts = {length: opts}
  if (!opts) opts = {}

  var self = this
  var pending = null
  var batchSize = opts.batchSize || 4 * 1024 * 1024 * 1024
  var lastSize = (opts.length % batchSize) || batchSize
  var ws = bulk(write, flush)
  var ondrain = null
  var onflush = null
  var flushed = false
  var proxy = null
  var missing = 0
  var uploadId = null
  var part = 1
  var parts = Math.ceil(opts.length / batchSize)
  var uploadedParts = new Array(parts)
  var completed = 0
  var s3Key = join(self.prefix, key)

  this.ready(function (err) {
    if (err) return ws.destroy(err)

    self.s3.createMultipartUpload({
      Bucket: self.bucket,
      Key: s3Key
    }, function (err, res) {
      if (err) return ws.destroy(err)
      uploadId = res.UploadId
      if (ws.destroyed) return cleanup()
      if (pending) write(pending[0], pending[1])
    })
  })

  ws.on('close', function () {
    if (completed !== parts) cleanup()
  })

  return ws

  function cleanup () {
    if (!uploadId) return
    self.s3.abortMultipartUpload({
      UploadId: uploadId,
      Bucket: self.bucket,
      Key: s3Key
    }, function () {
      // ... do nothing
    })
  }

  function write (data, cb) {
    if (!uploadId) {
      pending = [data, cb]
      return
    }

    var drained = true
    for (var i = 0; i < data.length; i++) {
      var d = data[i]

      if (!missing) {
        proxy = new stream.Readable({ read })
        missing = addPart(proxy)
        if (!missing) return cb(new Error('Writing too much'))
      }

      if (d.length < missing) {
        drained = proxy.push(d)
        missing -= d.length
      } else if (d.length === missing) {
        proxy.push(d)
        missing = 0
        proxy.push(null)
        drained = true
      } else {
        proxy.push(d.slice(0, missing))
        data[i--] = d.slice(missing)
        missing = 0
        proxy.push(null)
        drained = true
      }
    }

    if (!drained) ondrain = cb
    else cb()
  }

  function flush (cb) {
    if (flushed) return cb()
    onflush = cb
  }

  function read () {
    if (!ondrain) return
    var cb = ondrain
    ondrain = null
    cb()
  }

  function addPart (body) {
    var len = part === parts ? lastSize : batchSize
    if (part > parts) return 0

    var PartNumber = part++
    self.s3.uploadPart({
      UploadId: uploadId,
      Bucket: self.bucket,
      Key: s3Key,
      Body: body,
      PartNumber,
      ContentLength: len
    }, function (err, res) {
      if (err) return ws.destroy(err)

      uploadedParts[PartNumber - 1] = {
        ETag: res.ETag,
        PartNumber
      }

      if (++completed !== parts) return

      self.s3.completeMultipartUpload({
        UploadId: uploadId,
        Bucket: self.bucket,
        Key: s3Key,
        MultipartUpload: {
          Parts: uploadedParts
        }
      }, function (err) {
        if (err) return ws.destroy(err)

        flushed = true
        if (onflush) onflush()
      })
    })

    return len
  }
}

S3Storage.prototype.createWriteStream = function (key, opts) {
  if (typeof opts === 'number') opts = {length: opts}
  if (!opts) opts = {}
  if (opts.length > 4 * 1024 * 1024 * 1024) return this.createMultipartStream(key, opts)

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

S3Storage.prototype.get = function (key, opts, cb) {
  if (typeof opts === 'function') return this.get(key, null, opts)
  if (!cb) cb = noop

  var v = opts && opts.version
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)

    self.s3.getObject({
      Bucket: self.bucket,
      Key: join(self.prefix, key),
      VersionId: v
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
