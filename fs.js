var fs = require('fs')
var path = require('path')
var from = require('from2')
var mkdirp = require('mkdirp')
var duplexify = require('duplexify')
var each = require('stream-each')

module.exports = FSStorage

function FSStorage (dir) {
  if (!(this instanceof FSStorage)) return new FSStorage(dir)
  this.dir = path.resolve(dir.replace(/^fs:\/\//, ''))
}

FSStorage.prototype.list =
FSStorage.prototype.createListStream = function (opts) {
  if (!opts) opts = {}

  var self = this
  var stack = []
  var limit = opts.limit || Infinity
  var marker = (opts.marker || '').replace(/^\//, '')

  if (opts.prefix) stack.push(normalize(this.dir, opts.prefix))
  else stack.push(this.dir)

  var stream = from.obj(function read (size, cb) {
    if (!stack.length || !limit) return cb(null, null)

    var next = stack.pop()
    fs.stat(next, function (err, st) {
      if (err && err.code === 'ENOENT') return read(size, cb)
      if (err) return cb(err)
      if (st.isDirectory()) ondir()
      else onfile(st)
    })

    function ondir () {
      fs.readdir(next, function (err, files) {
        if (err) return cb(err)
        for (var i = files.length - 1; i >= 0; i--) {
          if (!/\.s3meta$/.test(files[i])) {
            stack.push(path.join(next, files[i]))
          }
        }
        read(size, cb)
      })
    }

    function onfile (st) {
      var key = next.replace(self.dir, '').slice(1).replace(/\\/g, '/')
      if (key <= marker) return read(size, cb)
      limit--
      cb(null, {
        key: key,
        modified: st.mtime,
        size: st.size
      })
    }
  })

  return stream
}

FSStorage.prototype.createWriteStream = function (key) {
  key = normalize(this.dir, key)

  var self = this
  var dup = duplexify()

  dup.setReadable(false)
  mkdirp(path.dirname(key), function (err) {
    if (err) return dup.destroy(err)
    if (dup.destroyed) return clean(self.dir, key, noop)
    dup.setWritable(fs.createWriteStream(key))
  })

  return dup
}

FSStorage.prototype.put = function (key, val, meta, cb) {
  if (typeof meta === 'function') {
    cb = meta
    meta = undefined
  }
  if (!cb) cb = noop

  key = normalize(this.dir, key)

  mkdirp(path.dirname(key), function (err) {
    if (err) return cb(err)
    fs.writeFile(key, val, function (err) {
      if (err) return cb(err)
      if (meta) {
        fs.writeFile(key + '.s3meta', JSON.stringify(meta), cb)
      } else {
        cb(null)
      }
    })
  })
}

FSStorage.prototype.createReadStream = function (key) {
  key = normalize(this.dir, key)
  return fs.createReadStream(key)
}

FSStorage.prototype.get = function (key, cb) {
  key = normalize(this.dir, key)
  fs.readFile(key, function (err, body) {
    if (err) return cb(err)
    fs.readFile(key + '.s3meta', function (err, meta) {
      // ENOENT just means no meta
      if (err && err.code !== 'ENOENT') return cb(err)
      if (meta) meta = JSON.parse(meta)
      cb(null, body, meta)
    })
  })
}

FSStorage.prototype.stat = function (key, cb) {
  key = normalize(this.dir, key)
  fs.stat(key, function (err, st) {
    if (err) return cb(err)
    cb(null, {size: st.size, modified: st.mtime})
  })
}

FSStorage.prototype.del = function (key, cb) {
  if (!cb) cb = noop

  var self = this
  key = normalize(this.dir, key)

  fs.unlink(key, function (err) {
    if (err) return cb(err)
    fs.unlink(key + '.s3meta', function (err) {
      if (err && err.code !== 'ENOENT') return cb(err)
      clean(self.dir, key, cb)
    })
  })
}

FSStorage.prototype.rename = function (src, dest, cb) {
  if (!cb) cb = noop

  var self = this
  var prefix = src

  src = normalize(this.dir, src)
  dest = normalize(this.dir, dest)

  each(this.list({prefix}), ondata, cb)

  function ondata (data, next) {
    var key = normalize(self.dir, data.key)
    rename(key, key.replace(src, dest), function (err) {
      if (err) return next(err)
      rename(key + '.s3meta', key.replace(src, dest) + '.s3meta', function (err) {
        if (err && err.code !== 'ENOENT') return next(err)
        next(null)
      })
    })
  }

  function rename (a, b, cb) {
    mkdirp(path.dirname(b), function (err) {
      if (err) return cb(err)
      fs.rename(a, b, function (err) {
        if (err) return cb(err)
        clean(self.dir, a, cb)
      })
    })
  }
}

function clean (dir, key, cb) {
  loop()

  function loop (err) {
    if (err) return cb(null)

    key = path.dirname(key)
    if (key === dir) return cb(null)

    fs.rmdir(key, loop)
  }
}

function noop () {}

function normalize (dir, key) {
  return path.join(dir, path.join('.', path.join('/', key)))
}
