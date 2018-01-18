var fs = require('fs')
var path = require('path')
var from = require('from2')
var mkdirp = require('mkdirp')
var duplexify = require('duplexify')

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
          stack.push(path.join(next, files[i]))
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

FSStorage.prototype.put = function (key, val, cb) {
  if (!cb) cb = noop

  key = normalize(this.dir, key)
  mkdirp(path.dirname(key), function (err) {
    if (err) return cb(err)
    fs.writeFile(key, val, cb)
  })
}

FSStorage.prototype.createReadStream = function (key) {
  key = normalize(this.dir, key)
  return fs.createReadStream(key)
}

FSStorage.prototype.get = function (key, cb) {
  key = normalize(this.dir, key)
  fs.readFile(key, cb)
}

FSStorage.prototype.del = function (key, cb) {
  if (!cb) cb = noop

  var self = this
  key = normalize(this.dir, key)

  fs.unlink(key, function (err) {
    if (err) return cb(err)
    clean(self.dir, key, cb)
  })
}

FSStorage.prototype.rename = function (src, dest, cb) {
  src = normalize(this.dir, src)
  dest = normalize(this.dir, src)
  fs.rename(src, dest, cb || noop)
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
