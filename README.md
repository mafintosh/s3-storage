# s3-storage

Small module wrapper for the AWS sdk that allows you to easily use s3.

Supports the local file system as well with the same set of operations so you can easily develop without having internet access

```
npm install s3-storage
```

[![build status](https://travis-ci.org/mafintosh/s3-storage.svg?branch=master)](https://travis-ci.org/mafintosh/s3-storage)

## Usage

``` js
var s3 = require('s3-storage')('my-bucket', {
  secretAccessKey: '...',
  accessKeyId: '...'
})

s3.put('hello', 'world', function () {
  s3.get('hello', console.log)
})
```

Or if you want use the file system locally instead

``` js
var s3 = require('s3-storage')('my-bucket', {
  type: 'fs' // will store the data in ./my-bucket
})

// s3 has the same api
```

## API

#### `var s3 = s3storage(bucket, [options])`

Make a new storage instance. Options include:

``` js
{
  type: 's3' | 'fs' // defaults to s3
  secretAccessKey: '...'
  accessKeyId: '...', // both forwarded to the AWS sdk
  region: 'us-west-2', // the default region
  // Prefix all operations. eg `test` to prefix all under `test/`
  // note that '' results in the empty prefix, which means all your objects
  // will go in the folder `/`
  prefix: null
}
```

#### `s3.put(key, value, [metadata], [callback])`

Write a new value.

#### `s3.get(key, callback)`

Read a value out

#### `s3.del(key, [callback])`

Delete a value

#### `s3.mdel(objects, [callback])`

Delete multiple values. Objects should look like this:

```js
{
  key: `value/key`, // required
  version: `version` // optional
}
```

#### `s3.createReadStream(key, options)`

Create a readable stream to a key.

#### `s3.createWriteStream(key, options)`

Create a writeable stream to a key. Options include

``` js
{
  length: sizeOfStream, // required
}
```

#### `s3.rename(src, dest, [callback])`

Rename a folder/file

#### `s3.exists(key, callback)`

Check if a key exists.

#### `s3.stat(key, callback)`

Return stat info about a key. The returned object looks like this:

``` js
{
  size: sizeOfValue,
  modified: lastModifiedDate
}
```

#### `s3.versions(key, callback)`

Get a list of versions of a specific key. You can pass the version to `stat`, `exists`, `get`, `createReadStream` and `del`
to interact with a specific one.

#### `var stream = s3.list([options])`

Create a list stream. Each data emitted looks like this

``` js
{
  key: 'value/key', // the value key
  size: 24, // how many bytes
  modified: Date() // when was it modified last?
}
```

Options include:

``` js
{
  prefix: 'foo', // only list keys under foo
  marker: 'foo/bar/baz', // only list keys after foo/bar/baz
  limit: 14 // only return this many
}
```

## Acknowledgements

This project was kindly sponsored by nearForm.

## License

MIT
