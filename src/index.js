import Storage from '@google-cloud/storage';
import once from 'once';
import http from 'http';

export default class BlobbyGCPStorage {
  constructor(opts) {
    this.options = opts || {};

    if (!this.options.project) throw new Error('options.project is required');
    if (!this.options.bucket) throw new Error('options.bucket is required');

    this.storage = Storage({ projectId: this.options.project });
    this.bucket = this.storage.bucket(this.options.bucket);
  }

  /*
    fileKey: unique id for storage
    opts: future
   */
  fetchInfo(fileKey, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    cb = once(cb);

    if (opts.acl === 'public') {
      return void this.httpRequest('HEAD', fileKey, (err, res, data) => {
        if (err) {
          return void cb(err);
        }

        cb(null, getInfoFromHeaders(res.headers));
      });
    }
    // else assume private

    const file = this.bucket.file(fileKey);
    file.getMetadata(function (err, metadata) {
      if (err) {
        return void cb(err);
      }

      cb(null, getInfoFromMeta(metadata));
    });
  }

  /*
    fileKey: unique id for storage
    opts: future
   */
  fetch(fileKey, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    cb = once(cb);

    if (opts.acl === 'public') {
      return void this.httpRequest('GET', fileKey, (err, res, data) => {
        if (err) {
          return void cb(err);
        }

        cb(null, getInfoFromHeaders(res.headers), data);
      });
    }
    // else assume private
    
    cb = once(cb);
    const file = this.bucket.file(fileKey);
    const bufs = [];
    const { bucket } = this.options;
    let res;
    file.createReadStream()
      .on('error', err => cb(err))
      .on('response', r => res = r)
      .on('data', chunk => bufs.push(chunk))
      .on('end', () => {
        if (res.statusCode !== 200) {
          return void cb(new Error('gcp.storage.fetch.error: '
            + ' for ' + (bucket + '/' + fileKey))
          );
        }

        cb(null, getInfoFromHeaders(res.headers), Buffer.concat(bufs));
      })
    ;
  }

  /*
   fileKey: unique id for storage
   file: file object
   file.buffer: Buffer containing file data
   file.headers: Any HTTP headers to supply to object
   opts: future
   */
  store(fileKey, fileInfo, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};
    const file = this.bucket.file(fileKey);

    const { bucket } = this.options;
    cb = once(cb);
    const bufs = [];
    const options = getOptionsFromInfo(fileInfo.headers);
    file.createWriteStream(options)
      .on('error', err => cb(err))
      .on('response', res => {
        if (res.statusCode !== 200) {
          return void cb(new Error('gcp.storage.store.error: '
            + ' for ' + (bucket + '/' + fileKey))
          );
        }
        
        res
          .on('error', err => cb(err))
          .on('data', chunk => bufs.push(chunk))
          .on('end', () => {
            const respData = Buffer.concat(bufs);
            const json = respData.toString('utf8');
            const metadata = JSON.parse(json);

            cb(null, getInfoFromMeta(metadata));
          })
        ;
      })
      .end(fileInfo.buffer)
    ;
  }

  setACL(fileKey, acl, cb) {
    const file = this.bucket.filename(fileKey);

    // very basic (read: limited) acl support
    if (/^public/.test(info.AccessControl)) file.makePublic(cb);
    else file.makePrivate(cb);
  }

  /*
   fileKey: unique id for storage
   */
  remove(fileKey, cb) {
    const file = this.bucket.file(fileKey);

    file.delete(cb);
  }

  /*
   dir: unique id for storage
   */
  removeDirectory(dir, cb) {
    const options = {
      prefix: dir + ((dir.length === 0 || dir[dir.length - 1] === '/') ? '' : '/') // prefix must always end with `/` if not root
    };

    this.bucket.deleteFiles(options, cb);
  }

  /* supported options:
   dir: Directory (prefix) to query
   opts: Options object
   opts.lastKey: if requesting beyond maxKeys (paging)
   opts.maxKeys: the max keys to return in one request
   opts.delimiter: can be used to control delimiter of query independent of deepQuery
   opts.deepQuery: true if you wish to query the world across buckets, not just the current directory
   cb(err, files, dirs, lastKey) - Callback fn
   cb.err: Error if any
   cb.files: An array of files: { Key, LastModified, ETag, Size, ... }
   cb.dirs: An array of dirs: [ 'a', 'b', 'c' ]
   cb.lastKey: An identifier to permit retrieval of next page of results, ala: 'abc'
  */
  list(dir, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts;
      opts = null;
    }
    opts = opts || {};

    const options = {
      autoPaginate: false, // we'll handle paging
      prefix: dir + ((dir.length === 0 || dir[dir.length - 1] === '/') ? '' : '/'), // prefix must always end with `/` if not root
      delimiter: typeof opts.delimiter === 'string' ? opts.delimiter : opts.deepQuery ? '' : '/',
      pageToken: opts.lastKey,
      maxResults: opts.maxKeys
    };

    this.bucket.getFiles(options, (err, files, nextQuery, apiRes) => {
      if (err) return void cb(err);

      // map from GCP to S3 spec
      files = files.map(f => getInfoFromMeta(f.metadata));

      cb(null, files, [], nextQuery && nextQuery.pageToken);
    });
  }

  httpRequest(method, fileKey, cb) {
    const opts = {
      protocol: 'http:',
      host: 'storage.googleapis.com',
      method,
      path: `/${this.options.bucket}/${fileKey}`
    };
    var bufs = [];
    http.request(opts, res => {
      if (res.statusCode !== 200) {
        return void cb(new Error('http.request.error: '
          + res.statusCode + ' for ' + opts.path)
        );
      }

      res.on('data', chunk => bufs.push(chunk));

      res.on('end', () => cb(null, res, Buffer.concat(bufs)));
    }).on('error', err => {
      cb(err);
    }).end();
  }
}

const gValidHeaders = {
  'cache-control': 'CacheControl',
  'content-encoding': 'ContentEncoding',
  'content-language': 'ContentLanguage',
  'content-type': 'ContentType',
  'last-modified': 'LastModified',
  'content-length': 'Size',
  'etag': 'ETag',
};
/*
metadata: {
  kind: 'storage#object',
  id: 'blobby-cache-us/test.txt/12345',
  selfLink: 'https://www.googleapis.com/storage/v1/b/blobby-cache-us/o/test.txt',
  name: 'test.txt',
  bucket: 'blobby-cache-us',
  generation: '12345',
  metageneration: '4',
  contentType: 'text/plain',
  timeCreated: '2017-07-24T23:14:54.880Z',
  updated: '2017-07-24T23:21:22.360Z',
  storageClass: 'MULTI_REGIONAL',
  timeStorageClassUpdated: '2017-07-24T23:14:54.880Z',
  size: '13',
  md5Hash: 'oPKjwdzVscrHG/DAPy/xvQ==',
  mediaLink: 'https://www.googleapis.com/download/storage/v1/b/blobby-cache-us/o/test.txt?generation=12345&alt=media',
  cacheControl: 'public, max-age=36000',
  metadata: { 'custom-key': 'custom-value' },
  crc32c: 'MJHBvg==',
  etag: 'CP+J6I+Go9UCEAQ='
}
*/
const gInfoFromMeta = {
  cacheControl: 'CacheControl',
  etag: 'ETag',
  name: 'Key',
  size: 'Size',
  timeCreated: 'LastModified'
};
const gOptionsFromInfo = {
  CacheControl: 'cacheControl',
  ContentType: 'contentType'
};

function getInfoFromHeaders(reqHeaders) {
  const info = { CustomHeaders: {} };
  Object.keys(reqHeaders).forEach(k => {
    const kLower = k.toLowerCase();
    const validHeader = gValidHeaders[kLower];
    if (!validHeader) { // if not a forward header, lets also check for dynamic headers
      const customHeader = /^x\-goog\-meta\-(.*)$/.exec(k);
      if (customHeader) { // if custom metadata avail, lets pass that along too
        info.CustomHeaders[customHeader[1]] = reqHeaders[k];
      }
      return;
    }
    const val = reqHeaders[k];
    if (!val) return;
    info[validHeader] = val; // map the values
  });
  
  if (typeof info.LastModified === 'string') info.LastModified = new Date(info.LastModified);
  if (typeof info.Size === 'string') info.Size = parseInt(info.Size);

  return info;
}

function getInfoFromMeta(meta) {
  const info = {};
  Object.keys(meta).forEach(k => {
    const validHeader = gInfoFromMeta[k];
    if (!validHeader) return;
    const val = meta[k];
    if (!val) return;
    info[validHeader] = val; // map the values
  });

  if (meta.metadata) {
    // if metadata (custom headers) avail, lets pass that along too
    info.CustomHeaders = meta.metadata;
  }

  if (typeof info.LastModified === 'string') info.LastModified = new Date(info.LastModified);
  if (typeof info.Size === 'string') info.Size = parseInt(info.Size);

  return info;
}

function getOptionsFromInfo(info) {
  const options = { metadata: {} };
  Object.keys(info).forEach(k => {
    const validHeader = gOptionsFromInfo[k];
    if (!validHeader) return;
    const val = info[k];
    if (!val) return;
    options.metadata[validHeader] = val; // map the values
  });

  if (/^public/.test(info.AccessControl)) options.public = true;
  else if (/^private/.test(info.AccessControl)) options.private = true;

  if (info.CustomHeaders) {
    // forward custom headers
    const customHeaders = options.metadata.metadata = {};
    Object.keys(info.CustomHeaders).forEach(k => {
      customHeaders[k] = info.CustomHeaders[k];
    });
  }
  
  return options;
}
