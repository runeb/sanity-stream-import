const fs = require('fs')
const path = require('path')
const sanityClient = require('@sanity/client')
const importer = require('@sanity/import')
const { default: PQueue } = require('p-queue')
const tar = require('tar-stream')
var miss = require('mississippi')
const { createGunzip } = require('gunzip-stream')
const { exit } = require('process')

const exportArchive = process.argv[2]
if (!exportArchive) {
  console.error('specify path to tar.gz as first argument')
  exit(1)
}

const client = sanityClient({
  projectId: process.env.PROJECT,
  dataset: process.env.DATASET,
  token: process.env.TOKEN,
  useCdn: false
})

var tarFileStream = fs.createReadStream(exportArchive)
var extract = tar.extract()

let assetIndex = {}
let assetKeys = []
const assetMap = {}
let rawNdjson = ''

const assetMappingFromFileinfo = fileinfo => {
  let type
  const key = assetKeys.find((key) => {
    const parts = key.split('-')
    type = parts[0]
    let hash = parts[1]
    return (fileinfo.name.lastIndexOf(hash, 0) === 0)
  })

  if (!key) return

  return {
    type,
    key,
    fileURI: `${type}@file://./${type}s/${fileinfo.base}`
  }
}

const queue = new PQueue({
  concurrency: 10,
  interval: 1000 / 25
})

const uploadAsset = async (type, filename, doc, stream) => {
  console.debug('Uploading', filename)
  return client.assets.upload(type, stream, doc)
}

extract.on('entry', async function (header, stream, next) {
  const fileinfo = path.parse(header.name)

  if (fileinfo.base === 'assets.json') {
    // Asset documents json
    console.debug('reading assets')
    miss.pipe(stream, miss.concat((buf => {
      assetIndex = JSON.parse(buf.toString())
      assetKeys = Object.keys(assetIndex)
      console.debug('Found', assetKeys.length, 'assets')
    })))
  } else if (fileinfo.ext === '.ndjson') {
    // documents ndjson
    console.debug('reading documents')
    // We keep this as a string so we more easily can replace all _sanityAsset
    // directives with actual asset references after asset upload
    miss.pipe(stream, miss.concat((buf => rawNdjson = buf.toString())))
  } else {
    // Asset
    const asset = assetMappingFromFileinfo(fileinfo)
    if (!asset) {
      console.error('uh oh, file not found in asset manifest', header.name)
      // fast forward
      stream.resume()
    } else {
      const doc = assetIndex[asset.key];
      (async () => {
        await queue.add(() => uploadAsset(asset.type, fileinfo.name, doc, stream))
          .then(result => {
            // For replacing the _sanityAsset directive with asset ref later
            assetMap[asset.fileURI] = result._id
          })
      })()
    }
  }

  stream.on('end', next)
})

extract.on('finish', async function () {
  // all entries read
  console.log('tar-stream finished')
  await queue.onIdle()
  console.log('upload finish')

  // Replace any _sanityAsset directives with asset references from the uploaded
  // assets by string replace
  const objects = []
  for (const line of rawNdjson.split("\n")) {
    if (line.length === 0) continue
    let newjson = line
    for (const key in assetMap) {
      const ref = { _type: 'reference', _ref: assetMap[key] }
      newjson = newjson.replace(
        `_sanityAsset":"${key}"`,
        `asset":${JSON.stringify(ref)}`
      )
    }
    objects.push(JSON.parse(newjson))
  }

  console.log('Document count', objects.length)
  await importer(objects, { client })
  console.log('done')
})

tarFileStream.pipe(createGunzip()).pipe(extract);
