import fs from 'fs'
import { Client } from 'minio'
import { promisify } from 'util'

import { minioConfig } from './minio/minioConfig.js'
import { ensureBucketExists } from './minio/ensureBucketExists.js'
import { getFilesInDirectory } from './minio/getFilesInDir.js'
import { uploadFileIntoMinio } from './minio/uploadFileIntoMinio.js'
import { deleteLocalFile } from './minio/deleteLocalFile.js'
import { deleteLocalDir } from './minio/deleteLocalDir.js'
import { createSimulationMetadata } from './minio/createSimulationMetadata.js'
import { updateSimulationsManifest } from './minio/updateSimulationsManifest.js'

const readFile = promisify(fs.readFile)


// minio configs
const config = await minioConfig()
console.log(`COLLECTOR-PROTO => connected to minIO: ${config.endPoint}:${config.port} (SSL: ${config.useSSL})`)

// minio client instance
const minioClient = new Client({
  endPoint: config.endPoint,
  port: config.port, 
  useSSL: config.useSSL,
  accessKey: config.accessKey,
  secretKey: config.secretKey,
})


//--- upload files and terminate session
export const terminateSession = async (sessionJsonPath) => {
  try {
    // get files in directory
    const sessionRawData = await readFile(sessionJsonPath, 'utf8')
    const sessionData = JSON.parse(sessionRawData)
    console.log('COLLECTOR-PROTO => session data retrieved from ', sessionData.dir + "/" + sessionData.metadataFile) 
    
    // check bucket existance
    await ensureBucketExists(minioClient, config.bucketName)

    // assigning partitions --> insert partitionName (staging) within bucket reference
    const objectPrefix = `${config.partitionName}/${sessionData.parentFolder}/${sessionData.childFolder}`
    console.log(`COLLECTOR-PROTO => using object prefix ${objectPrefix}`)

    // retrieve files in session folder
    const files = await getFilesInDirectory(sessionData.dir)
    console.log(`COLLECTOR-PROTO => ${files.length} files found in session folder ${sessionData.dir}`)

    // upload files to minio
    console.log('COLLECTOR-PROTO => uploading PROTO files to minIO...')
    let successCount = 0
    const protoFiles = files.filter(file => file.fileName !== sessionData.metadataFile)
    for (const protoFile of protoFiles) {
      const objectName = `${objectPrefix}/${protoFile.fileName}`
      const isUploaded = await uploadFileIntoMinio(minioClient, config.bucketName, objectName, protoFile.fullPath)
      const isDeleted = await deleteLocalFile(protoFile.fullPath)
      if (isUploaded && isDeleted) successCount++
    }
    console.log('COLLECTOR-PROTO => uploading JSON metadata file to minIO...')
    const metadataFile = files.filter(file => file.fileName === sessionData.metadataFile)[0]
    if (metadataFile) {
      const objectName = `${objectPrefix}/${metadataFile.fileName}`
      const isUploaded = await uploadFileIntoMinio(minioClient, config.bucketName, objectName, metadataFile.fullPath)
      const isDeleted = await deleteLocalFile(metadataFile.fullPath)    // delete local file after upload
      if (isUploaded && isDeleted) successCount++
    }
    console.log(`COLLECTOR-PROTO => ${successCount} of ${files.length} files transferred to minIO bucket ${config.bucketName}, at partition ${objectPrefix}.`)

    // delete session folder
    const isDeleted = await deleteLocalDir(sessionData)
    if (isDeleted) console.log(`COLLECTOR-PROTO => session folder ${sessionData.dir} deleted.`)
    
    // register on manifest.json
    const manifestData = {
      id: `${sessionData.parentFolder}`,
      data: {
        origin: `${sessionData.sources[0]}`,
        path: `${config.partitionName}/${sessionData.parentFolder}`,
        createdAt: new Date().toISOString()
      }
    }

    await createSimulationMetadata(minioClient, manifestData)
    await updateSimulationsManifest(minioClient, manifestData)
    

    return true
  } catch (err) {
    console.error('COLLECTOR-PROTO => error reading session data:', err)
    return false
  }
}