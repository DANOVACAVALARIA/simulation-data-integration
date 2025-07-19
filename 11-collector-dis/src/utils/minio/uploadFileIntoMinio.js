

export const uploadFileIntoMinio = async (minioClient, bucketName, objectName, filePath) => {
  try {
    await minioClient.fPutObject(bucketName, objectName, filePath)
    console.log(`COLLECTOR-DIS (minIO) => file ${objectName} uploaded to bucket ${bucketName}.`)
    return true
  } catch (err) {
    console.error('COLLECTOR-DIS (minIO) => error uploading file:', err)
    return false
  }
}