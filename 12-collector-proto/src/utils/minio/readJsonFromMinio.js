// read JSON from MinIO
export async function readJsonFromMinio(minioClient, bucketName, objectPath, fileName) {
  try {
    const data = await minioClient.getObject(bucketName, objectPath)
    let content = ''
    for await (const chunk of data) {
      content += chunk.toString()
    }
    // ensure content is valid JSON
    if (!content) {
      return objectPath.endsWith(fileName) ? [] : {}
    }
    const parsed = JSON.parse(content)
    if (objectPath.endsWith(fileName)) {
      return Array.isArray(parsed) ? parsed : []
    }
    return typeof parsed === 'object' && parsed !== null ? parsed : {}
    
  } catch (error) {
    if (error.code === 'NoSuchKey') {
      return objectPath.endsWith(fileName) ? [] : {}
    }
    console.error(`COLLECTOR-PROTO (minio) => error reading JSON from ${bucketName}/${objectPath}:`, error.message)
    return objectPath.endsWith(fileName) ? [] : {}
  }
}