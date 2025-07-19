
export async function writeJsonToMinio(minioClient, bucketName, objectPath, data) {
  const content = JSON.stringify(data, null, 2)
  await minioClient.putObject(bucketName, objectPath, Buffer.from(content), {
    'Content-Type': 'application/json'
  })
}