
export const ensureBucketExists = async (minioClient, bucketName) => {
  try {
    const exists = await minioClient.bucketExists(bucketName)
    if (!exists) {
      await minioClient.makeBucket(bucketName, 'us-east-1')
      console.log(`COLLECTOR-DIS (minIO) => bucket ${bucketName} created.`)
    } else {
      console.log(`COLLECTOR-DIS (minIO) => bucket ${bucketName} already exists.`)
    }
  } catch (err) {
    console.error('COLLECTOR-DIS (minIO) => error checking/creting bucket:', err)
  }
} 