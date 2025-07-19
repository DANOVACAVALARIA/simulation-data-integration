
export const ensureBucketExists = async (minioClient, bucketName) => {
  try {
    const exists = await minioClient.bucketExists(bucketName)
    if (!exists) {
      await minioClient.makeBucket(bucketName, 'us-east-1')
      console.log(`COLLECTOR-PROTO (minIO) => bucket ${bucketName} created.`)
    } else {
      console.log(`COLLECTOR-PROTO (minIO) => bucket ${bucketName} already exists.`)
    }
  } catch (err) {
    console.error('COLLECTOR-PROTO (minIO) => error checking/creting bucket:', err)
  }
} 