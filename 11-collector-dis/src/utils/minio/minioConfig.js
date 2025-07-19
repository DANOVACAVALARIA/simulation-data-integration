
export const minioConfig = async () => {

  const config = {
    endPoint: 'minio-storage',
    port: 9000,
    useSSL: false,
    accessKey: 'admin',
    secretKey: 'password',
    bucketName: 'bronze',
    partitionName: 'staging'
  }

  // environment variables
  if (process.env.MINIO_END_POINT) config.endPoint = process.env.MINIO_END_POINT
  if (process.env.MINIO_PORT) config.port = parseInt(process.env.MINIO_PORT, 10)
  if (process.env.MINIO_USE_SSL) config.useSSL = process.env.MINIO_USE_SSL === true
  if (process.env.MINIO_ACCESS_KEY) config.accessKey = process.env.MINIO_ACCESS_KEY
  if (process.env.MINIO_SECRET_KEY) config.secretKey = process.env.MINIO_SECRET_KEY 
  if (process.env.MINIO_BUCKET) config.bucketName = process.env.MINIO_BUCKET
  if (process.env.MINIO_PARTITION) config.partitionName = process.env.MINIO_PARTITION

  console.log('COLLECTOR-DIS (minIO Client) => minIO configuration loaded.')

  return config
}