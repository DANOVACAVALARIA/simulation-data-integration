export const minioConfig = {
  bucketName: process.env.MINIO_BUCKET || 'bronze',
  stagingPartition: process.env.MINIO_STAGING_PARTITION || 'staging',
  exercisesPartition: process.env.MINIO_EXERCISES_PARTITION || 'exercises',
  metadataFileName: process.env.MINIO_METADATA_FILENAME || 'manifest.json',
};