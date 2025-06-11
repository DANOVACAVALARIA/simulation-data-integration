import { minioConfig } from './minioConfig.js'
import { writeJsonToMinio } from './writeJsonToMinio.js'

const config = await minioConfig()

export const createSimulationMetadata = async(minioClient, metadata) => {
  await writeJsonToMinio(
    minioClient,
    config.bucketName,
    `${config.partitionName}/manifest.json`,
    metadata
  )
}