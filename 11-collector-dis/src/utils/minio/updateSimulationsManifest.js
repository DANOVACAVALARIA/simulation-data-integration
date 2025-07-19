import { minioConfig } from './minioConfig.js'
import { readJsonFromMinio } from './readJsonFromMinio.js'
import { writeJsonToMinio } from './writeJsonToMinio.js'

const config = await minioConfig()

export const updateSimulationsManifest = async(minioClient, newEntry) => {
  const manifest = await readJsonFromMinio(minioClient,
    config.bucketName,
    `${config.partitionName}/manifest.json`,
    'manifest.json'
  )

  manifest.push(newEntry)
  await writeJsonToMinio(
    minioClient,
    config.bucketName,
    `${config.partitionName}/manifest.json`,
    manifest
  )
}