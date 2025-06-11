import { readJsonFromMinio, writeJsonToMinio } from '../minio/minioService.js';
import { minioConfig } from '../config/minioConfig.js';

export async function getExercisesManifest() {
  const manifest = await readJsonFromMinio(
    minioConfig.bucketName,
    `${minioConfig.exercisesPartition}/${minioConfig.metadataFileName}`
  );
  return manifest || { created: [], processed: [] };
}

export async function updateExercisesManifest(exerciseData) {
  const manifest = await getExercisesManifest();
  
  // Remove existing entry if exists
  manifest.created = manifest.created.filter(item => item.id !== exerciseData.id);
  
  // Add new entry
  manifest.created.push(exerciseData);
  
  await writeJsonToMinio(
    minioConfig.bucketName,
    `${minioConfig.exercisesPartition}/${minioConfig.metadataFileName}`,
    manifest
  );
}

export async function removeFromExercisesManifest(exerciseId) {
  const manifest = await getExercisesManifest();
  manifest.created = manifest.created.filter(item => item.id !== exerciseId);
  
  await writeJsonToMinio(
    minioConfig.bucketName,
    `${minioConfig.exercisesPartition}/${minioConfig.metadataFileName}`,
    manifest
  );
}

export async function getStagingManifest() {
  const manifest = await readJsonFromMinio(
    minioConfig.bucketName,
    `${minioConfig.stagingPartition}/${minioConfig.metadataFileName}`
  );
  return manifest || [];
}

export async function updateStagingManifest(filteredData) {
  await writeJsonToMinio(
    minioConfig.bucketName,
    `${minioConfig.stagingPartition}/${minioConfig.metadataFileName}`,
    filteredData
  );
}