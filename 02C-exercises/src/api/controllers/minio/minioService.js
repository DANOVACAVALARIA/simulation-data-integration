import { minioClient } from './minioClient.js';
import { minioConfig } from '../config/minioConfig.js';

export async function readJsonFromMinio(bucketName, objectName) {
  try {
    const chunks = [];
    const stream = await minioClient.getObject(bucketName, objectName);
    
    return new Promise((resolve, reject) => {
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('end', () => {
        try {
          const data = Buffer.concat(chunks).toString();
          resolve(JSON.parse(data));
        } catch (error) {
          reject(error);
        }
      });
      stream.on('error', reject);
    });
  } catch (error) {
    if (error.code === 'NoSuchKey') {
      return null;
    }
    throw error;
  }
}

export async function writeJsonToMinio(bucketName, objectName, data) {
  const jsonString = JSON.stringify(data, null, 2);
  const buffer = Buffer.from(jsonString);
  await minioClient.putObject(bucketName, objectName, buffer);
}

export async function listObjects(bucketName, prefix) {
  const objects = [];
  const stream = minioClient.listObjects(bucketName, prefix, true);
  
  return new Promise((resolve, reject) => {
    stream.on('data', obj => objects.push(obj.name));
    stream.on('end', () => resolve(objects));
    stream.on('error', reject);
  });
}

export async function copyObject(bucketName, destObject, sourceObject) {
  await minioClient.copyObject(bucketName, destObject, sourceObject);
}

export async function removeObject(bucketName, objectName) {
  await minioClient.removeObject(bucketName, objectName);
}

export async function uploadFile(bucketName, objectName, buffer, contentType) {
  await minioClient.putObject(bucketName, objectName, buffer, buffer.length, {
    'Content-Type': contentType
  });
}