import { Client as Minio } from 'minio';

const config = {
  endPoint: process.env.MINIO_END_POINT || 'minio-storage',
  port: parseInt(process.env.MINIO_PORT, 10) || 9000,
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'admin',
  secretKey: process.env.MINIO_SECRET_KEY || 'password',
};

export const minioClient = new Minio(config);