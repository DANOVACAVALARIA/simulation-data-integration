import { 
  listObjects, 
  removeObject, 
} from './minio/minioService.js';
import { 
  getStagingManifest,
  updateStagingManifest 
} from './services/manifestService.js';
import { minioConfig } from './config/minioConfig.js';


export async function listSimulationFolders(req, res) {
  try {
    const manifest = await getStagingManifest();
    res.status(200).json(manifest);
  } catch (error) {
    res.status(500).json({
      error: 'Error listing simulation folders',
      details: error.message
    });
  }
}

export async function deleteSimulationFolder(req, res) {
  try {
    const { id } = req.params;

    // List and remove all objects in the staging folder
    const objects = await listObjects(minioConfig.bucketName, `${minioConfig.stagingPartition}/${id}`);
    for (const object of objects) {
      await removeObject(minioConfig.bucketName, object);
    }

    // Update staging manifest
    const manifest = await getStagingManifest();
    const filteredManifest = manifest.filter(item => item.id !== id);
    await updateStagingManifest(filteredManifest);

    res.status(200).json({ message: 'Simulation folder deleted successfully' });
  } catch (error) {
    res.status(500).json({
      error: 'Error deleting simulation folder',
      details: error.message
    });
  }
}