import { Router } from 'express';
import { listSimulationFolders, deleteSimulationFolder } from './controllers/simulationController.js';

export const simulationRoutes = () => {
  const router = Router();

  router.get('/', listSimulationFolders);
  router.delete('/:id', deleteSimulationFolder);

  return router;
};