import { Router } from 'express';
import {
  createExercise,
  listExercises,
  getExercise,
  updateExercise,
  deleteExercise,
  uploadDocument,
  moveSimulationsData
} from './controllers/exerciseController.js';

export const exerciseRoutes = (upload) => {
  const router = Router();

  router.post('/', createExercise);
  router.get('/', listExercises);
  router.get('/:id', getExercise);
  router.put('/:id', updateExercise);
  router.delete('/:id', deleteExercise);
  router.post('/:id/documents', upload.single('file'), uploadDocument);
  router.post('/:id/move-input', moveSimulationsData);

  return router;
};




















// import { Router } from 'express'

// import { createExercise } from './controllers/createExercise.js'
// import { updateExercise } from './controllers/updateExercise.js'
// import { uploadDocument } from './controllers/uploadDocument.js'
// import { deleteExercise } from './controllers/deleteExercise.js'
// import { moveSimulationsData } from './controllers/moveSimulationsData.js'
// import { listExercises } from './controllers/listExercises.js'
// import { getExercise } from './controllers/getExercise.js'

// export const exerciseRoutes = (upload) => {
//   const router = Router()

//   router.post('/', createExercise)
//   router.post('/:id/documents', upload.single('file'), uploadDocument)
//   router.post('/:id/move-input', moveSimulationsData)
//   router.get('/', listExercises)
//   router.get('/:id', getExercise)
//   router.put('/:id', updateExercise)
//   router.delete('/:id', deleteExercise)

//   return router
// }