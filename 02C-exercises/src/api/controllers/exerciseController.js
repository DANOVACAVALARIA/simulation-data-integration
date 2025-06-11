import { 
  readJsonFromMinio, 
  writeJsonToMinio, 
  listObjects, 
  removeObject, 
  copyObject,
  uploadFile 
} from './minio/minioService.js';
import { 
  getExercisesManifest, 
  updateExercisesManifest, 
  removeFromExercisesManifest,
  getStagingManifest,
  updateStagingManifest 
} from './services/manifestService.js';
import { validateExerciseData, generateTimestamp } from './utils/validators.js';
import { minioConfig } from './config/minioConfig.js';

export async function createExercise(req, res) {
  try {
    const { name, description, simulationType, origin, startDate, endDate, trainedForce, wikiUrl } = req.body;
    const shortDescription = description?.substring(0, 6) || '';
    
    const validationError = validateExerciseData({ origin, simulationType, shortDescription });
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    const exerciseId = `${origin}-${simulationType}-${shortDescription}-${generateTimestamp()}`;
    
    const exerciseMetadata = {
      id: exerciseId,
      name,
      description,
      simulationType,
      origin,
      startDate,
      endDate,
      trainedForce,
      wikiUrl,
      documents: [],
      simulations: [],
      createdAt: new Date().toISOString()
    };

    // Save exercise metadata
    await writeJsonToMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${exerciseId}/exercise-metadata.json`,
      exerciseMetadata
    );

    // Update exercises manifest
    const exerciseManifest = {
      id: exerciseId,
      data: {
        origin,
        path: `${minioConfig.exercisesPartition}/${exerciseId}`,
        createdAt: exerciseMetadata.createdAt
      }
    };

    await updateExercisesManifest(exerciseManifest);

    res.status(201).json({
      id: exerciseId,
      message: 'Exercise created successfully'
    });
  } catch (error) {
    console.error('Erro ao criar exercício:', error);
    res.status(500).json({
      error: 'Error creating exercise',
      details: error.message
    });
  }
}

export async function listExercises(req, res) {
  try {
    console.log('🔍 Iniciando listagem de exercícios...');
    
    // Get exercises manifest first
    const manifest = await getExercisesManifest();
    console.log('📋 Manifest obtido:', manifest);

    // Extract exercises from manifest structure
    let createdExercises = [];
    let processedExercises = [];
    
    if (manifest && typeof manifest === 'object') {
      // Extract created exercises
      if (Array.isArray(manifest.created)) {
        createdExercises = manifest.created;
      }
      // Extract processed exercises
      if (Array.isArray(manifest.processed)) {
        processedExercises = manifest.processed;
      }
      // If manifest is directly an array (fallback)
      if (Array.isArray(manifest)) {
        createdExercises = manifest;
      }
    }

    // Create a Set of processed exercise IDs for quick lookup
    const processedIds = new Set(processedExercises.map(ex => ex.id));
    
    console.log('📋 Exercícios criados:', createdExercises.map(ex => ex.id));
    console.log('📋 Exercícios processados:', processedExercises.map(ex => ex.id));

    // Combine all exercises
    const allExercises = [...createdExercises, ...processedExercises];

    if (allExercises.length === 0) {
      console.log('⚠️ Nenhum exercício encontrado');
      return res.json({ created: [], total: 0 });
    }

    const exercises = [];

    // Process each exercise from manifest
    for (const exercise of allExercises) {
      try {
        const exerciseId = exercise.id;
        console.log(`\n🔍 Processando exercício: ${exerciseId}`);

        // Determine status based on manifest categorization
        const isProcessed = processedIds.has(exerciseId);
        const status = isProcessed ? 'processado' : 'criado';
        
        console.log(`📊 Status do exercício ${exerciseId}: ${status} (baseado no manifest)`);

        // Get exercise metadata
        const metadataPath = `${minioConfig.exercisesPartition}/${exerciseId}/exercise-metadata.json`;
        console.log(`📄 Carregando metadados de: ${metadataPath}`);
        
        const metadata = await readJsonFromMinio(minioConfig.bucketName, metadataPath);
        
        if (!metadata) {
          console.log(`⚠️ Metadados não encontrados para ${exerciseId}`);
          // Add basic exercise even without metadata
          exercises.push({
            id: exerciseId,
            name: `Exercício ${exerciseId}`,
            description: 'Metadados não disponíveis',
            status: status, // Use status from manifest
            documentCount: 0,
            simulationCount: 0,
            createdAt: exercise.data?.createdAt || new Date().toISOString(),
            origin: exercise.data?.origin || 'Desconhecida'
          });
          continue;
        }

        console.log(`✅ Metadados carregados para ${exerciseId}:`, {
          name: metadata.name,
          origin: metadata.origin,
          documentsCount: metadata.documents?.length || 0,
          simulationsCount: metadata.simulations?.length || 0
        });

        // Count documents
        let documentCount = 0;
        try {
          const documentsPrefix = `${minioConfig.exercisesPartition}/${exerciseId}/documents/`;
          const documentObjects = await listObjects(minioConfig.bucketName, documentsPrefix);
          documentCount = documentObjects.length;
          console.log(`📁 Documentos encontrados: ${documentCount}`);
        } catch (error) {
          console.log(`⚠️ Erro ao contar documentos: ${error.message}`);
          // Use metadata count as fallback
          documentCount = metadata.documents?.length || 0;
        }

        // Count simulations
        let simulationCount = 0;
        try {
          const simulationsPrefix = `${minioConfig.exercisesPartition}/${exerciseId}/simulations/`;
          const simulationObjects = await listObjects(minioConfig.bucketName, simulationsPrefix);
          // Count unique simulation folders
          const simulationFolders = new Set();
          simulationObjects.forEach(obj => {
            const pathParts = obj.replace(simulationsPrefix, '').split('/');
            if (pathParts.length > 0 && pathParts[0]) {
              simulationFolders.add(pathParts[0]);
            }
          });
          simulationCount = simulationFolders.size;
          console.log(`🔗 Simulações encontradas: ${simulationCount}`);
        } catch (error) {
          console.log(`⚠️ Erro ao contar simulações: ${error.message}`);
          // Use metadata count as fallback
          simulationCount = metadata.simulations?.length || 0;
        }

        // Build exercise object with status from manifest
        const exerciseData = {
          id: exerciseId,
          name: metadata.name || 'Sem nome',
          description: metadata.description || 'Sem descrição',
          simulationType: metadata.simulationType,
          origin: metadata.origin,
          startDate: metadata.startDate,
          endDate: metadata.endDate,
          trainedForce: metadata.trainedForce,
          wikiUrl: metadata.wikiUrl,
          documents: metadata.documents || [],
          simulations: metadata.simulations || [],
          documentCount,
          simulationCount,
          status: status, // Use status determined from manifest
          createdAt: metadata.createdAt,
          updatedAt: metadata.updatedAt
        };

        exercises.push(exerciseData);
        console.log(`✅ Exercício ${exerciseId} processado com sucesso - Status: ${status}`);

      } catch (error) {
        console.error(`❌ Erro ao processar exercício ${exercise.id}:`, error);
        // Add basic exercise data even with errors
        const isProcessed = processedIds.has(exercise.id);
        exercises.push({
          id: exercise.id,
          name: `Exercício ${exercise.id}`,
          description: 'Erro ao carregar metadados',
          status: isProcessed ? 'processado' : 'erro',
          documentCount: 0,
          simulationCount: 0,
          createdAt: exercise.data?.createdAt || new Date().toISOString(),
          origin: exercise.data?.origin || 'Desconhecida'
        });
      }
    }

    // Sort by creation date (newest first)
    exercises.sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0));

    console.log(`🎉 Listagem concluída. Total de exercícios: ${exercises.length}`);
    console.log('📋 Exercícios processados:', exercises.map(ex => ({ 
      id: ex.id, 
      name: ex.name, 
      status: ex.status,
      simulations: ex.simulationCount 
    })));

    res.json({ 
      created: exercises,
      total: exercises.length 
    });

  } catch (error) {
    console.error('❌ Erro fatal ao listar exercícios:', error);
    res.status(500).json({ 
      error: 'Erro interno do servidor ao listar exercícios',
      details: error.message 
    });
  }
}

export async function getExercise(req, res) {
  try {
    const { id } = req.params;
    console.log(`🔍 Buscando exercício: ${id}`);
    
    const metadata = await readJsonFromMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`
    );

    if (!metadata) {
      console.log(`❌ Exercício não encontrado: ${id}`);
      return res.status(404).json({ error: 'Exercise not found' });
    }

    console.log(`✅ Exercício encontrado: ${id}`);
    res.status(200).json(metadata);
  } catch (error) {
    console.error(`❌ Erro ao buscar exercício ${req.params.id}:`, error);
    res.status(500).json({
      error: 'Error getting exercise',
      details: error.message
    });
  }
}

export async function updateExercise(req, res) {
  try {
    const { id } = req.params;
    const { name, description, simulationType, origin, startDate, endDate, trainedForce, wikiUrl } = req.body;

    console.log(`🔄 Atualizando exercício: ${id}`, req.body);

    const validationError = validateExerciseData({ origin, simulationType });
    if (validationError) {
      return res.status(400).json({ error: validationError });
    }

    const metadata = await readJsonFromMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`
    );

    if (!metadata) {
      return res.status(404).json({ error: 'Exercise not found' });
    }

    const updatedMetadata = {
      ...metadata,
      name: name || metadata.name,
      description: description || metadata.description,
      simulationType: simulationType || metadata.simulationType,
      origin: origin || metadata.origin,
      startDate: startDate || metadata.startDate,
      endDate: endDate || metadata.endDate,
      trainedForce: trainedForce || metadata.trainedForce,
      wikiUrl: wikiUrl || metadata.wikiUrl,
      updatedAt: new Date().toISOString()
    };

    await writeJsonToMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`,
      updatedMetadata
    );

    console.log(`✅ Exercício atualizado: ${id}`);
    res.status(200).json({ 
      message: 'Exercise updated successfully',
      id: id 
    });
  } catch (error) {
    console.error(`❌ Erro ao atualizar exercício ${req.params.id}:`, error);
    res.status(500).json({
      error: 'Error updating exercise',
      details: error.message
    });
  }
}

export async function deleteExercise(req, res) {
  try {
    const { id } = req.params;
    console.log(`🗑️ Excluindo exercício: ${id}`);

    // List and remove all objects in the exercise folder
    const objects = await listObjects(minioConfig.bucketName, `${minioConfig.exercisesPartition}/${id}`);
    console.log(`📁 Objetos a excluir: ${objects.length}`);
    
    for (const object of objects) {
      await removeObject(minioConfig.bucketName, object);
    }

    // Update manifest
    await removeFromExercisesManifest(id);

    console.log(`✅ Exercício excluído: ${id}`);
    res.status(200).json({ message: 'Exercise deleted successfully' });
  } catch (error) {
    console.error(`❌ Erro ao excluir exercício ${req.params.id}:`, error);
    res.status(500).json({
      error: 'Error deleting exercise',
      details: error.message
    });
  }
}

export async function uploadDocument(req, res) {
  try {
    const { id } = req.params;
    const file = req.file;
    const { description } = req.body;

    console.log(`📤 Upload de documento para exercício: ${id}`, {
      fileName: file?.originalname,
      fileSize: file?.size,
      description
    });

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    // Check if exercise exists
    const metadata = await readJsonFromMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`
    );

    if (!metadata) {
      return res.status(404).json({ error: 'Exercise not found' });
    }

    // Upload file to MinIO
    const filePath = `${minioConfig.exercisesPartition}/${id}/documents/${file.originalname}`;
    await uploadFile(minioConfig.bucketName, filePath, file.buffer, file.mimetype);

    // Create document object
    const document = {
      id: Date.now().toString(),
      name: file.originalname,
      description: description || '',
      path: filePath,
      size: file.size,
      mimetype: file.mimetype,
      uploadedAt: new Date().toISOString()
    };

    // Update exercise metadata
    metadata.documents.push(document);
    metadata.updatedAt = new Date().toISOString();

    await writeJsonToMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`,
      metadata
    );

    console.log(`✅ Documento enviado: ${file.originalname} para exercício ${id}`);
    res.status(200).json({
      message: 'Document uploaded successfully',
      document
    });
  } catch (error) {
    console.error(`❌ Erro ao fazer upload de documento:`, error);
    res.status(500).json({
      error: 'Error uploading document',
      details: error.message
    });
  }
}

export async function moveSimulationsData(req, res) {
  try {
    const { id } = req.params;
    const { inputPaths } = req.body;

    console.log(`🔄 Movendo simulações para exercício: ${id}`, { inputPaths });

    if (!id) {
      return res.status(400).json({ error: 'Exercise ID is required' });
    }

    if (!inputPaths || !Array.isArray(inputPaths) || inputPaths.length === 0) {
      return res.status(400).json({ error: 'inputPaths must be a non-empty array' });
    }

    // Check if exercise exists
    const metadata = await readJsonFromMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`
    );

    if (!metadata) {
      return res.status(404).json({ error: 'Exercise not found' });
    }

    const movedSimulations = [];
    const invalidPaths = [];

    for (const inputPath of inputPaths) {
      try {
        console.log(`🔍 Processando simulação: ${inputPath}`);

        // Check if path already exists in exercise
        const existingSimulation = metadata.simulations.find(s => 
          s.originalPath === inputPath || s.originalPath === `${minioConfig.stagingPartition}/${inputPath}`
        );

        if (existingSimulation) {
          invalidPaths.push({
            path: inputPath,
            error: 'Folder already linked to exercise'
          });
          continue;
        }

        const fullPath = inputPath.startsWith(minioConfig.stagingPartition) 
          ? inputPath 
          : `${minioConfig.stagingPartition}/${inputPath}`;

        // List objects in staging folder
        const objects = await listObjects(minioConfig.bucketName, fullPath);
        console.log(`📁 Objetos encontrados em ${fullPath}: ${objects.length}`);

        if (objects.length === 0) {
          invalidPaths.push({
            path: inputPath,
            error: 'Simulation data folder not found or empty'
          });
          continue;
        }

        const simulationId = inputPath.split('/').pop();
        const targetPath = `${minioConfig.exercisesPartition}/${id}/simulations/${simulationId}`;

        // Move objects
        for (const object of objects) {
          const relativePath = object.replace(fullPath + '/', '');
          const newPath = `${targetPath}/${relativePath}`;
          
          await copyObject(minioConfig.bucketName, newPath, `${minioConfig.bucketName}/${object}`);
          await removeObject(minioConfig.bucketName, object);
        }

        const simulation = {
          id: simulationId,
          name: simulationId,
          path: targetPath,
          originalPath: fullPath,
          movedAt: new Date().toISOString()
        };

        movedSimulations.push(simulation);
        console.log(`✅ Simulação movida: ${simulationId}`);
      } catch (error) {
        console.error(`❌ Erro ao mover simulação ${inputPath}:`, error);
        invalidPaths.push({
          path: inputPath,
          error: error.message
        });
      }
    }

    if (movedSimulations.length === 0) {
      return res.status(400).json({
        error: 'No folders were moved',
        invalidPaths
      });
    }

    // Update exercise metadata
    metadata.simulations.push(...movedSimulations);
    metadata.updatedAt = new Date().toISOString();

    await writeJsonToMinio(
      minioConfig.bucketName,
      `${minioConfig.exercisesPartition}/${id}/exercise-metadata.json`,
      metadata
    );

    // Update staging manifest
    const stagingManifest = await getStagingManifest();
    const filteredManifest = stagingManifest.filter(item => 
      !inputPaths.some(inputPath => 
        item.data.path === inputPath || item.data.path === `${minioConfig.stagingPartition}/${inputPath}`
      )
    );
    await updateStagingManifest(filteredManifest);

    console.log(`✅ Simulações movidas com sucesso. Total: ${movedSimulations.length}`);
    res.status(200).json({
      message: 'Simulation data moved successfully',
      moved: movedSimulations,
      invalid: invalidPaths.length > 0 ? invalidPaths : undefined,
      summary: {
        total: inputPaths.length,
        successful: movedSimulations.length,
        failed: invalidPaths.length
      }
    });
  } catch (error) {
    console.error('❌ Erro ao mover dados de simulação:', error);
    res.status(500).json({
      error: 'Error moving simulation data',
      details: error.message
    });
  }
}