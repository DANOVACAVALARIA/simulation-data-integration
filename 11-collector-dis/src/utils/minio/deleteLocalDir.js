import fs from 'fs'
import path from 'path'
export const deleteLocalDir = async (sessionData) => {
  try {
    const fullPath = sessionData.dir
    await fs.promises.rm(fullPath, { recursive: true, force: true })

    const parentDir = path.join(sessionData.baseDir, sessionData.parentFolder)
    await fs.promises.rm(parentDir, { recursive: true, force: true  })
    
    console.log(`COLLECTOR-DIS (minIO) => directory ${sessionData.dir} deleted`)
    return true
  } catch (err) {
    console.error('COLLECTOR-DIS (minIO) => error deleting directory:', err)
    return false
  }
}