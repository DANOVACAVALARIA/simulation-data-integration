import fs from 'fs'
import path from 'path'
import { promisify } from 'util'

const readdir = promisify(fs.readdir)
const stat = promisify(fs.stat)


export const getFilesInDirectory = async (directoryPath) => {
  try {
    const items = await readdir(directoryPath)
    const files = []

    for (const item of items) {
      const fullPath = path.join(directoryPath, item)
      const stats = await stat(fullPath)

      if (stats.isFile()) {
        files.push({
          fullPath,
          fileName: item
        })
      }
    }

    return files    
  } catch (err) {
    console.error('COLLECTOR-DIS (minIO) => error reading directory:', err)
    return []
  }
}