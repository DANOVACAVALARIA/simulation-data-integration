import fs from 'fs'
import { promisify } from 'util'
const unlink = promisify(fs.unlink)

export const deleteLocalFile = async (filePath) => {
  try {
    await unlink(filePath)
    console.log(`COLLECTOR-PROTO (minIO) => file ${filePath} deleted from local folder.`)
    return true
  } catch (err) {
    console.error('COLLECTOR-PROTO (minIO) => error deleting file:', err)
    return false
  }
}