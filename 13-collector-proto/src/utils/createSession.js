import fs from 'fs'

const dateTimestampShortener = (date) => {
  const pad = (n) => String(n).padStart(2, '0')
  return `${pad(date.getFullYear())}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`	
}

const hourTimestampShortener = (date) => {
  const pad = (n) => String(n).padStart(2, '0')
  return `${pad(date.getHours())}-${pad(date.getMinutes())}.${pad(date.getSeconds())}`
}

export const createSession = (baseDir, clientIp, port) => {

  const sessionStart = new Date()
  const safeIp = clientIp.replace(/::ffff:/, '').replace(/:/g, '_')
  const parentFolder = `${safeIp}_${dateTimestampShortener(sessionStart)}`
  const childFolder = `${hourTimestampShortener(sessionStart)}`	  

  const metadataFileName = `session-metadata.json`
  
  // create folders
  const sessionDir = `${baseDir}/${parentFolder}/${childFolder}`
  if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true })
  
  console.log('COLLECTOR-PROTO => session successfully created...')
  
  return {
    sessionStart: new Date().toISOString(),
    sessionEnd: null,
    DurationInMinutes: 0,
    port: port,
    protoMsgs: 0,
    totalKB: 0,
    connections: 0,
    sources: [clientIp],
    parentFolder: parentFolder,
    childFolder: childFolder,
    protoFiles: [],
    metadataFile: metadataFileName,
    baseDir: baseDir,
    dir: sessionDir,
  }
}