import fs from 'fs'

export const savePduFile = (pduBufferArray, session) => {  

  if (pduBufferArray.length > 0) {
    const pduFileIndex = session.pduFiles.length + 1
    const pduFileName = `session-pdus_${pduFileIndex}.bin`
    const filePath = `${session.dir}/${pduFileName}`
    const pduFile = fs.createWriteStream(filePath)

    pduBufferArray.forEach((pdu) => pduFile.write(pdu))
    // pduBufferArray = [] 
    pduBufferArray.length = 0

    const pduFileNames = session.pduFiles
    pduFileNames.push(pduFileName)
  }
}