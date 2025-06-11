import fs from 'fs'

export const saveProtoFile = (protoBufferArray, session) => {  

  if (protoBufferArray.length > 0) {
    const protoFileIndex = session.protoFiles.length + 1
    const protoFileName = `session-proto_${protoFileIndex}.bin`
    const filePath = `${session.dir}/${protoFileName}`
    const protoFile = fs.createWriteStream(filePath)

    protoBufferArray.forEach((proto) => protoFile.write(proto))
    // protoBufferArray = []
    protoBufferArray.length = 0

    const protoFileNames = session.protoFiles
    protoFileNames.push(protoFileName)
  }
}