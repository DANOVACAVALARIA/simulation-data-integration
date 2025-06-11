import fs from 'fs'
import WebSocket from 'ws'
import seedrandom from 'seedrandom'

// scripts
import { encodeProto } from './lib/encodeProto.js'
import { generateMessages } from './lib/generateMessages.js'
import { saveCsvFile } from './lib/saveCsvFile.js'
import { generatorProtoConfig } from './config/generatorProtoConfig.js'

// load config
const config = await generatorProtoConfig()

// getting execution number
const execArg = process.argv.find(arg => arg.startsWith('--exec='))
const execNum = execArg ? parseInt(execArg.split('=')[1]) : 0

// WebSocket client instance
let socket = null
let isConnected = false

// metrics (PROTO)
let packetCount = 0
const metricsFile = fs.createWriteStream(`${config.metricsDir}/proto-sender-metrics-${execNum}.csv`)
metricsFile.write('PacketCount,Sent_Time,Encoding_Time,Size_In_Bytes,Msg_Rate\n')
let metrics = []

// create seed
const rng = seedrandom(config.seed)

// Connection function
function connect() {
  socket = new WebSocket(`ws://${config.wsDestHost}:${config.wsDestPort}`)
  
  socket.on('open', () => {
    isConnected = true
    console.log(`GENERATOR-PROTO => connected to WebSocket server at ${config.wsDestHost}:${config.wsDestPort}`)
  })

  socket.on('close', (code, reason) => {
    isConnected = false
    console.log(`GENERATOR-PROTO => connection lost! Code: ${code}, Reason: ${reason}. Trying to reconnect...`)
    
    // Reconnect after 5 seconds
    setTimeout(() => {
      if (!isConnected) {
        connect()
      }
    }, 5000)
  })

  socket.on('error', (error) => {
    console.error('GENERATOR-PROTO => WebSocket error:', error)
    isConnected = false
  })
}

// PDU message sent to server
const sendProtoMessage = async () => {
  if (isConnected && socket.readyState === WebSocket.OPEN) {
    const protoData = await encodeProto(rng)
  
    packetCount++
    metrics.push(`${packetCount},${protoData.sentTime},${protoData.encodingTime.toFixed(4)},${protoData.encodedProto.length},${(packetCount / (protoData.sentTime/1000)).toFixed(2)}`)

    // Send binary data
    socket.send(protoData.encodedProto)

    // Termination check
    if (packetCount === config.totalEspduToBeSent) {
      clearInterval(saveMetrics)
      setTimeout(async() => {
        if (await saveCsvFile(metricsFile, metrics)) {
          metrics = []
          metricsFile.end()
          
          // Close connection gracefully
          if (socket.readyState === WebSocket.OPEN) {
            socket.close(1000, 'Generation completed')
          }
          
          console.log('PROTO-GENERATOR HAS FINISHED')
          process.exit(0)
        }
      }, config.terminationCheckIntervalMs)    
    } 
  } else {
    console.log('GENERATOR-PROTO => Cannot send message, WebSocket not connected')
  }
}

// Save metrics periodically
const saveMetrics = setInterval(async () => {
  if (await saveCsvFile(metricsFile, metrics)) {
    metrics = []
  }
}, config.applicationSavingIntervalMs)

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('GENERATOR-PROTO => Shutting down gracefully...')
  clearInterval(saveMetrics)
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.close(1000, 'Process terminating')
  }
  process.exit(0)
})

// Initialize connection and start generation
connect()

// Wait for connection before starting message generation
socket.on('open', () => {
  generateMessages(config.totalEspduToBeSent,
                  config.intervalBetweenProtoMsgInMs,
                  sendProtoMessage)
})






















// import fs from 'fs'

// import seedrandom from 'seedrandom'
// import io from 'socket.io-client'

// // scripts
// import { encodeProto } from './lib/encodeProto.js'
// import { generateMessages } from './lib/generateMessages.js'
// import { saveCsvFile } from './lib/saveCsvFile.js'
// import { generatorProtoConfig } from './config/generatorProtoConfig.js'

// // load config
// const config = await generatorProtoConfig()

// // getting execution number
// const execArg = process.argv.find(arg => arg.startsWith('--exec='))
// const execNum = execArg ? parseInt(execArg.split('=')[1]) : 0

// // WS lient instance
// const socket = io(`ws://${config.wsDestHost}:${config.wsDestPort}`)

// // metrics (PROTO)
// let packetCount = 0
// const metricsFile = fs.createWriteStream(`${config.metricsDir}/proto-sender-metrics-${execNum}.csv`)
// metricsFile.write('PacketCount,Sent_Time,Encoding_Time,Size_In_Bytes,Msg_Rate\n')
// let metrics = []

// // create seed
// const rng = seedrandom(config.seed)

// // handle WS events
// socket.on('connect', () => {
//   console.log(`GENERATOR-PROTO => connected to WS server at ${config.wsDestHost}:${config.wsDestPort}`)
// })

// socket.on('disconnect', () => {
//   console.log('GENERATOR-PROTO => connection lost! Trying to reconect...')
//   setTimeout(() => socket.connect(), 5000) // reconnects after 5 seconds
// })

// // PDU msg sent to server
// const sendESPDU = async () => {
//   if (socket.connected) {
//     const protoData = await encodeProto(rng)
  
//     packetCount++
//     metrics.push(`${packetCount},${protoData.sentTime},${protoData.encodingTime.toFixed(4)},${protoData.encodedProto.length},${(packetCount / (protoData.sentTime/1000)).toFixed(2)}`)

//     socket.emit('proto-message', protoData.encodedProto)

//     // ---termination check
//     if (packetCount == config.totalEspduToBeSent) {
//       clearInterval(saveMetrics)
//       setTimeout(async() => {
//         if(await saveCsvFile(metricsFile, metrics)) {
//           metrics = []
//           metricsFile.end()
//           socket.close(() => {
//             console.log('PROTO-GENERATOR HAS FINISHED')
//             process.exit(0)
//           })
//         }
//       }, config.terminationCheckIntervalMs)    
//     } 
//   }
// }

// // ---save metrics
// const saveMetrics = setInterval(async () => {
//   if (await saveCsvFile(metricsFile, metrics)) {
//     metrics = []
//   }
// }, config.applicationSavingIntervalMs)


// // ---start PDU generation
// generateMessages(config.totalEspduToBeSent,
//                 config.intervalBetweenProtoMsgInMs,
//                 sendESPDU)