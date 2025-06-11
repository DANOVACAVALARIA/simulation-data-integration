import dgram from 'dgram'
import fs from 'fs'

import seedrandom from 'seedrandom'

// scripts
import { encodeESPDU } from './lib/encodeESPDU.js'
import { generateMessages } from './lib/generateMessages.js'
import { saveCsvFile } from './lib/saveCsvFile.js'
import { generatorDisConfig } from './config/generatorDisConfig.js'

// load config
const config = await generatorDisConfig()

// getting execution number
const execArg = process.argv.find(arg => arg.startsWith('--exec='))
const execNum = execArg ? parseInt(execArg.split('=')[1]) : 0

// client instance
const client = dgram.createSocket('udp4')

// metrics (DIS)
let packetCount = 0
const metricsFile = fs.createWriteStream(`${config.metricsDir}/dis-sender-metrics-${execNum}.csv`)
metricsFile.write('PacketCount,Sent_Time,Encoding_Time,Size_In_Bytes,Msg_Rate\n')
let metrics = []

// create seed
const rng = seedrandom(config.seed)

// PDU msg sent to server
const sendESPDU = async () => {
  const pduData = await encodeESPDU(rng)
  
  packetCount++
  metrics.push(`${packetCount},${pduData.sentTime},${pduData.encodingTime.toFixed(4)},${pduData.encodedPdu.length},${(packetCount / (pduData.sentTime/1000)).toFixed(2)}`)
  client.send(pduData.encodedPdu, 0, pduData.encodedPdu.length, config.udpPort, config.udpReceiverIp, (err, bytes) => {
    
    if (err) {
      console.error('Error on sending ESPU with description ', err)
    } else {
      console.log(`[DIS-SENDER] => ESPDU sent to ${config.udpReceiverIp}:${config.udpPort} with ${bytes} bytes | Encoding time: ${pduData.encodingTime.toFixed(4)}.`)
    }
  })

  // ---termination check
  if (packetCount == config.totalEspduToBeSent) {
    clearInterval(saveMetrics)
    setTimeout(async() => {
      if(await saveCsvFile(metricsFile, metrics)) {
        metrics = []
        metricsFile.end()
        client.close(() => {
          console.log('DIS-GENERATOR HAS FINISHED')
          process.exit(0)
        })
      }
    }, config.terminationCheckIntervalMs)    
  }  
}

// ---save metrics
const saveMetrics = setInterval(async () => {
  if (await saveCsvFile(metricsFile, metrics)) {
    metrics = []
  }
}, config.applicationSavingIntervalMs)


// ---start PDU generation
generateMessages(config.totalEspduToBeSent,
                config.intervalBetweenEspduInMs,
                sendESPDU)