import fs from 'fs/promises'
import path from 'path'
import { WebSocketServer } from 'ws'

// scripts
import { collectorDisConfig } from './config/collectorDisConfig.js'
import { createSession } from './utils/createSession.js'
import { terminateSession } from './utils/terminateSession.js'
import { savePduFile } from './utils/savePduFile.js'
import { saveMetadataFile } from './utils/saveMetadataFile.js'

// load config
const config = await collectorDisConfig()

// metrics storage
let session = null
let pduBufferArray = []
let pduCount = 0
let totalKB = 0
let disconnectCount = 0
let lastDisconnectTimes = {} // { clientIp: timestamp }
let successfulUploads = 0
let failedUploads = 0

// save metrics to CSV
async function saveMetricToCsv(metricName, data) {
  const timestamp = new Date().toISOString()
  const filePath = path.join(config.metricsDir, `${metricName}.csv`)
  const header = !await fs.access(filePath).then(() => true).catch(() => false)
  const csvLine = header ? `timestamp,${Object.keys(data).join(',')}\n` : ''
  const values = Object.values(data).join(',')
  await fs.appendFile(filePath, `${csvLine}${timestamp},${values}\n`)
}

// WebSocket server instance
const wss = new WebSocketServer({ 
  port: config.wsLocalPort,
  perMessageDeflate: false, // Disable compression for better performance with small messages
})

console.log(`COLLECTOR-DIS => WebSocket server started on port ${config.wsLocalPort}...`)

// Handle WebSocket connections
wss.on('connection', (socket, request) => {
  const clientIp = request.socket.remoteAddress
  console.log(`COLLECTOR-DIS => new connection from ${clientIp}.`)

  // Check for reestablishment time
  if (lastDisconnectTimes[clientIp]) {
    const reconnectTime = Date.now()
    const reestablishmentTime = (reconnectTime - lastDisconnectTimes[clientIp]) / 1000
    saveMetricToCsv('reestablishment_time', { clientIp, reestablishmentTime })
    delete lastDisconnectTimes[clientIp]
  }

  // Create session
  if (!session) {
    session = createSession(config.baseDataDir, clientIp, config.wsLocalPort)
    savePduFile(pduBufferArray, session)
    saveMetadataFile(session)
  } else {
    session.sources.push(clientIp) // Fixed: use push instead of append
  }

  session.connections++

  // Handle incoming messages
  socket.on('message', (data) => {
    console.log(`COLLECTOR-DIS => received message with ${data.length} bytes.`)
    pduBufferArray.push(data)
    pduCount++
    session.pdus = pduCount
    totalKB += data.length
    session.totalKB = totalKB
  })

  // Handle connection close
  socket.on('close', (code, reason) => {
    disconnectCount++
    lastDisconnectTimes[clientIp] = Date.now()
    saveMetricToCsv('disconnect_count', { disconnectCount })

    console.log(`COLLECTOR-DIS => connection with ${clientIp} ended. Code: ${code}, Reason: ${reason}`)

    // Only terminate session after a delay to allow for reconnections
    setTimeout(() => {
      // Check if no active connections remain
      const activeConnections = Array.from(wss.clients).filter(client => client.readyState === 1)
      
      if (activeConnections.length === 0 && session) {
        console.log('COLLECTOR-DIS => No active connections, terminating session...')
        
        const startUploadTime = Date.now()
        let isTerminated = false
        try {
          isTerminated = terminateSession(`${session.dir}/${session.metadataFile}`)
          successfulUploads++
        } catch (err) {
          console.error(`Upload failed: ${err.message}`)
          failedUploads++
        }
        
        const uploadTime = (Date.now() - startUploadTime) / 1000
        const successRate = (successfulUploads / (successfulUploads + failedUploads)) * 100

        // Save metrics
        saveMetricToCsv('upload_time', { uploadTime })
        saveMetricToCsv('upload_success_rate', { successRate })

        if (isTerminated) session = null
      }
    }, 30000) // 30 second delay
  })

  // Handle errors
  socket.on('error', (error) => {
    console.error(`COLLECTOR-DIS => WebSocket error from ${clientIp}:`, error)
  })
})

// Save data periodically
setInterval(() => {
  if (session) {
    savePduFile(pduBufferArray, session)
    saveMetadataFile(session)
  }
}, config.pduSaveInterval)

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('COLLECTOR-DIS => Shutting down gracefully...')
  wss.close(() => {
    console.log('COLLECTOR-DIS => WebSocket server closed.')
    process.exit(0)
  })
})