import dgram from 'dgram'
import WebSocket from 'ws'

// scripts
import { listenerDisConfig } from './config/listenerDisConfig.js'

// load config
const config = await listenerDisConfig()

// UDP server instance
const server = dgram.createSocket('udp4')

// WebSocket client instance
let socket = null
let isConnected = false

// Connection function
function connect() {
  socket = new WebSocket(`ws://${config.wsDestHost}:${config.wsDestPort}`)
  
  socket.on('open', () => {
    isConnected = true
    console.log(`LISTENER-DIS => connected to WebSocket server at ${config.wsDestHost}:${config.wsDestPort}`)
  })

  socket.on('close', (code, reason) => {
    isConnected = false
    console.log(`LISTENER-DIS => connection lost! Code: ${code}, Reason: ${reason}. Trying to reconnect...`)
    
    // Reconnect after 5 seconds
    setTimeout(() => {
      if (!isConnected) {
        connect()
      }
    }, 5000)
  })

  socket.on('error', (error) => {
    console.error('LISTENER-DIS => WebSocket error:', error)
    isConnected = false
  })
}

// Handle UDP messages
server.on('message', (msg, rinfo) => {
  console.log(`LISTENER-DIS => received UDP message from ${rinfo.address}:${rinfo.port} with ${msg.length} bytes.`)
  
  // Forward to collector via WebSocket if connected
  if (isConnected && socket.readyState === WebSocket.OPEN) {
    socket.send(msg)
    console.log(`LISTENER-DIS => forwarded message to collector via WebSocket (${msg.length} bytes)`)
  } else {
    console.warn('LISTENER-DIS => Cannot forward message, WebSocket not connected')
  }
})

// Handle UDP server errors
server.on('error', (err) => {
  console.error('LISTENER-DIS => UDP server error:', err)
})

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('LISTENER-DIS => Shutting down gracefully...')
  
  // Close UDP server
  server.close(() => {
    console.log('LISTENER-DIS => UDP server closed')
  })
  
  // Close WebSocket connection
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.close(1000, 'Process terminating')
  }
  
  process.exit(0)
})

// Initialize WebSocket connection
connect()

// Start UDP server
server.bind(config.udpLocalPort, () => {
  console.log(`LISTENER-DIS => UDP server started on port ${config.udpLocalPort}...`)
})





















// import dgram from 'dgram'
// import io from 'socket.io-client'

// // scripts
// import { listenerDisConfig } from './config/listenerDisConfig.js'

// // load config
// const config = await listenerDisConfig()

// // UDP server instance
// const server = dgram.createSocket('udp4')

// // WS client instance
// const socket = io(`ws://${config.wsDestHost}:${config.wsDestPort}`)


// // handle UDP messages
// server.on('message', (msg, rinfo) => {
//   console.log(`LISTENER-DIS => received message from ${rinfo.address}:${rinfo.port} with ${msg.length} bytes.`)
//   socket.emit('dis-message', msg)
// })

// // handle WS events
// socket.on('connect', () => {
//   console.log(`LISTENER-DIS => connected to WS server at ${config.wsDestHost}:${config.wsDestPort}`)
// })

// socket.on('disconnect', () => {
//   console.log('LISTENER-DIS => connection lost! Trying to reconect...')
//   setTimeout(() => socket.connect(), 5000) // reconnects after 5 seconds
// })

// // start UDP server
// server.bind(config.udpLocalPort, () => {
//   console.log(`LISTENER-DIS => UDP server started on port ${config.udpLocalPort}...`)
// })