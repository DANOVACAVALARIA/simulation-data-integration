import { spawn } from 'child_process'
import fs from 'fs/promises'

// configs
const MODE = 'DIS'    // 'DIS' ou 'PROTO'

const TOTAL_OF_EXPERIMENTS = 30
const DELAY_BETWEEN_EXPERIMENTS_MS = 30000
const EXPERIMENT_TIMEOUT_MS = 30000 // Timeout só para casos de erro/travamento
const CONNECTION_TIMEOUT_MS = 60000 // Timeout para estabelecer conexão inicial

const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms))

const logWithTimestamp = (message) => {
  console.log(`[${new Date().toISOString()}] ${message}`)
}

const validateScripts = async () => {
  try {
    if (MODE == 'DIS') {
      await fs.access('listener-dis/listener-dis.js')
      await fs.access('generator-dis/generator-dis.js')
    } else {  // MODE == 'PROTO'
      await fs.access('generator-proto/generator-proto.js')
    }
    
  } catch (err) {
    throw new Error(`ORCHESTRATOR => script files not found for ${MODE} mode. See error message: ` + err.message)
  }
}

const executeExperiment = async (number) => {
  logWithTimestamp(`ORCHESTRATOR => starting experiment number ${number} of ${TOTAL_OF_EXPERIMENTS}`)
  
  return new Promise((resolve, reject) => {
    let disListener = null
    let generator = null
    let experimentStarted = false
    let connectionEstablished = false

    let timeoutId = null
    let connectionTimeoutId = null
    
    const cleanup = () => {
      if (timeoutId) clearTimeout(timeoutId)
      if (connectionTimeoutId) clearTimeout(connectionTimeoutId)
      if (disListener) disListener.kill('SIGTERM')
      if (generator) generator.kill('SIGTERM')
    }
    
    const activateTimeoutOnDisconnection = () => {
      if (timeoutId) clearTimeout(timeoutId)
      
      // Só ativa timeout se houver desconexão E o experimento já começou
      if (experimentStarted && !connectionEstablished) {
        logWithTimestamp(`ORCHESTRATOR => Connection lost for experiment ${number}, starting timeout...`)
        timeoutId = setTimeout(() => {
          logWithTimestamp(`ORCHESTRATOR => Experiment ${number} timed out due to disconnection without reconnection`)
          cleanup()
          reject(new Error(`Experiment ${number} timed out after ${EXPERIMENT_TIMEOUT_MS}ms of disconnection`))
        }, EXPERIMENT_TIMEOUT_MS)
      }
    }
    
    const cancelTimeout = () => {
      if (timeoutId) {
        clearTimeout(timeoutId)
        timeoutId = null
        logWithTimestamp(`ORCHESTRATOR => Timeout cancelled for experiment ${number} - connection restored`)
      }
    }
    
    // Timeout para conexão inicial (mais restritivo)
    connectionTimeoutId = setTimeout(() => {
      if (!connectionEstablished) {
        logWithTimestamp(`ORCHESTRATOR => Experiment ${number} failed to establish connection within ${CONNECTION_TIMEOUT_MS}ms`)
        cleanup()
        reject(new Error(`Experiment ${number} connection timeout`))
      }
    }, CONNECTION_TIMEOUT_MS)
    
    if (MODE == 'DIS') {
      disListener = spawn('node', ['listener-dis/listener-dis.js'])
      generator = spawn('node', ['generator-dis/generator-dis.js', `--exec=${number}`])

      disListener.stdout.on('data', (data) => {
        const output = data.toString()
        process.stdout.write(`[LISTENER-DIS ${number}] ${data}`)
        
        // Detecta quando a conexão é estabelecida
        if (output.includes('connected to WebSocket server')) {
          connectionEstablished = true
          if (connectionTimeoutId) clearTimeout(connectionTimeoutId)
          cancelTimeout() // Cancela timeout se estava ativo por desconexão
          logWithTimestamp(`ORCHESTRATOR => Connection established for experiment ${number}`)
        }
        
        // Detecta perda de conexão
        if (output.includes('connection lost') || output.includes('Trying to reconnect')) {
          connectionEstablished = false
          activateTimeoutOnDisconnection()
        }
      })

      disListener.stderr.on('data', (data) => {
        process.stderr.write(`[LISTENER-DIS ${number} ERROR] ${data}`)
        
        // Se há erro de conexão, marca como desconectado
        if (data.toString().includes('connection') || data.toString().includes('error')) {
          connectionEstablished = false
          activateTimeoutOnDisconnection()
        }
      })

      generator.stdout.on('data', (data) => {
        const output = data.toString()
        process.stdout.write(`[GENERATOR-DIS ${number}] ${data}`)
        
        // Detecta início da geração (marca início do experimento)
        if (output.includes('ESPDU sent') && !experimentStarted) {
          experimentStarted = true
          if (connectionTimeoutId) clearTimeout(connectionTimeoutId)
          logWithTimestamp(`ORCHESTRATOR => Experiment ${number} started generating messages`)
        }
        
        // Detecta fim do experimento
        if (output.includes('DIS-GENERATOR HAS FINISHED')) {
          logWithTimestamp(`ORCHESTRATOR => Experiment ${number} completed successfully`)
          cleanup()
          resolve()
        }
      })

      generator.stderr.on('data', (data) => {
        process.stderr.write(`[GENERATOR-DIS ${number} ERROR] ${data}`)
        cleanup()
        reject(new Error(`Generator error: ${data}`))
      })
      
    } else {  // MODE == 'PROTO'
      generator = spawn('node', ['generator-proto/generator-proto.js', `--exec=${number}`])

      generator.stdout.on('data', (data) => {
        const output = data.toString()
        process.stdout.write(`[GENERATOR-PROTO ${number}] ${data}`)
        
        // Detecta quando a conexão WebSocket é estabelecida (igual ao DIS)
        if (output.includes('connected to WebSocket server')) {
          connectionEstablished = true
          if (connectionTimeoutId) clearTimeout(connectionTimeoutId)
          cancelTimeout() // Cancela timeout se estava ativo por desconexão
          logWithTimestamp(`ORCHESTRATOR => Connection established for experiment ${number}`)
        }
        
        // Detecta perda de conexão
        if (output.includes('connection lost') || output.includes('Trying to reconnect')) {
          connectionEstablished = false
          activateTimeoutOnDisconnection()
        }
        
        // Detecta início da geração de mensagens (marca início do experimento)
        if (!experimentStarted && connectionEstablished) {
          experimentStarted = true
          logWithTimestamp(`ORCHESTRATOR => Experiment ${number} started generating messages`)
        }
        
        if (output.includes('PROTO-GENERATOR HAS FINISHED')) {
          logWithTimestamp(`ORCHESTRATOR => Experiment ${number} completed successfully`)
          cleanup()
          resolve()
        }
      })

      generator.stderr.on('data', (data) => {
        process.stderr.write(`[GENERATOR-PROTO ${number} ERROR] ${data}`)
        
        // Se há erro de conexão, marca como desconectado
        if (data.toString().includes('connection') || data.toString().includes('WebSocket error')) {
          connectionEstablished = false
          activateTimeoutOnDisconnection()
        }
        
        // Se é erro fatal, falha o experimento
        if (data.toString().includes('error') && !data.toString().includes('WebSocket error')) {
          cleanup()
          reject(new Error(`Generator error: ${data}`))
        }
      })
    }

    // Handlers para detecção de crash/saída inesperada
    if (disListener) {
      disListener.on('exit', (code, signal) => {
        if (code !== 0 && code !== null) {
          logWithTimestamp(`LISTENER-DIS ${number} exited unexpectedly with code ${code}, signal ${signal}`)
          cleanup()
          reject(new Error(`Listener crashed with code ${code}`))
        }
      })
    }

    if (generator) {
      generator.on('exit', (code, signal) => {
        if (code !== 0 && code !== null) {
          logWithTimestamp(`GENERATOR ${number} exited unexpectedly with code ${code}, signal ${signal}`)
          cleanup()
          reject(new Error(`Generator crashed with code ${code}`))
        }
      })
    }
  })
}

const main = async () => {
  try {
    await validateScripts()
    for (let i = 1; i <= TOTAL_OF_EXPERIMENTS; i++) {
      await executeExperiment(i)
      if (i < TOTAL_OF_EXPERIMENTS) {
        logWithTimestamp(`ORCHESTRATOR => waiting ${DELAY_BETWEEN_EXPERIMENTS_MS / 1000} seconds...`)
        await delay(DELAY_BETWEEN_EXPERIMENTS_MS)
      }
    }
    logWithTimestamp('All experiments were executed.')
  } catch (err) {
    logWithTimestamp(`ORCHESTRATOR => Error: ${err.message}`)
  }
}

main()









































// import { spawn } from 'child_process'
// import fs from 'fs/promises'

// // configs
// const MODE = 'PROTO'    // 'DIS' ou 'PROTO'

// const TOTAL_OF_EXPERIMENTS = 5
// const DELAY_BETWEEN_EXPERIMENTS_MS = 3000
// const EXPERIMENT_TIMEOUT_MS = 30000

// const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms))

// const logWithTimestamp = (message) => {
//   console.log(`[${new Date().toISOString()}] ${message}`)
// }

// const validateScripts = async () => {
//   try {
//     if (MODE == 'DIS') {
//       await fs.access('listener-dis/listener-dis.js')
//       await fs.access('generator-dis/generator-dis.js')
//     } else {  // MODE == 'PROTO'
//       await fs.access('generator-proto/generator-proto.js')
//     }
    
//   } catch (err) {
//     throw new Error(`ORCHESTRATOR => script files not found for ${MODE} mode. See error message: ` + err.message)
//   }
// }

// const executeExperiment = async (number) => {
//   logWithTimestamp(`ORCHESTRATOR => starting experiment number ${number} of ${TOTAL_OF_EXPERIMENTS}`)
//   return Promise.race([
//     new Promise((resolve, reject) => {
//       if (MODE == 'DIS') {
//         const disListener = spawn('node', ['listener-dis/listener-dis.js'])
//         const generator = spawn('node', ['generator-dis/generator-dis.js', `--exec=${number}`])

//         disListener.stdout.on('data', (data) => {
//           process.stdout.write(`[LISTENER-DIS ${number}] ${data}`)
//         })

//         disListener.stderr.on('data', (data) => {
//           process.stderr.write(`[LISTENER-DIS ${number} ERROR] ${data}`)
//         })

//         generator.stdout.on('data', (data) => {
//           process.stdout.write(`[GENERATOR-DIS ${number}] ${data}`)
//           if (data.toString().includes('DIS-GENERATOR HAS FINISHED')) {
//             disListener.kill('SIGTERM')
//             generator.kill('SIGTERM')
//             resolve()
//           }
//         })

//         generator.stderr.on('data', (data) => {
//           process.stderr.write(`[GENERATOR-DIS ${number} ERROR] ${data}`)
//           disListener.kill('SIGTERM')
//           generator.kill('SIGTERM')
//           reject(new Error(`Generator error: ${data}`))
//         })
//       } else {  // MODE == 'PROTO'
//         const generator = spawn('node', ['generator-proto/generator-proto.js', `--exec=${number}`])

//         generator.stdout.on('data', (data) => {
//           process.stdout.write(`[GENERATOR-PROTO ${number}] ${data}`)
//           if (data.toString().includes('PROTO-GENERATOR HAS FINISHED')) {
//             // disListener.kill('SIGTERM')
//             generator.kill('SIGTERM')
//             resolve()
//           }
//         })

//         generator.stderr.on('data', (data) => {
//           process.stderr.write(`[GENERATOR-PROTO ${number} ERROR] ${data}`)
//           // disListener.kill('SIGTERM')
//           generator.kill('SIGTERM')
//           reject(new Error(`Generator error: ${data}`))
//         })
//       }      

//       // disListener.on('exit', (code, signal) => {
//       //   logWithTimestamp(`LISTENER-DIS ${number} exited with code ${code}, signal ${signal}`)
//       // })

//       // generator.on('exit', (code, signal) => {
//       //   logWithTimestamp(`GENERATOR-DIS ${number} exited with code ${code}, signal ${signal}`)
//       // })
//     }),
//     new Promise((_, reject) => {
//       setTimeout(() => {
//         reject(new Error(`Experiment ${number} timed out after ${EXPERIMENT_TIMEOUT_MS / 1000} seconds`))
//       }, EXPERIMENT_TIMEOUT_MS)
//     }),
//   ])
// }

// const main = async () => {
//   try {
//     await validateScripts()
//     for (let i = 1; i <= TOTAL_OF_EXPERIMENTS; i++) {
//       await executeExperiment(i)
//       if (i < TOTAL_OF_EXPERIMENTS) {
//         logWithTimestamp(`ORCHESTRATOR => waiting ${DELAY_BETWEEN_EXPERIMENTS_MS / 1000} seconds...`)
//         await delay(DELAY_BETWEEN_EXPERIMENTS_MS)
//       }
//     }
//     logWithTimestamp('All experiments were executed.')
//   } catch (err) {
//     logWithTimestamp(`ORCHESTRATOR => Error: ${err.message}`)
//   }
// }

// main()