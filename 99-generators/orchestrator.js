import { spawn } from 'child_process'
import fs from 'fs/promises'

// configs
const MODE = 'PROTO'    // 'DIS' ou 'PROTO'

const TOTAL_OF_EXPERIMENTS = 5
const DELAY_BETWEEN_EXPERIMENTS_MS = 3000
const EXPERIMENT_TIMEOUT_MS = 30000

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
  return Promise.race([
    new Promise((resolve, reject) => {
      if (MODE == 'DIS') {
        const disListener = spawn('node', ['listener-dis/listener-dis.js'])
        const generator = spawn('node', ['generator-dis/generator-dis.js', `--exec=${number}`])

        disListener.stdout.on('data', (data) => {
          process.stdout.write(`[LISTENER-DIS ${number}] ${data}`)
        })

        disListener.stderr.on('data', (data) => {
          process.stderr.write(`[LISTENER-DIS ${number} ERROR] ${data}`)
        })

        generator.stdout.on('data', (data) => {
          process.stdout.write(`[GENERATOR-DIS ${number}] ${data}`)
          if (data.toString().includes('DIS-GENERATOR HAS FINISHED')) {
            disListener.kill('SIGTERM')
            generator.kill('SIGTERM')
            resolve()
          }
        })

        generator.stderr.on('data', (data) => {
          process.stderr.write(`[GENERATOR-DIS ${number} ERROR] ${data}`)
          disListener.kill('SIGTERM')
          generator.kill('SIGTERM')
          reject(new Error(`Generator error: ${data}`))
        })
      } else {  // MODE == 'PROTO'
        const generator = spawn('node', ['generator-proto/generator-proto.js', `--exec=${number}`])

        generator.stdout.on('data', (data) => {
          process.stdout.write(`[GENERATOR-PROTO ${number}] ${data}`)
          if (data.toString().includes('PROTO-GENERATOR HAS FINISHED')) {
            // disListener.kill('SIGTERM')
            generator.kill('SIGTERM')
            resolve()
          }
        })

        generator.stderr.on('data', (data) => {
          process.stderr.write(`[GENERATOR-PROTO ${number} ERROR] ${data}`)
          // disListener.kill('SIGTERM')
          generator.kill('SIGTERM')
          reject(new Error(`Generator error: ${data}`))
        })
      }      

      // disListener.on('exit', (code, signal) => {
      //   logWithTimestamp(`LISTENER-DIS ${number} exited with code ${code}, signal ${signal}`)
      // })

      // generator.on('exit', (code, signal) => {
      //   logWithTimestamp(`GENERATOR-DIS ${number} exited with code ${code}, signal ${signal}`)
      // })
    }),
    new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error(`Experiment ${number} timed out after ${EXPERIMENT_TIMEOUT_MS / 1000} seconds`))
      }, EXPERIMENT_TIMEOUT_MS)
    }),
  ])
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