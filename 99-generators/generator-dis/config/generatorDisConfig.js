export const generatorDisConfig = async () => {

  const config = {
    udpPort: 3000,
    udpReceiverIp: 'localhost',
    totalEspduToBeSent: 100,
    intervalBetweenEspduInMs: 1,
    terminationCheckIntervalMs: 2000,
    applicationSavingIntervalMs: 30000,
    metricsDir: '../15-metrics/generator-dis',
    seed: 123
  }

  // environment variables
  if (process.env.UDP_PORT) config.udpPort = process.env.UDP_PORT
  if (process.env.UDP_RECEIVER_IP) config.udpReceiverIp = process.env.UDP_RECEIVER_IP

  if (process.env.TOTAL_ESPDU_TO_BE_SENT) config.totalEspduToBeSent = process.env.TOTAL_ESPDU_TO_BE_SENT
  if(process.env.INTERVAL_BETWEEN_ESPDU_IN_MS) config.intervalBetweenEspduInMs = process.env.INTERVAL_BETWEEN_ESPDU_IN_MS
  if (process.env.TERMINATION_CHECK_INTERVAL_MS) config.terminationCheckIntervalMs = process.env.TERMINATION_CHECK_INTERVAL_MS
  if (process.env.APPLICATION_SAVING_INTERVAL_MS) config.applicationSavingIntervalMs = process.env.APPLICATION_SAVING_INTERVAL_MS
  if (process.env.METRICS_DIR) config.metricsDir = process.env.METRICS_DIR
  if (process.env.SEED) config.seed = process.env.SEED

  console.log('DIS-GENERATOR => configuration loaded.')

  return config
}