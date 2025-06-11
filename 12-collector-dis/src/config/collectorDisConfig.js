export const collectorDisConfig = async () => {

  const config = {
    wsLocalPort: 3001,
    pduSaveInterval: 30000,
    // pingInterval: 25000,
    // pingTimeout: 60000,
    baseDataDir: './data',
    metricsDir: './metrics'
  }

  // environment variables
  if (process.env.WS_LOCAL_PORT) config.wsLocalPort = process.env.WS_LOCAL_PORT
  if (process.env.PDU_SAVE_INTERVAL_MS) config.pduSaveInterval = process.env.PDU_SAVE_INTERVAL_MS
  // if (process.env.PING_INTERVAL) config.pingInterval = process.env.PING_INTERVAL
  // if (process.env.PING_TIMEOUT) config.pingTimeout = process.env.PING_TIMEOUT
  if (process.env.BASE_DATA_DIR) config.baseDataDir = process.env.BASE_DATA_DIR

  console.log('COLLECTOR-DIS => configuration loaded.')

  return config
}