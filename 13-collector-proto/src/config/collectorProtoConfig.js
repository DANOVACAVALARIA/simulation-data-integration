export const collectorProtoConfig = async () => {

  const config = {
    wsLocalPort: 3002,
    protoSaveInterval: 30000,
    // pingInterval: 25000,
    // pingTimeout: 60000,
    baseDataDir: './data',
    metricsDir: './metrics'
  }

  // environment variables
  if (process.env.WS_LOCAL_PORT) config.wsLocalPort = process.env.WS_LOCAL_PORT
  if (process.env.PROTO_SAVE_INTERVAL_MS) config.protoSaveInterval = process.env.PROTO_SAVE_INTERVAL_MS
  // if (process.env.PING_INTERVAL) config.pingInterval = process.env.PING_INTERVAL
  // if (process.env.PING_TIMEOUT) config.pingTimeout = process.env.PING_TIMEOUT
  if (process.env.BASE_DATA_DIR) config.baseDataDir = process.env.BASE_DATA_DIR

  console.log('COLLECTOR-PROTO => configuration loaded.')

  return config
}