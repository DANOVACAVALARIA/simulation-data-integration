export const generatorProtoConfig = async () => {

  const config = {
    wsDestHost: 'proto.local',
    wsDestPort: 80,
    totalEspduToBeSent: 20000,
    intervalBetweenProtoMsgInMs: 1,
    terminationCheckIntervalMs: 2000,
    applicationSavingIntervalMs: 30000,
    metricsDir: '../98-metrics/generator-proto',
    seed: 123
  }

  // environment variables
  if (process.env.WS_DEST_HOST) config.wsDestHost = process.env.WS_DEST_HOST
  if (process.env.WS_DEST_PORT) config.wsDestPort = process.env.WS_DEST_PORT 

  if (process.env.TOTAL_ESPDU_TO_BE_SENT) config.totalEspduToBeSent = process.env.TOTAL_ESPDU_TO_BE_SENT
  if (process.env.INTERVAL_BETWEEN_PROTO_MSG_IN_MS) config.intervalBetweenProtoMsgInMs = process.env.INTERVAL_BETWEEN_PROTO_MSG_IN_MS
  if (process.env.TERMINATION_CHECK_INTERVAL_MS) config.terminationCheckIntervalMs = process.env.TERMINATION_CHECK_INTERVAL_MS
  if (process.env.APPLICATION_SAVING_INTERVAL_MS) config.applicationSavingIntervalMs = process.env.APPLICATION_SAVING_INTERVAL_MS
  if (process.env.METRICS_DIR) config.metricsDir = process.env.METRICS_DIR
  if (process.env.SEED) config.seed = process.env.SEED

  console.log('GENERATOR-PROTO => configuration loaded.')

  return config
}