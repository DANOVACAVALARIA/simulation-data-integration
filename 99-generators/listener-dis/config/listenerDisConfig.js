export const listenerDisConfig = async () => {

  const config = {
    udpLocalPort: 3000,
    wsDestHost: 'dis.local',  // 'dis.local'
    wsDestPort: 80,           // 80
  }

  // environment variables
  if (process.env.UDP_LOCAL_PORT) config.udpLocalPort = process.env.UDP_LOCAL_PORT
  if (process.env.WS_DEST_HOST) config.wsDestHost = process.env.WS_DEST_HOST
  if (process.env.WS_DEST_PORT) config.wsDestPort = process.env.WS_DEST_PORT 

  console.log('DIS-LISTENER => configuration loaded.')

  return config
}