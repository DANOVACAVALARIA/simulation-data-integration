import fs from 'fs'

export const saveMetadataFile = (session) => {
  session.sessionEnd = new Date().toISOString()
  const duration = ((new Date(session.sessionEnd) - new Date(session.sessionStart)) / 60000).toFixed(2)
  session.DurationInMinutes = duration
  const totalKB = Math.round(session.totalKB / 10000)
  session.totalKB = totalKB
  fs.writeFileSync(session.dir + "/" + session.metadataFile, JSON.stringify(session, null, 2))
  console.log(`COLLECTOR-PROTO => files saved in ${session.dir}.`)
}