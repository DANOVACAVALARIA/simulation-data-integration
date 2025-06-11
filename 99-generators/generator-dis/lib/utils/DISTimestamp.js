const MAX_DIS_TIMESTAMP = 2147483647 
const MILLISECONDS_PER_HOUR = 3600000

export const timestampToDis = () => {
  const currentMilliseconds = Date.now() % MILLISECONDS_PER_HOUR

  return Math.floor((currentMilliseconds / MILLISECONDS_PER_HOUR) * MAX_DIS_TIMESTAMP)  
}

export const disToTimestamp = (disTimestamp) => {
  return Math.floor((disTimestamp / MAX_DIS_TIMESTAMP) * MILLISECONDS_PER_HOUR)
}