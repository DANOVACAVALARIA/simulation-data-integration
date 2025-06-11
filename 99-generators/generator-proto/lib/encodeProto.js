import { performance } from 'perf_hooks'

import { swordProto } from './proto/swordProto.js'

export const encodeProto = async (rng) => {
  const startTime = performance.now()
  const encodedProto = swordProto(rng)
  const encodingTime = performance.now() - startTime
  const sentTime = performance.now()
  
  return {
    encodedProto,
    sentTime,
    encodingTime,
  }
}