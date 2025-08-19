import { performance } from 'perf_hooks'

import DISUtils from './dis/DISUtils.js'
import { ESPDU } from './dis/ESPDU.js'

const utils = new DISUtils()


export const encodeESPDU = async (rng) => {
  const startTime = performance.now()
  const payload = ESPDU(rng)
  const encodedPdu = utils.DISPduToBuffer(payload)
  const encodingTime = performance.now() - startTime
  const sentTime = performance.now()
  
  return {
    payload,
    encodedPdu,
    sentTime,
    encodingTime,
  }
}