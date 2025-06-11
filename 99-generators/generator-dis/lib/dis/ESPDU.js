import dis from 'open-dis'
import { timestampToDis } from '../utils/DISTimestamp.js' 

// instantiate message
const entityStatePDU = new dis.EntityStatePdu()


export const ESPDU = (rng) => {

  entityStatePDU.pduLength = 160

  entityStatePDU.entityID.site = 2
  entityStatePDU.entityID.application = 1
  entityStatePDU.entityID.entity = Math.ceil(rng() * 4)
  
  entityStatePDU.forceId = 1

  entityStatePDU.entityType.entityKind = 1
  entityStatePDU.entityType.domain = 1
  entityStatePDU.entityType.country = 29
  entityStatePDU.entityType.category = 1

  entityStatePDU.alternativeEntityType.entityKind = 1
  entityStatePDU.alternativeEntityType.domain = 1
  entityStatePDU.alternativeEntityType.country = 29
  entityStatePDU.alternativeEntityType.category = 1

  entityStatePDU.timestamp = timestampToDis()

  entityStatePDU.entityLocation.x = (-30001142 + (rng() < 0.5 ? -1 : 1))
  entityStatePDU.entityLocation.y = (-55016077 + (rng() < 0.5 ? -1 : 1))

  entityStatePDU.entityLocation.z = 0
  

  return entityStatePDU
}