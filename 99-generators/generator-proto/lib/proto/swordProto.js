
import { initSwordProto } from './initSwordProto.js'


// instantiate message (protobuf)
let [SimToClientProto, simToClientProto] = await initSwordProto()
let contextCounter = 1 // Contador para rastrear contexto das mensagens


// encode protobuf message
export const swordProto = (rng) => {
  const id = Math.ceil(rng() * 4)
  simToClientProto.message.unit_attributes.unit.id = id
  
  simToClientProto.message.unit_attributes.location.coordinates.elem[0].latitude = -30.001142 + (rng() < 0.5 ? -0.000001 : 0.000001)
  simToClientProto.message.unit_attributes.location.coordinates.elem[0].longitude = -55.016077 + (rng() < 0.5 ? -0.000001 : 0.000001)
  
  simToClientProto.message.unit_attributes.id = `unit_type_${id}`
  // Define o timestamp ISO-8601
  simToClientProto.message.unit_attributes.timestamp.data = new Date().toISOString()
  
  // Incrementa o contexto para rastrear a mensagem
  simToClientProto.context = contextCounter++

  // Codifica a mensagem
  const encodedProtoMsg = SimToClientProto.encode(simToClientProto).finish()

  return encodedProtoMsg
}