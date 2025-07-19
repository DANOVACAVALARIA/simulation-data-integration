// import initSwordProto from './initSwordProto.js'

// const protoMsg = await initSwordProto()
import protobuf from 'protobufjs'

const root = await protobuf.load([
  "generator-proto/lib/proto/common.proto",
  "generator-proto/lib/proto/simulation_client.proto"
])

const SimToClient = root.lookupType("sword.SimToClient")

let contextCounter = 0

// encode protobuf message
export const swordProto = (rng) => {

  const payload = {
    context: contextCounter++,
    clientId: Math.ceil(rng() * 4),
    message: {
      unitKnowledgeUpdate: {
        knowledge: {
          id: Math.ceil(rng() * 1000)
        },
        knowledgeGroup: {
          id: Math.ceil(rng() * 2000)
        },
        pertinence: Math.floor(Date.now() / 1000),
        position: {
          latitude: -30.001142 + (rng() < 0.5 ? -0.000001 : 0.000001),
          longitude: -55.016077 + (rng() < 0.5 ? -0.000001 : 0.000001)
        },
        direction: {
          heading: 0
        },
        speed: 0,
        height_f: 0.0
      }
    }
  }

  const message = SimToClient.create(payload)
  const buffer = SimToClient.encode(message).finish()

  // prefixa com o tamanho (4 bytes, big-endian)
  const lengthBuffer = Buffer.alloc(4);
  lengthBuffer.writeUInt32BE(buffer.length, 0);

  return Buffer.concat([lengthBuffer, buffer])
}