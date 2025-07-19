import protobuf from 'protobufjs'

// let SimToClientProto = null
// let protoSword = null 

const root = await protobuf.load([
  "common.proto",
  "simulation_client.proto"
])

const SimToClient = root.lookupType('sword.SimToClient')

export const initSwordProto = async () => { 

  const payload = {
    context: 1,
    clientId: 0,
    message: {
      unitKnowledgeUpdate: {
        knowledge: {
          id: 1001
        },
        knowledgeGroup: {
          id: 2001
        },
        pertinence: Math.floor(Date.now() / 1000),
        position: {
          latitude: -15.7939,
          longitude: -47.8828
        },
        direction: {
          heading: 90
        },
        speed: 25,
        height_f: 550.0
      }
    }
  }

  const err = SimToClient.verify(payload)
  if (err) throw Error(err)

  const message = SimToClient.create(payload)
  
  return message
  // // load proto file
  // const root = await protobuf.load('generator-proto/lib/proto/simulation_client.proto')
  // if (!root) throw new Error('Failed to load simulation_client.proto')
   
  // SimToClientProto = root.lookupType('sword.SimToClient') 
  // if (!SimToClientProto) throw new Error('SimToClient type not found')
  
  // // Criar uma estrutura básica compatível com o schema
  // protoSword = SimToClientProto.create({
  //   context: 0,
  //   client_id: 0,
  //   error_msg: '',
  //   message: {
  //     // Usar unit_attributes que é um dos campos válidos do Content
  //     unit_attributes: {
  //       // Campo obrigatório: unit (do tipo Id)
  //       unit: {
  //         id: 0
  //       },
  //       // Campos opcionais mais importantes para nossa simulação
  //       position: {
  //         latitude: 0.0,
  //         longitude: 0.0
  //       },
  //       // Outros campos podem ser null/undefined pois são opcionais
  //       human_dotations: null,
  //       equipment_dotations: null,
  //       resource_dotations: null,
  //       lent_equipments: null,
  //       borrowed_equipments: null,
  //       direction: null,
  //       height: null,
  //       altitude: null,
  //       speed: null,
  //       raw_operational_state: null,
  //       reinforcements: null,
  //       reinforced_unit: null,
  //       dead: null,
  //       neutralized: null,
  //       stealth: null,
  //       underground: null,
  //       embarked: null,
  //       transporters_available: null
  //       // Muitos outros campos são opcionais e podem ser omitidos
  //     }
  //   }
  // })

  // return [SimToClientProto, protoSword]
}