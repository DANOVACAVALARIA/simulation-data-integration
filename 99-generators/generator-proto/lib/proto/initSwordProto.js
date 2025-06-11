import protobuf from 'protobufjs'

let SimToClientProto = null
let protoSword = null 

export const initSwordProto = async () => { 
  
  // load proto file
  const root = await protobuf.load('generator-proto/lib/proto/simulation_client.proto')
  if (!root) throw new Error('Failed to load simulation_client.proto')
   
  
  SimToClientProto = root.lookupType('sword.SimToClient') 
  if (!SimToClientProto) throw new Error('SimToClient type not found')
  
  // instatiante message
  protoSword = SimToClientProto.create({
    context: 0,
    client_id: 0,
    error_msg: '',
    message: {
      unit_attributes: root.lookupType('sword.UnitAttributes').create({
        unit: root.lookupType('sword.Id').create({
          id: 0
        }),
        location: root.lookupType('sword.Location').create({
          type: root.lookupEnum('sword.Location.Geometry').values.point, // Geometria de ponto
          coordinates: root.lookupType('sword.CoordLatLongList').create({
            elem: [
              root.lookupType('sword.CoordLatLong').create({
                latitude: 0,
                longitude: 0
              })
            ]
          })
        }),
        objectType: root.lookupType('sword.ObjectType').create({
          id: 'unit_type_1' // Tipo genérico para unidade
        }),
        timestamp: root.lookupType('sword.DateTime').create({
          data: '20250610T171605' // Timestamp ISO-8601 padrão (10/06/2025 17:16:05)
        })
      })
    }
  }) 

  return [SimToClientProto, protoSword]
}