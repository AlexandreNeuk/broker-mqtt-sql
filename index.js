var mqtt = require('mqtt')
const sql = require('mssql')
var express = require('express')
const storage = require('node-sessionstorage')
var cors = require('cors')
const bodyParser = require('body-parser')

var app = express()
app.use(cors())
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(bodyParser.raw());

const PORT = 1883
const HOST = 'localhost'

const mqtt_options = {
    port: PORT,
    host: HOST,
    rejectUnauthorized : false
}

var client = mqtt.connect(mqtt_options)

const sql_config = {
  user: 'sa',
  password: '98848540',
  server: 'localhost',
  database: 'inequil',
  "options": {
    "encrypt": true,
    "enableArithAbort": true
    }
}


const getNormalizeDate = async () => {
  var date = new Date()
  var userTimezoneOffset = date.getTimezoneOffset() * 60000
  date = new Date(date.getTime() - userTimezoneOffset)   
  return date
}

app.get('/', function (req, res) {
  //
  res.send('Broker version 1.1')
})

app.post('/carregar', function (req, res) {
  //
  let publish_success = false
  //  
  try {
   //console.log('req', req.body.topics.length)
   if (req.body.topico && req.body.mensagem) {
    //
    publish_success = publishMetadata(req.body.topico, req.body.mensagem) 
    //console.group('Daat Hora: ', new Date().toISOString())
    console.log('Daat Hora: ', new Date().toISOString())
    console.log('Enviado: ', req.body.topico, ' - ', req.body.mensagem)
   }
   else {
    publish_success = 'topicomensagem'
   }
  } catch (error) {
    console.log('ERROR (carregar) ', error);
    res.send(500, 'ERROR (carregar)', error)  
  }
  //
  res.status(200).send({publish_success, 'id': req.body.id, 'indice': req.body.indice})
})

function publishMetadata(topic, metaData) {
  try {
    //console.log('topic: ', topic, ' - metaData: ', metaData)
    client.publish(topic, metaData);
  } catch (error) {
    console.log('Error Broker (publishMetadata) : ', error);
    return false
  }
  return true
}

app.listen(3000, function () {
  //
  console.log('Broker started on port 3000!');
})

client.on('connect', function(){
  //
  console.log('Mqtt Connected!')
})

client.subscribe('maq1')
client.subscribe('maq2')
client.subscribe('maq3')

client.on('message', function(topic, message, packet) {
  //
  switch (topic) {
    case 'maq1':
      saveData(packet.payload.toString('utf-8'), 'maq1')
      break;
    case 'maq2':
      saveData(packet.payload.toString('utf-8'), 'maq2')
      break;
    case 'maq3':
      saveData(packet.payload.toString('utf-8'), 'maq3')
      break;
  }
})

async function saveData(val, idMaquina) {
  //
  try {
    //
    /* 
    Se status = 0, então máquina parada
    Se status = 1, então máquina rodando
    Se status = 2, Salva dados de processo no banco
    Se status = 3, então máquina falha   
    */
    let jsonData = JSON.parse(val.replace('maq1', ''))
    //
    let pool = await sql.connect(sql_config)
    const request = pool.request()

    //console.log('etste: ', )
    var nDate = new Date().toLocaleString({ timeZone: 'America/Sao_Paulo' })
    //
    request.input('Id_Maquina', sql.Int, 1)
    request.input('DataHora', sql.DateTime, await getNormalizeDate())    
    request.input('Kilos', sql.VarChar, jsonData.MQTT_PRODUCAO_ROUPA)
    request.input('Programa', sql.VarChar, jsonData.MQTT_PROGRAMA_EXECUTADO)
    request.input('NumeroCiclo', sql.VarChar, jsonData.MQTT_NUMERO_DE_CICLOS)
    request.input('Temperatura', sql.VarChar, jsonData.MQTT_TEMPERATURA)
    request.input('Status', sql.VarChar, jsonData.MQTT_STATUS)
    request.input('Consumo', sql.VarChar, jsonData.MQTT_CONSUMO)
    request.input('TempoTrabalhando', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_TRABALHANDO)
    request.input('TempoParada', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_PARADA)
    request.input('TempoFalha', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_FALHA)
    request.input('TempoReserva1', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_RESERVA1)
    request.input('TempoReserva2', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_RESERVA2)
    request.input('TempoReserva3', sql.VarChar, jsonData.MQTT_TEMPO_MAQUINA_RESERVA3)
    //
    let strSql = 'insert into MaquinaLog (Id_Maquina, DataHora, Kilos, Programa, NumeroCiclo, Temperatura, Status, Consumo, ' + 
                 'TempoTrabalhando, TempoParada, TempoFalha, TempoReserva1, TempoReserva2, TempoReserva3) ' + 
                 'values (@Id_Maquina, @DataHora, @Kilos, @Programa, @NumeroCiclo, @Temperatura, @Status, @Consumo, ' + 
                 '@TempoTrabalhando, @TempoParada, @TempoFalha, @TempoReserva1, @TempoReserva2, @TempoReserva3);'
    //
    request.query(strSql, (err, result) => {
        //
       console.log("Registro inserido");
    })
    
  } catch (err) {
      console.log('SQL Error (saveData) : ', err)
  }
}

sql.on('error', err => {
  // ... error handler
  console.log('SQL Error (on) : ', err)
})