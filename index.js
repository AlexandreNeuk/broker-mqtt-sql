var mqtt = require('mqtt')
const sql = require('mssql')
var express = require('express')
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

client.subscribe('test')

client.on('message', function(topic, message, packet) {
  //
  saveData(packet.payload.toString('utf-8'))
})

async function saveData (val) {
  //
  try {
    //
    let jsonData = JSON.parse(val)    
    let pool = await sql.connect(sql_config)
    const request = pool.request()
    //
    var nDate = new Date().toLocaleString({
        timeZone: 'America/Sao_Paulo'
    })
    //
    request.input('Id_Maquina', sql.Int, jsonData.idmaquna)
    request.input('DataHora', sql.DateTime, nDate)    
    request.input('Kilos', sql.VarChar, jsonData.kilos)
    request.input('Programa', sql.VarChar, jsonData.programa)
    request.input('NumeroCiclo', sql.VarChar, jsonData.numerociclo)
    request.input('Temperatura', sql.VarChar, jsonData.temperatura)
    request.input('Status', sql.VarChar, jsonData.status)
    request.input('Consumo', sql.VarChar, jsonData.consumo)
    //
    let strSql = 'insert into MaquinaLog (Id_Maquina, DataHora, Kilos, Programa, NumeroCiclo, Temperatura, Status, Cosumo) ' + 
                'values (@Id_Maquina, @DataHora, @Kilos, @Programa, @NumeroCiclo, @Temperatura, @Status, @Cosumo)'

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