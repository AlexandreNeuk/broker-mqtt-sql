var mqtt = require('mqtt')
const sql = require('mssql')
var express = require('express')
var app = express()

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
  password: '12345678',
  server: 'localhost',
  database: 'connectoriot',
  "options": {
    "encrypt": true,
    "enableArithAbort": true
    }
}

app.get('/', function (req, res) {
  //
  res.send('Broker version 1.0')
})

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

function publishMetadata(topic, metaData) {
  client.publish(topic, metaData);
}

async function saveData (val) {
  //
  try {
    //
    let pool = await sql.connect(sql_config)
    const request = pool.request()
    request.input('Id_ColetorTopico', sql.Int, 1)
    //
    var nDate = new Date().toLocaleString({
        timeZone: 'America/Sao_Paulo'
    })
    //
    request.input('DataHora', sql.DateTime, nDate)
    request.input('Valor', sql.VarChar, val)
    //nDate.setHours(nDate.getHours()+1);
    //insert into programa (Id_Empresa, Descricao) values (17, 'Haaaa')
    //insert into ColetorTopicoLog (Id_ColetorTopico, DataHora, Valor) values ()
    //
    //console.log('Teste insert log - ', date_ob)
    request.query('insert into ColetorTopicoLog (Id_ColetorTopico, DataHora, Valor) values (@Id_ColetorTopico, @DataHora, @Valor)', (err, result) => {
        //console.log('HHHHHHHHHHHHHHA', result)
        console.log("Registro inserido");
    })

  } catch (err) {
      console.log('SQL error 1: ', err)
  } 
}

sql.on('error', err => {
  // ... error handler
  console.log('SQL error 2: ', err)
})
