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
  res.send('Broker version 1.0')
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
  res.status(200).send({publish_success, 'id': req.body.id})
})

function publishMetadata(topic, metaData) {
  try {
    //console.log('topic: ', topic, ' - metaData: ', metaData)
    client.publish(topic, metaData);

    //client.publish("test", metaData);
    //client.publish("test2", metaData);

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
  //console.log('messsage: ', topic)
  //saveData(packet.payload.toString('utf-8'))
})

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
      console.log('SQL Error (saveData) : ', err)
  }
}

sql.on('error', err => {
  // ... error handler
  console.log('SQL Error (on) : ', err)
})