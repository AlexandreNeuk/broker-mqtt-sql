var mqtt = require('mqtt')
const sql = require('mssql')
var express = require('express')
const bodyParser = require('body-parser')

var app = express()
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
    
    console.log('req.body ', req.body);
    //
    /*
    let message = { 
      "MAQUINA":"20", 
      "RECEITA_ID": "17",
      "PASSOS_LAVAGEM": {
        "PASSO1" : {
          "Tipo" : "Lavagem",
          "TipoCode" : "01",          
          "ModoTrabalho" : "1",
          "TempoOperacao" : "2",
          "TempoReversao" : "3",
          "RPM" : "500",
          "Temperatura" : "37",
          "SemVapor" : "True",
          "Entrada" : "AA",
          "Nivel" : "75",
          "Saida" : "BB",
          "ProdutoA" : "ProdutoA",
          "ValorA" : "ValorA",
          "ProdutoB" : "ProdutoB",
          "ValorB" : "ValorB",
          "ProdutoC" : "ProdutoC",
          "ValorC" : "ValorC",
          "ProdutoD" : "ProdutoD",
          "ValorD" : "ValorD",
          "ProdutoE" : "ProdutoE",
          "ValorE" : "ValorE",
          "ProdutoF" : "ProdutoF",
          "ValorF" : "ValorF",
          "ProdutoG" : "ProdutoG",
          "ValorG" : "ValorG"
        },
        "PASSO2" : {
          "Tipo" : "Lavagem",
          "TipoCode" : "01",          
          "ModoTrabalho" : "3",
          "TempoOperacao" : "4",
          "TempoReversao" : "6",
          "RPM" : "700",
          "Temperatura" : "40",
          "SemVapor" : "False",
          "Entrada" : "AA",
          "Nivel" : "75",
          "Saida" : "BB",
          "ProdutoA" : "ProdutoA",
          "ValorA" : "ValorA",
          "ProdutoB" : "ProdutoB",
          "ValorB" : "ValorB",
          "ProdutoC" : "ProdutoC",
          "ValorC" : "ValorC",
          "ProdutoD" : "ProdutoD",
          "ValorD" : "ValorD",
          "ProdutoE" : "ProdutoE",
          "ValorE" : "ValorE",
          "ProdutoF" : "ProdutoF",
          "ValorF" : "ValorF",
          "ProdutoG" : "ProdutoG",
          "ValorG" : "ValorG"
        },
        "PASSO3" : {
          "Tipo" : "Lavagem",
          "TipoCode" : "01",
          "ModoTrabalho" : "12",
          "TempoOperacao" : "6",
          "TempoReversao" : "2",
          "RPM" : "800",
          "Temperatura" : "40",
          "SemVapor" : "False",
          "Entrada" : "AA",
          "Nivel" : "75",
          "Saida" : "BB",
          "ProdutoA" : "ProdutoA",
          "ValorA" : "ValorA",
          "ProdutoB" : "ProdutoB",
          "ValorB" : "ValorB",
          "ProdutoC" : "ProdutoC",
          "ValorC" : "ValorC",
          "ProdutoD" : "ProdutoD",
          "ValorD" : "ValorD",
          "ProdutoE" : "ProdutoE",
          "ValorE" : "ValorE",
          "ProdutoF" : "ProdutoF",
          "ValorF" : "ValorF",
          "ProdutoG" : "ProdutoG",
          "ValorG" : "ValorG"
        },
        "PASSO4" : {
          "Tipo" : "Centrifugacao",
          "TipoCode" : "02",          
          "ModoTrabalho" : "5",
          "Saida" : "AA",
          "Velocidade1" : "500",
          "Tempo1" : "20",
          "Velocidade2" : "600",
          "Tempo2" : "40",
          "Velocidade3" : "400",
          "Tempo3" : "60",
          "Velocidade4" : "300",
          "Tempo4" : "10",
          "Velocidade5" : "200",
          "Tempo5" : "70",
        },
        "PASSO5" : {
          "Tipo" : "Centrifugacao",
          "TipoCode" : "02",          
          "ModoTrabalho" : "15",
          "Saida" : "BB",
          "Velocidade1" : "44",
          "Tempo1" : "44",
          "Velocidade2" : "868",
          "Tempo2" : "67",
          "Velocidade3" : "424",
          "Tempo3" : "60",
          "Velocidade4" : "300",
          "Tempo4" : "12",
          "Velocidade5" : "150",
          "Tempo5" : "70",
        },
        "PASSO6" : {
          "Tipo" : "Centrifugacao",
          "TipoCode" : "02",          
          "ModoTrabalho" : "518",
          "Saida" : "CC",
          "Velocidade1" : "55",
          "Tempo1" : "20",
          "Velocidade2" : "77",
          "Tempo2" : "40",
          "Velocidade3" : "33",
          "Tempo3" : "60",
          "Velocidade4" : "300",
          "Tempo4" : "10",
          "Velocidade5" : "200",
          "Tempo5" : "70",
        }   
      }
    }
    */
   //
   if (req.body.topic && req.body.recipes) {
    let recipes = JSON.stringify(req.body.recipes);
    console.log("topic: ", req.body.topic)
    publish_success = publishMetadata(req.body.topic, recipes) 
   }
   else {
    res.status(500).send("Invalid Parameters")
   }
  } catch (error) {
    console.log('ERROR (carregar) ', error);
    res.send(500, 'ERROR (carregar)', error)  
  }
  //
  if (publish_success) {
    res.status(200).send("OK")
  }
  else {
    res.status(200).send("NOK")
  }  
})

function publishMetadata(topic, metaData) {
  try {
    console.log('topic: ', topic, ' - metaData: ', metaData)
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
