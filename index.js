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
  password: 'sqladmin',
  server: 'localhost',
  database: 'inequil',
  "options": {
    "encrypt": true,
    "enableArithAbort": true,
     cryptoCredentialsDetails: {
            minVersion: 'TLSv1'
        }
    }
}

app.get('/', function (req, res) {
  //
  res.send('Broker version 3.1.3')
})

app.get('/databasetest', async function (req, res) {
  //
  try {
      //
      console.log('1')
      let pool = await sql.connect(sql_config)
      console.log('2')
      const request = pool.request()
      console.log('3')
      request.query("select top 1 id from maquina", (err, result) => {
        //
        console.log('4')
        if (err) {
           res.status(200).send('Database error (1): ', err)
        }
        else {
           res.status(200).send('Database OK')
        }
      }) 
  } catch (err) {
      res.status(200).send('Database error (2): ' + err)
  }
})

app.post('/carregar', function (req, res) {
  //
  let publish_success = false
  //  
  try {
   if (req.body.topico && req.body.mensagem) {
    //
    publish_success = publishMetadata(req.body.topico, req.body.mensagem) 
    console.log('carregar - Enviado: ', req.body.topico, ' - ', req.body.mensagem)
   }
   else {
    publish_success = 'topicomensagem'
   }
  } catch (error) {
    console.log('ERROR (carregar) ', error);
    res.send(500, 'ERROR (carregar)', error)  
  }
  //
  res.status(200).send({
    publish_success, 
    'id': req.body.id, 
    'indice': req.body.indice, 
    'topico' : req.body.topico, 
    'maquina_topico' : req.body.maquina_topico,
    'receita' : req.body.receita
  })
})

app.post('/passocomando', function (req, res) {
  //
  let publish_success = false
  //  
  try {
   if (req.body.topico && req.body.mensagem) {
    //
    publish_success = publishMetadata(req.body.topico, req.body.mensagem) 
    console.log('passocomando - Data Hora: ', new Date().toISOString(), ' - ', req.body.mensagem)
   }
   else {
    publish_success = 'topicomensagem'
   }
  } catch (error) {
    console.log('ERROR (carregar) ', error);
    res.send(500, 'ERROR (carregar)', error)  
  }
  //
  res.status(200).send({publish_success, 'receita': req.body.receita})
})

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
client.subscribe('consumo')

client.on('message', async function(topic, message, packet) {
  //
  saveData(packet.payload.toString('utf-8'), await getIdMaquina(topic), topic)
})

getNormalizeDate = async () => {
  var date = new Date()
  var userTimezoneOffset = date.getTimezoneOffset() * 60000
  date = new Date(date.getTime() - userTimezoneOffset)   
  return date
}

publishMetadata = (topic, metaData) => {
  try {
    //console.log('topic: ', topic, ' - metaData: ', metaData)
    client.publish(topic, metaData);
  } catch (error) {
    console.log('Error Broker (publishMetadata) : ', error);
    return false
  }
  return true
}

getIdMaquina = async (topico) => {
  try {
      //
      let idmaquina = 0
      await sql.connect(sql_config)
      const result = await sql.query`select id from maquina where topico = ${topico}`
      objKeysMap = Object.keys(result).map((k) => result[k])
      objKeysMap.forEach(element => {
        if (element && element[0] && element[0].id) {
          idmaquina = element[0].id
        }
      })
      return idmaquina
  } catch (err) {
      // ... error checks
      console.log('Error: ', err)
  }
}

async function saveData(val, idMaquina, topico) {
  try {
    //
    /* 
    Se status = 0, máquina parada
    Se status = 1, máquina rodando
    Se status = 2, Salva dados
    Se status = 3, máquina falha   
    */
    //
    let jsonData = JSON.parse(val.replace('maq1', ''))
    //
    let strSql = ''
    let pool = await sql.connect(sql_config)
    const request = pool.request()
    //    
    if (topico === 'consumo') {
      //
      request.input('Id_Maquina', sql.Int, idMaquina)
      request.input('DataHora', sql.DateTime, await getNormalizeDate())
      request.input('Tempo', sql.VarChar, jsonData.TEMPO)
      request.input('Passo', sql.VarChar, jsonData.PASSO)
      request.input('Temperatura', sql.VarChar, jsonData.TEMPE)
      request.input('Kilos', sql.VarChar, jsonData.KG)
      request.input('ProgramaExec', sql.VarChar, jsonData.PRG_EXEC)
      request.input('ProdutoA', sql.VarChar, jsonData.PRA)
      request.input('ProdutoB', sql.VarChar, jsonData.PRB)
      request.input('ProdutoC', sql.VarChar, jsonData.PRC)
      request.input('ProdutoD', sql.VarChar, jsonData.PRD)
      request.input('ProdutoE', sql.VarChar, jsonData.PRE)
      request.input('ProdutoF', sql.VarChar, jsonData.PRF)
      request.input('ProdutoG', sql.VarChar, jsonData.PRG)
      request.input('RPM', sql.VarChar, jsonData.RPM)
      //
      strSql = 'insert into MaquinaLogReport (' + 
               'Id_Maquina, DataHora, Tempo, Passo, Temperatura, Kilos, ProgramaExec, ProdutoA, ProdutoB, ProdutoC, ProdutoD, ProdutoE, ProdutoF, ProdutoG, RPM) ' + 
               'values ' + 
               '(@Id_Maquina, @DataHora, @Tempo, @Passo, @Temperatura, @Kilos, @ProgramaExec, @ProdutoA, @ProdutoB, @ProdutoC, @ProdutoD, @ProdutoE, @ProdutoF, @ProdutoG, @RPM);'
      //
      request.query(strSql, (err, result) => {
      //
        console.log("Registro inserido - Topico: ", topico);
      })               
    }
    else if (topico === 'maq1' && await checkData(idMaquina, jsonData)) {
      //
      request.input('Id_Maquina', sql.Int, idMaquina)
      request.input('DataHora', sql.DateTime, await getNormalizeDate())    
      request.input('DataHoraCLP', sql.VarChar, jsonData.DATA)
      request.input('Consumo', sql.VarChar, jsonData.CONSUMO)
      request.input('NumeroCiclo', sql.VarChar, jsonData.NUME_CICLOS)
      request.input('TempoTrabalhando', sql.VarChar, jsonData.TEMPO_MAQ_TRABA)
      request.input('TempoParada', sql.VarChar, jsonData.TEMPO_MAQ_PARAD)
      request.input('TempoFalha', sql.VarChar, jsonData.TEMPO_MAQ_FALHA)
      request.input('Reserva1', sql.VarChar, jsonData.RESERVA1)
      request.input('Reserva2', sql.VarChar, jsonData.RESERVA2)
      //
      strSql = 'insert into maquinalog (' + 
               'Id_Maquina, DataHora, DataHoraCLP, Consumo, NumeroCiclo, ' + 
               'TempoTrabalhando, TempoParada, TempoFalha, Reserva1, Reserva2) ' + 
               'values ( ' + 
               '@Id_Maquina, @DataHora, @DataHoraCLP, @Consumo, @NumeroCiclo, ' + 
               '@TempoTrabalhando, @TempoParada, @TempoFalha, @Reserva1, @Reserva2);'
      //
      request.query(strSql, (err, result) => {
        //
        console.log("Registro inserido - Topico: ", topico);
      })
    }
    else {
      console.log('Nao insere')
    }
    //
  } catch (err) {
      console.log('SQL Error (saveData) : ', err)
  }
}

sql.on('error', err => {
  // ... error handler
  console.log('SQL Error (on) : ', err)
})


async function checkData(id, data) { 
  //
  let dataMachine = storage.getItem(id)
  //
  /*
  //var str = 'maq1{"DATA":"2020-05-14T14:59:23.945Z","PRD_ROUPA":"1"}'
  console.log('1 ')
  console.log('JSON: ', typeof(data))
  console.log('JSON: ', data)
  console.log('STORAGE: ', typeof(dataMachine))
  console.log('STORAGE: ', dataMachine)
  */ 
  //
  if (dataMachine) {
    if (data.CONSUMO == dataMachine.CONSUMO) {
      return false
    }
    else {
      console.log('LOG: ', dataMachine.CONSUMO, ' - ', data.CONSUMO)
      return true
    }
  }
  storage.setItem(id, data)
  return true
}