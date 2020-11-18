var Service = require('node-windows').Service;

var svc = new Service({
  name:'AppWeb',
  description: 'Plataforma Inequil',
  script: 'C:\\inetpub\\wwwroot\\broker\\broker-mqtt-sql\\index.js'
})

svc.on('install',function(){
  svc.start()
})

svc.install()