var Service = require('node-windows').Service;

// Create a new service object
var svc = new Service({
  name:'AppWeb',
  description: 'Plataforma Inequil',
  script: 'C:\\inetpub\\wwwroot\\broker\\broker-mqtt-sql\\index.js'
});

// Listen for the "install" event, which indicates the
// process is available as a service.
svc.on('install',function(){
  svc.start();
});

svc.install();