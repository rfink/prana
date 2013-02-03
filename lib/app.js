/* ~ Application dependencies ~ */

var config = require('./config.json')
    , express = require('express')
    , app = express()
    , server = require('http').createServer(app)
    , io = require('socket.io').listen(server)
    , _ = require('underscore')
    , prana = require('./prana')();

prana.on('processList', function(data) {
  io.sockets.emit('processList', data);
});

prana.start();

// Create our socket.io server config
io.set('log level', 1);
io.sockets.on('connection', function(socket) {
  socket.emit('serverConfig', prana.servers);
});

// Set up our express app
app.use(express.bodyParser());

/**
 * Get best available slave and return host/port
 */

app.get('/slave/bestavailable', function(req, res, next) {

  var slave = prana.getBestAvailableSlave();
  if (!slave) return res.json({ error: new Error('No slaves available') });

  return res.json(slave);

});

/**
 * Kill running process (query) id on given server
 */

app.get('/kill/{server}/{id}', function(req, res, next) {

  if (!serverData[req.params.server]) {
    return next(new Error('Server ' + req.params.server + ' not found'));
  }

  var client = serverData[req.params.server].client;

  client.query('KILL ' + client.escape(req.params.id), function(err, results, field) {
    if (err) return next(err);
    return res.json({ 'success': true });
  });

});

if (require.main === module) {
  server.listen(process.env.PORT || config.port);
}
