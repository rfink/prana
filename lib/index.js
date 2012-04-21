var http = require('http');
var config = require('./config.json');
var _ = require('underscore');
var async = require('async');
var mysql = require('mysql');
var io = require('socket.io').listen(config.ioSocketPort);
var exportDispatch;
var serverData = {};
var socketClients = {};

// Create our socket.io server config
io.set('log level', 1);
io.sockets.on('connection', function(socket) {
	socketClients[socket.id] = socket;
	socket.on('disconnect', function() {
		delete socketClients[socket.id];
	});
});

// Status constants
var status = {
	AVAILABLE: 1,
	BEHIND: 2,
	UNAVAILABLE: 3
};

// Prime our data collection
config.servers.forEach(function(server) {
	serverData[server.host] = {
		client: null,
		secondsBehind: 0,
		status: null
	};
});

// Make this an export, so we can run it via an OS service, if we want
exports = module.exports = exportDispatch = (function dispatch() {
	async.map(
		config.servers,
		function(server, callback) {
			if (!serverData[server.host].client) {
				var opts = {
					host: server.host,
					port: server.port,
					user: server.username,
					password: server.password
				};
				serverData[server.host].client = mysql.createClient(opts, function(err) {
					serverData[server.host].status = status.UNAVAILABLE;
					// TODO: Error handling
					console.error(arguments);
					return callback();
				});
			}
			serverData[server.host].client.query('SHOW SLAVE STATUS', function(err, results, fields) {
				if (err) {
					serverData[server.host].status = status.UNAVAILABLE;
					return callback();
				}
				if (results.Seconds_Behind_Master) {
					serverData[server.host].status = status.BEHIND;
				} else {
					serverData[server.host].status = status.AVAILABLE;
				}
			});

		},
		function(err, results) {
			if (err) console.error(err);
			setTimeout(dispatch, config.repeatInterval);
		}
	);
});

if (require.main === module) {
	exportDispatch();
	http.createServer(function(request, response) {
		var data = '';
		request.on('data', function(chunk) {
			data += chunk;
		});
		request.on('end', function() {
			data = JSON.parse(data);

			// response.end();
		});
	}).listen(config.mainApiPort);
}
