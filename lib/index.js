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
		status: null,
		host: server.host,
		port: server.port
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
					serverData[server.host].secondsBehind = 0;
					// TODO: Error handling
					console.error(arguments);
					return callback();
				});
			}
			// Since the create error would be async, do another check if the client
			//   is created correctly
			if (!serverData[server.host].client) return callback();
			// If we have a master, don't run the slave status query
			if (server.isMaster) return callback();
			serverData[server.host].client.query('SHOW SLAVE STATUS', function(err, results, fields) {
				// We have an error, update our stats and return
				if (err) {
					serverData[server.host].status = status.UNAVAILABLE;
					serverData[server.host].secondsBehind = 0;
					return callback();
				}
				// Slave not running, update stats and return
				if (results.Slave_SQL_Running !== 'Yes' || results.Slave_IO_Running !== 'Yes') {
					serverData[server.host].status = status.UNAVAILABLE;
					serverData[server.host].secondsBehind = 0;
					return callback();
				}
				// Check how far behind we are, set our status then bail
				if (results.Seconds_Behind_Master) {
					serverData[server.host].status = status.BEHIND;
					serverData[server.host].secondsBehind = results.Seconds_Behind_Master;
				} else {
					serverData[server.host].status = status.AVAILABLE;
					serverData[server.host].secondsBehind = 0;
				}
				return callback();
			});

		},
		function(err, results) {
			if (err) console.error(err);
			setTimeout(dispatch, config.repeatInterval);
		}
	);
});

serverData = {
	'localhost-1': {
		isMaster: false,
		secondsBehind: 4,
		status: status.BEHIND,
		host: 'localhost-1',
		port: '3306'
	},
	'localhost-2': {
		isMaster: false,
		secondsBehind: 2,
		status: status.BEHIND,
		host: 'localhost-2',
		port: '3306'
	},
	'localhost-3': {
		isMaster: true,
		secondsBehind: 0,
		status: status.AVAILABLE,
		host: 'localhost-3',
		port: '3306'
	},
	'localhost-4': {
		isMaster: false,
		secondsBehind: 0,
		status: status.AVAILABLE,
		host: 'localhost-4',
		port: '3306'
	}
};

if (require.main === module) {
	//exportDispatch();
	http.createServer(function(request, response) {
		var data = '';
		request.on('data', function(chunk) {
			data += chunk;
		});
		request.on('end', function() {
			data = JSON.parse(data);
			if (data.requestType === 'getSlave') {
				// Best available means the least behind slave that is available
				if (data.strategy === 'bestAvailable') {
					var availableHosts = [];
					// Traverse the host collection and try to find an available server
					var returnServer = _.find(serverData, function(server, host) {
						// No need to mind with these
						if (server.isMaster) return false;
						if ([status.AVAILABLE, status.BEHIND].indexOf(server.status) !== -1) {
							if (!server.secondsBehind) {
								return server;
							}
							availableHosts.push(server);
						}
					});
					if (returnServer) {
						response.write(JSON.stringify({ host: returnServer.host, port: returnServer.port, delay: 0 }));
						return response.end();
					} else {
						if (!availableHosts.length) {
							response.write(JSON.stringify({ error: 'No hosts found' }));
							return response.end();
						} else {
							// Sort to by number of seconds behind (least)
							availableHosts.sort(function(a, b) {
								return a.secondsBehind - b.secondsBehind;
							});
							console.log(availableHosts[0]);
							response.write(JSON.stringify({
								host: availableHosts[0].host,
								port: availableHosts[0].port,
								delay: availableHosts[0].secondsBehind
							}));
							return response.end();
						}
					}
				}
			} else {
				return response.end();
			}
		});
	}).listen(config.mainApiPort);
}
