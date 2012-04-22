var config = require('./config.json');
var _ = require('underscore');
var async = require('async');
var mysql = require('mysql');
var express = require('express');
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
		port: server.port,
		processes: []
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
			// Function ran for slave databases
			var slaveFunc = function(cb) {
				// If we have a master, don't run the slave status query
				if (server.isMaster) return cb();
				serverData[server.host].client.query('SHOW SLAVE STATUS', function(err, results, fields) {
					// We have an error, update our stats and return
					if (err || !results.length) {
						serverData[server.host].status = status.UNAVAILABLE;
						serverData[server.host].secondsBehind = 0;
						return cb();
					}
					var result = results[0];
					// Slave not running, update stats and return
					if (result.Slave_SQL_Running !== 'Yes' || result.Slave_IO_Running !== 'Yes') {
						serverData[server.host].status = status.UNAVAILABLE;
						serverData[server.host].secondsBehind = 0;
						return cb();
					}
					// Check how far behind we are, set our status then bail
					if (result.Seconds_Behind_Master) {
						serverData[server.host].status = status.BEHIND;
						serverData[server.host].secondsBehind = result.Seconds_Behind_Master;
					} else {
						serverData[server.host].status = status.AVAILABLE;
						serverData[server.host].secondsBehind = 0;
					}
					return cb();
				});
			};
			var statusFunc = function(cb) {
				serverData[server.host].client.query('SHOW FULL PROCESSLIST', function(err, results, fields) {
					if (err || !results.length) {
						serverData[server.host].processes = [];
						return cb();
					}
					serverData[server.host].processes = results;
					return cb();
				});
			};
			async.parallel(
				{
					slave: slaveFunc,
					status: statusFunc
				},
				function(err, results) {
					// Distill our data to eliminate circular references
					var retData = {};
					_.forEach(serverData, function(server) {
						retData[server.host] = {
							secondsBehind: server.secondsBehind,
							status: server.status,
							host: server.host,
							port: server.port,
							processes: server.processes
						};
					});
					// Blast this out to our clients
					_.forEach(socketClients, function(socket) {
						socket.emit('processList', retData);
					});
					return callback();
				}
			);
		},
		function(err, results) {
			if (err) console.error(err);
			setTimeout(dispatch, config.repeatInterval);
		}
	);
});

if (require.main === module) {
	exportDispatch();
	// Set up our express app
	var app = express.createServer();
	app.use(express.bodyParser());
	app.get('/slave/bestavailable', function(req, res, next) {
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
			return res.json({ host: returnServer.host, port: returnServer.port, delay: 0 });
		} else {
			if (!availableHosts.length) {
				return res.json({ error: 'No hosts found' });
			} else {
				// Sort to by number of seconds behind (least)
				availableHosts.sort(function(a, b) {
					return a.secondsBehind - b.secondsBehind;
				});
				return res.json({
					host: availableHosts[0].host,
					port: availableHosts[0].port,
					delay: availableHosts[0].secondsBehind
				});
			}
		}
	});
	app.listen(config.mainApiPort);
}
