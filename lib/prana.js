/* ~ Application dependencies ~ */

var debug = require('debug')('prana')
		, config = require('./config.json')
		, _ = require('underscore')
		, async = require('async')
		, mysql = require('mysql')
		, EventEmitter = require('events').EventEmitter
		, errorHandler = require('./errorhandler');

// Status constants
var status = {
	AVAILABLE: 1,
	BEHIND: 2,
	UNAVAILABLE: 3
};

exports = module.exports = Prana;

/**
 * Prana constructor
 */

function Prana() {

	if (!(this instanceof Prana)) return new Prana();

	var self = this;
	this.servers = [];

	// Prime our data collection
	config.servers.forEach(function(server) {
		self.servers.push({
			client: null,
			secondsBehind: 0,
			status: null,
			host: server.host,
			port: server.port,
			username: server.username,
			password: server.password,
			processes: [],
			id: server.id || (server.host + ':' + server.port)
		});
	});

	debug('%d servers configured', config.servers.length);

}

Prana.prototype.__proto__ = EventEmitter.prototype;

/**
 * Start our prana service
 */

Prana.prototype.start = function() {

	debug('Started at %s', new Date());

	var self = this;

	// Create our initial database connections
	this.createConnections();

	function checkStatus(server) {

		// Since the create error would be async, do another check if the client
		//   is created correctly
		if (!server.client) return;

		/**
		 * Function ran for slave databases
		 */
	
		function getSlaveStatus(cb) {

			// If we have a master, don't run the slave status query
			if (server.isMaster) return cb();

			server.client.query('SHOW SLAVE STATUS', function(err, results, fields) {

				// We have an error, update our stats and return
				if (err || !results.length) {
					server.status = status.UNAVAILABLE;
					server.secondsBehind = 0;
					return cb();
				}

				var result = results[0];

				// Slave not running, update stats and return
				if (result.Slave_SQL_Running !== 'Yes' || result.Slave_IO_Running !== 'Yes') {
					server.status = status.UNAVAILABLE;
					server.secondsBehind = 0;
					return cb();
				}

				// Check how far behind we are, set our status then bail
				if (result.Seconds_Behind_Master) {
					server.status = status.BEHIND;
					server.secondsBehind = result.Seconds_Behind_Master;
				} else {
					server.status = status.AVAILABLE;
					server.secondsBehind = 0;
				}

				return cb();

			});

		}

		/**
		 * Get our process list for the server
		 */

		function getProcessList(cb) {

			server.client.query('SHOW FULL PROCESSLIST', function(err, results, fields) {
				if (err || !results.length) {
					server.processes = [];
					return cb();
				}
				results.forEach(function(result, index) {
					if (result.Info === 'SHOW FULL PROCESSLIST') results.splice(index, 1);
				});
				server.processes = results;
				return cb();
			});

		}

		/**
		 * Process results
		 */

		function processResults(err, results) {

			// Distill our data to eliminate circular references
			var retData = {
				secondsBehind: server.secondsBehind,
				status: server.status,
				host: server.host,
				port: server.port,
				processes: server.processes
			};

			self.emit('processList', retData);

		}

		// Start getting our data
		async.parallel({ slave: getSlaveStatus, processList: getProcessList }, processResults);

	}

	this.servers.forEach(checkStatus);

	// Make sure to not accidentally spawn more than one of these
	if (this.timer) clearTimeout(this.timer);

	// Schedule our next ticking
	this.timer = setTimeout(this.start.bind(this), config.repeat || process.env.REPEAT);

	return this;

};

/**
 * Stop our prana service
 */

Prana.prototype.stop = function() {

	if (this.timer) clearTimeout(this.timer);
	this.timer = null;

	return this;

};

/**
 * Create initial database connections
 */

Prana.prototype.createConnections = function() {

	this.servers.forEach(function(server) {

		if (!server.client) {
			var opts = {
				host: server.host,
				port: server.port,
				user: server.username,
				password: server.password
			};
			server.client = mysql.createClient(opts, function(err) {
				server.status = status.UNAVAILABLE;
				server.secondsBehind = 0;
				errorHandler('warn', err);
			});
		}

	});

	return this;

};

/**
 * Get best available slave, defined as status: AVAILABLE and least behind
 */

Prana.prototype.getBestAvailableSlave = function() {

  var availableHosts = [];

  // Traverse the host collection and try to find an available server
  var returnServer = _.find(this.servers, function(server, host) {
    // No need to mind with these
    if (server.isMaster) return false;
    if ([status.AVAILABLE, status.BEHIND].indexOf(server.status) !== -1) {
      if (!server.secondsBehind) {
        return server;
      }
      availableHosts.push(server);
    }
  });

  if (returnServer) return { host: returnServer.host, port: returnServer.port, delay: 0 };
  if (!availableHosts.length) return null;

  // Sort to by number of seconds behind (least)
  availableHosts.sort(function(a, b) {
    return a.secondsBehind - b.secondsBehind;
  });

  var slave = availableHosts.shift();

  return {
    host: slave.host,
    port: slave.port,
    delay: slave.secondsBehind
  };

};
