var cluster = require('cluster');
var util = require('util');
var repl = require("repl");
var net = require("net");
var vm = require("vm");
var os = require('os');
var numCPUs = os.cpus().length;

var config = {
	'port' : 801,
	'repl' : 5050
}

var servers = {
	'/test' : {
		'desc' : 'test server',
		'--key' : '', 	// hmac private key
		'limit' : 1024,	// content size limit
		'--rabbitmq' : {
			'host' : '127.0.0.1',
			'port' : 5672,
			'login' : 'guest',
			'password': 'guest',
			'vhost': '/'
		},
		// TODO: add to module
		'handler' : {
			'time' : function() { return Date.now(); }
		}
	}
}

if(cluster.isMaster) {
	var workers = [];
	// supervisor and spawner
	for(var i = 0; i < numCPUs; ++i){
		workers[i] = cluster.fork();
	}

	net.createServer(function(socket){
		repl.start('> ', socket, function(code, context, file, cb) {
			var err, result;
			try {
				result = eval(code);//vm.runInThisContext(code);
				for(var e in workers){
					workers[e].send({cmd: code});
				}
			} catch (e) {
				err = e;
			}
			cb(err, result);
		});
	}).listen(config.repl);

	cluster.on('death', function(worker){
			// TODO: respawn
			console.log('worker ' + worker.pid + ' died');
			});
	cluster.on('message', function(result){
			console.log("[" + i + '] :' + util.inspect(result));
			});
	return;
}

// TODO: create connections to each server
var amqp = require ('amqp');
//var conn = amqp.createConnection(opt);

console.log('worker ' + process.pid);
/*
conn.on('ready', function() {
		});

conn.on('error', function() {
		console.log('conn ' + conn.destroyed);
		});
console.log(util.inspect(conn))
*/
var http = require('http');
var crypto = require('crypto');
// TODO: respond proto extend
// checkHeader

http.ServerResponse.prototype.serverError = function(code) {
	// TODO: log?
	this.end(JSON.stringify({ 'error' : code }))
}

Buffer.prototype.append = function(data) {
	if(this.written == null)
		this.written = 0;
	else if(this.length < this.written + data.length){
		//TODO better log
		console.log('buffer overflow');
		return -1;
	}
	data.copy(this, this.written);
	return this.written += data.length;
}

http.createServer(function(request,respond){
	var handler = servers[request.url];
	if(handler == null){
		return respond.serverError('invalid server');
	}

	var headers = request.headers;
	var hmac = null
	// plain text
	if(handler.key != null){
		hmac = crypto.createHmac('sha1', handler.key);
		hmac.update(headers['timestamp']);
		hmac.update(headers['method']);
	}

	var size = parseInt(headers['content-length']);
	var limit = handler.limit;
	var buffer = new Buffer(size<limit ? size : limit);
	
	request.on('data', function(chunk){
		if(hmac != null)
			hmac.update(chunk);
		if(buffer.append(chunk) < 0){
			respond.serverError('data overflow');
		}
		// TODO: check over write
	});

	request.on('end', function(){
		if(hmac != null && hmac.digest('base64') !== headers['signature'])
			return respond.serverError('invalid signature');

		if(handler.rabbitmq != null){
			// queue the message and wait for respond
		}else{
			// process locally
			var json = {};
			try{
				json = JSON.parse(buffer.toString('utf8'));
			}catch(e){
				return respond.serverError('invalid method');
			}

			// TODO better design: exception handler, fault tollerance and security
			var result = null;
			try{
				result = eval('handler.handler.' + json.cmd)(json.args);
				respond.end(JSON.stringify({'result': result}));
			}catch(e){
				return respond.serverError('method failed');
			}
		}
	});
}).listen(config.port);

process.on("message", function(msg){
	if(msg.cmd == null) return;

	try{
		eval(msg.cmd);//vm.runInThisContext(msg.cmd);
	}catch(e){
		console.log('[' + process.pid + '] error:' + e);
	}
});
