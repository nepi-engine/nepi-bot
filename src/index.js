import express from 'express';
import bodyParser from 'body-parser';
import net from 'net';
//import JsonSocket from 'json-socket';

// HTTP Server
const app = express();
const server = require('http').Server(app);

// Web Socket Io Server
const io = require('socket.io')(server);
server.listen(81);

// Float Socket Server
const FLOATPORT = 3002;
const floatServer = net.createServer();

let floatConnected = false;

// Middleware
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.get('/', function (req, res) {
    res.sendFile(__dirname + '/server.html');
});

app.get('/socket.io.js', function (req, res) {
    res.sendFile(__dirname + '../node_modules/socket')
})

app.get('/api/status', (req, res) => {
    res.status(200).send({
      success: 'true',
      message: 'Server is running',
    })
});

// Web interface
io.on('connection', function (socket) {
    console.log('web connected')
    socket.emit('status', { status: 'connected' });
    if (floatConnected) {
        io.emit('float', {float: 'connected'})
    }
});

// Float interface
floatServer.on('connection', function(socket) {
    console.log('float connected')
    floatConnected = true;
    io.emit('float', {float: 'connected'})
    //socket = new JsonSocket(socket);

    socket.on('message', function(message) {
        console.log(message)
        io.emit('message', message)
    });
    socket.on('data', function(data) {
        let decoded = data.toString();
        let lines = decoded.split('\n');
        let packetInfo = lines[0];
        const packetNumber = parseInt(packetInfo.match(/\d+/), 10);
        let newRecord = packetNumber === 0;
        if (newRecord) {
            io.emit('newRecord', '');
        }
        lines.splice(0,1); // Remove first line with packet info
        let payload = lines.join('\n');
        console.log('data', payload);
        io.emit('message', payload)
    });
    socket.on('disconnect', function(data) {
        floatConnected = false;
        io.emit('float', {float: 'disconnected'})
    });
    socket.on('close', function(data) {
        floatConnected = false;
        io.emit('float', {float: 'disconnected'})
    });
    socket.on('error', function(error) {
        console.log('error', error)
        io.emit('float', {float: 'disconnected'})
    })
});

floatServer.listen(FLOATPORT)

app.post('/api/save-status', (req, res) => {
    res.status(201).send({
      success: 'true',
      message: 'Status saved successfully',
    })
});

app.post('/api/message', (req, res) => {
    res.status(201).send();
});

const PORT = 5000; // TODO Get from ENV variable

app.listen(PORT, () => {
    console.log(`server running on port ${PORT}`)
});
