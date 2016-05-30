// Mouse movement
var startX, startY, endX, endY
$('#whiteboard').mousedown(function(e){
  startX = e.pageX - this.offsetLeft;
  startY = e.pageY - this.offsetTop;
});
$('#whiteboard').mouseup(function(e){
  endX = e.pageX - this.offsetLeft;
  endY = e.pageY - this.offsetTop;
  newMessage('line', {'start':{'x':startX, 'y':startY},'end':{'x':endX, 'y':endY}})
});

// Send message through socket
function newMessage(method, message) {
    msg = {"method": method, "message": message}
    messenger.socket.send(JSON.stringify(msg))
}

// Socket initialization
var messenger = {
    socket: null,

    start: function() {
        var url = ""
        if (location.protocol == 'https:') {
          url = "wss://" + location.host + "/vectorsocket";
        } else {
          url = "ws://" + location.host + "/vectorsocket";
          
        }
        messenger.socket = new WebSocket(url)
        messenger.socket.onmessage = function(event) {
            messenger.handleMessage(JSON.parse(event.data));
        }
    },

    // handle incoming messages
    handleMessage: function(message) {
        console.debug(message)
        switch (message.method) {
            case "connected":
                $("#messages").append("<p>Node connected " + message.message)
                $("#connection-button").html("Disconnect")
                break
            case "disconnected":
                $("#messages").append("<p>Node disconnected " + message.message)
                $("#connection-button").html("Connect")
                break
            case "line":
                start = {'x': message.message.start.x, 'y': message.message.start.y}
                end = {'x': message.message.end.x, 'y': message.message.end.y}
                newLine(start, end)
                break
            case "clean":
                clearScreen(message.message.x, message.message.y)
                break
        }
    }
}

// Clear drawing
function clearScreen(width, height) {
    console.debug("Cleaning screen")
    var context = $('#whiteboard')[0].getContext('2d');
    var canvas = $('#whiteboard')
    context.clearRect(0, 0, canvas.width(), canvas.height());
    canvas.width = width
    canvas.height = height
}

// handle connect button press
function connect () {
  address = $('#address').val()
  method = "connect"
  newMessage(method, address)
}

// handle disconnect button press
function disconnect () {
  method = "disconnect";
  newMessage(method)
}

// handle clean button press
function clean() {
  method = "clean"
    message = {'x': $('#whiteboard').width(), 'y': $('#whiteboard').height()}
    newMessage(method, message)
}

// draw new line
function newLine(start, end) {
    var context = $('#whiteboard')[0].getContext('2d');
      context.beginPath();
      context.moveTo(start.x, start.y);
      context.lineTo(end.x, end.y);
      context.stroke();
}


$(function () {
    messenger.start();
    $('#connection-button').on('click', function () {
        if ($('#connection-button').html() === "Connect") {
            connect()
        } else {
            disconnect()
        }
    })
    $('#clear-button').on('click', function () {
        clean()
    })
})