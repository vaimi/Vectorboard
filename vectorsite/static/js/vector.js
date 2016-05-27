CONNECTION_URL = "/vectorsite/api/ui/connection/"
MESSAGES_URL = "/vectorsite/api/ui/messages/"
SELF = ""
CONNECTED = false
DEBUG = true
CURRENT = 0

TIMER = ""

var APP_JSON = 'application/json'

var startX, startY, endX, endY

$('#whiteboard').mousedown(function(e){
  startX = e.pageX - this.offsetLeft;
  startY = e.pageY - this.offsetTop;
});

$('#whiteboard').mouseup(function(e){
  endX = e.pageX - this.offsetLeft;
  endY = e.pageY - this.offsetTop;
  sendMessage('line', {'start':{'x':startX, 'y':startY},'end':{'x':endX, 'y':endY}})
});



function connect () {
  var apiurl = SELF + CONNECTION_URL
  var body = {'host': $('#address').val()}
  $.ajax({
    url: apiurl,
    type: 'GET',
    data: body
  }).done(function (data, textStatus, jqXHR) {
    if (DEBUG) {
      console.log('RECEIVED RESPONSE: data:', data, ' textStatus:', textStatus)
    }
    $('#connection-button').html("Disconnect")
      CONNECTED = true
      TIMER = setInterval(getMessages, 5000)
  }).fail(function (jqXHR, textStatus, errorThrown) {
    if (DEBUG) {
      console.log('RECEIVED ERROR: textStatus:', textStatus, 'error:', errorThrown)
    }
  })
}

function disconnect () {
  var apiurl = SELF + CONNECTION_URL
  $.ajax({
    url: apiurl,
    type: 'POST'
  }).done(function (data, textStatus, jqXHR) {
    if (DEBUG) {
      console.log('RECEIVED RESPONSE: data:', data, ' textStatus:', textStatus)
    }
    $('#connection-button').html("Connect")
      CONNECTED = false
      clearInterval(TIMER)
  }).fail(function (jqXHR, textStatus, errorThrown) {
    if (DEBUG) {
      console.log('RECEIVED ERROR: textStatus:', textStatus, 'error:', errorThrown)
    }
  })
}

function getMessages() {
    var apiurl = SELF + MESSAGES_URL
      $.ajax({
    url: apiurl,
    type: 'GET',
          data: {'from_id':CURRENT}
  }).done(function (data, textStatus, jqXHR) {
          if (DEBUG) {
              console.log('RECEIVED RESPONSE: data:', data, ' textStatus:', textStatus)
          }
          var message = ""
          for (message in data) {
              if (data[message].method == 'line') {
                  console.debug("New line")
                  var context = $('#whiteboard')[0].getContext('2d');

      context.beginPath();
      context.moveTo(data[message]['message']['start']['x'], data[message]['message']['start']['y']);
      context.lineTo(data[message]['message']['end']['x'], data[message]['message']['end']['y']);
      context.stroke();
              }
              else if (data[message].method == 'control') {
                  $('#messages').append('<p>' + data[message].message + '</p>')
              }
          }
          if (Number(message) > 0 ) CURRENT = Number(message) + 1
  }).fail(function (jqXHR, textStatus, errorThrown) {
    if (DEBUG) {
      console.log('RECEIVED ERROR: textStatus:', textStatus, 'error:', errorThrown)
    }
  })
}

function sendMessage(method, message) {
    var apiurl = SELF + MESSAGES_URL
      $.ajax({
    url: apiurl,
    type: 'POST',
          data: JSON.stringify({'method':method, 'message': message}),
          contentType: APP_JSON,
  }).done(function (data, textStatus, jqXHR) {
          if (DEBUG) {
              console.log('RECEIVED RESPONSE: data:', data, ' textStatus:', textStatus)
          }
  }).fail(function (jqXHR, textStatus, errorThrown) {
    if (DEBUG) {
      console.log('RECEIVED ERROR: textStatus:', textStatus, 'error:', errorThrown)
    }
  })
}

$(function () {
    SELF = location.origin
    $('#connection-button').on('click', function () {
        if (!CONNECTED) {
            connect()
        } else {
            disconnect()
        }
    })
})