import sys, os
from ClientAPI import *
from Event import *

es_client = ClientAPI(ip_address = "127.0.0.2")
projections = es_client.projections
stream_id = "chat-1"
es_client.create_stream(stream_id)
name= "chat-on-projections"
query = "fromStream('chat-1').when({'message': function(state,event) {return ['user ', event.body.user, ' says: ', event.body.message].join(''); },"\
        "'login': function(state,event) {return ['user ', event.body.user, ' login: '].join(''); },"\
        "'logout': function(state,event) {return ['user ', event.body.user, ' logout: '].join(''); }}"\
        ").emitStateUpdated();"

projections.post_continuous(query, name, enabled = "1", emit = "1")
print "Started"