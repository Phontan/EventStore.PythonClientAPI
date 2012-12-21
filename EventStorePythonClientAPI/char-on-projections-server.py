import sys, os
sys.path.append(os.path.dirname(__file__)+"\\ClientAPI")
from ClientAPI import *
from Event import *

es_client = ClientAPI()
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