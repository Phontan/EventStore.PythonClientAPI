fromStream('chat-1').when({
	'message': function (state, event) { return ['user ', event.body.user, ' says: ', event.body.message].join(''); },
	'login': function (state, event) { return ['user ', event.body.user, ' has entered the chat'].join(''); },
	'logout': function (state, event) { return ['user ', event.body.user, ' has left the chat'].join(''); }}
).emitStateUpdated();