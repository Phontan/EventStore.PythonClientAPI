import EventStoreClient;
from  Event import Event;
import getopt;
import http.client
import sys;

def printHelp():
    print(functionDict.keys());

client = EventStoreClient.EventStoreClient();
args = sys.argv[1:]
#print(args)
functionDict = {"--help": printHelp, "--createStream":client.CreateStream};

optlist, args = getopt.getopt(args, '', ['help', 'createStream', 'streamId=', 'metadata=']);
if len(optlist):
    firstArg = optlist[0][0];
    if firstArg == '--help':
         functionDict[firstArg]();
    elif optlist[0] == "--createStream":
         functionDict[firstArg](optlist[1][1], optlist[2][1])