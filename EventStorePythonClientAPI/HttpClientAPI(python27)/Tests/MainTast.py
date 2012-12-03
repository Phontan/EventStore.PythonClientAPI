from CreateStreamAsyncTest import *
#from DeleteStreamAsyncTest import *
from AppendToStreamAsyncTest import *
#from ReadStreamEventsBackwardAsyncTest import *

if __name__ == '__main__':
    os.startfile('D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe');
    time.sleep(3)
    unittest.main()