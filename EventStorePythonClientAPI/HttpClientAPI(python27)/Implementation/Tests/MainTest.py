from DeleteStreamTest import *
from CreateStreamTest import *
from AppendToStreamTest import *
from ReadAllEventsBackwardTest import *
from ReadAllEventsForwardTest import *
from ReadEventsData import *
from ReadEventTest import *
from ReadStreamEventsBackwardTest import *
from ReadStreamEventsForwardTest import *
import subprocess

if __name__ == '__main__':
    p=subprocess.Popen("D:\\apps\\EventStore\\bin\\eventstore\\debug\\anycpu\\EventStore.SingleNode.exe",shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(3)
    unittest.main()
    p.kill()