import time
import wtiproj01_client as client


if __name__  == "__main__":
   for i in range(10 * 4):
      print(client.get_queue('queue1'))
      time.sleep(1.0 / 4)
   # while True:
   #    print(client.get_queue('queue1'))
   #    client.flush_queue('queue1')
   #    time.sleep(0.5)

