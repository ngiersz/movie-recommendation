import time
import pandas as pd
import wtiproj01_client as client

if __name__  == "__main__":
   data = pd.read_csv('user_ratedmovies.dat', sep='\t', nrows=100)
   print(data.head())

   client.flush_db()

   row_iterator = data.iterrows()
   for index, row in row_iterator:
      client.add_to_queue('queue1', row.to_json(orient='index'))
      print('row ' + str(index) + ' sent')
      # time.sleep(1)


