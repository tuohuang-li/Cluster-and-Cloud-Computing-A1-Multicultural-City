from mpi4py import MPI
import math
import re
import json
from lang import languages
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()
data = []
def read_line(line):
  try:
    if (line.decode("utf-8").strip()[-1] == ','):
      row = json.loads(line.decode('utf-8').strip()[:-1])
      if (row["doc"]["coordinates"] is not None):
        #print(row["doc"]["coordinates"])
        data.append({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"]})
    else:
      row = json.loads(line.decode('utf-8').strip()[:-2])  #last row contains special charas cannot be parsed by JSON??
      if (row["doc"]["coordinates"] is not None):
        #print(row)
        data.append({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"]})
  except:
    pass

def loadData(x):
  total_row = 0
  with open("bigTwitter.json", 'rb') as f:
    i = 0
    for item in f:
      if(i != 0):
        number = i % x
        #print('x is :',x ,'i is: ', i, 'number is: ', number)
        if(number == 0 and comm_rank == x - 1):
          #print('i am ---- ', comm_rank, 'I am reading line: ', i)
          read_line(item)
        elif(number == comm_rank + 1):
          read_line(item)
          #print('i am +++', comm_rank, 'i am reading line: ', i)
      i += 1
  return i

loadData(comm_size)

#for item in final:
#  all_data.append(item)

def loadFileGrid(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

def getGeoCodeSets(grid):
  logi = []
  lati = []
  for items in grid["features"]:
    for coordinates in items["geometry"]["coordinates"][0]:
      logi.append(coordinates[0])
      lati.append(coordinates[1])
  logi_set = sorted(set(logi))
  lati_set = sorted(set(lati), reverse=True)
  return logi_set, lati_set

sydGrid = loadFileGrid('sydGrid.json')
logi, lati = getGeoCodeSets(sydGrid)
logiCode=['1', '2', '3', '4']
latiCode = ['A', 'B', 'C', 'D']



def getAreaCode(x, y, logi, lati):
  area = {"logiCode": '', "latiCode": ''}

  if(x > logi[-1] or x < logi[0] or y < lati[-1] or y > lati[0]):
    return None
  grid_length = len(logi)
  for i in range(grid_length - 1):
    if(x >= logi[i] and x <= logi[i + 1]):
      area["logiCode"] = logiCode[i]
      break
  if( y == lati[-1]):
    area["latiCode"] = latiCode[-1]
  for i in range(grid_length - 1):
    if(y <= lati[i] and y > lati[i + 1]):
      area["latiCode"] = latiCode[i]
      break
 # print(area['latiCode'] + area['logiCode'])
  return area['latiCode'] + area['logiCode']


def get_languages(lan):
  for i in languages:
    if(i[0] == lan):
      return i[1]

  return 'Unknown'

def generate_results_array():
  results = []
  for i in latiCode:
    for j in logiCode:
      results.append({"cell":i + j, "total_tweets" : 0, "number_of_languages": 0 , "languages": {}})
  return results

def getResult(twitter_file):
  results = generate_results_array()
  for row in twitter_file: #f_list:
    coordinates = row["coordinates"]
    area_code = getAreaCode(coordinates[0], coordinates[1], logi, lati)  #find twitter location
    for result in results:
      if(result["cell"] == area_code):
        language = get_languages(row["lang"])
        result["total_tweets"] += 1
        if (language in result["languages"]):
          result["languages"][language] += 1
        else:
          result["languages"][language] = 1
        result[ "number_of_languages"] = len(result["languages"])
  final_result = []
  for result in results:
    if(result["total_tweets"] != 0):
      newDic = dict(sorted(result["languages"].items(), key=lambda item: item[1], reverse=True)[:10])
      result["languages"] = newDic
      final_result.append(result)

  return final_result

def print_result(results):
  print('cell', '#Total Tweets', '#Number of Languages Used')
  for result in results:
    print(result['cell'],'\t', result['total_tweets'], '\t\t', result['number_of_languages'] ,'\t', result['languages'])


all_data = []
gathered = comm.gather(data, root = 0)
if(gathered is not None):
  for item in gathered:
    for da in item:
      all_data.append(da)
  print_result(getResult(all_data))
