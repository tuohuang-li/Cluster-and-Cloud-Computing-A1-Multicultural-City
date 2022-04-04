import json
import re
import time
from lang import languages

#from mpi4py import MPI

def chunkTwitter(flist, x):
  """

  :param flist: A list of dictionary read from twitter file
  :param x: slices end before index x
  :return: sliced list for parallel
  """
  list = []
  for i in range(4): #0, 1, 2, 3
    list.append(flist[i::x]) # start from i, every xth element
  return list
   
#comm = MPI.COMM_WORLD
#comm_rank = comm.Get_rank()
#comm_size = comm.Get_size()


def loadFile(file):
  """

  :param file: json file contains twitts
  :return: lines in json file that contains geo information in the format of:
          [{"coordinates": [x, y], "lang": 'language code' }]
  """
  f_list = []
  total_row = 0
  #file = 'tinyTwitter.json'
  with open(file, 'r', encoding="utf8") as f:

    for item in f:

      if (item.endswith(':[\n')):
        #print(item)
        total_row = re.findall(r'-?\d+\.?\d*', item)[0]

      else:
        if(item.endswith('}},\n')):
          item = item[:-2]

        elif(item.endswith('}}\n')):
          item = item[:-1]

        elif(item.endswith('}}]}\n')): #last row
          item = item[:-3]
          #print(item)
        row = json.loads(item)

        if (row["doc"]["coordinates"] != None):
            # row["doc"]["coordinates"]["coordinates"]
            # row["doc"]["lang"]
          f_list.append({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"] })
          #print({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"] })

    print(total_row)
    return f_list, total_row

def loadFileGrid(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

#sydGrid = loadFileGrid('sydGrid.json')

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

#logi, lati = getGeoCodeSets(sydGrid)
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
  return area['latiCode'] + area['logiCode']


def get_languages(lan):
  for i in languages:
    if(i[0] == lan):
      return i[1]
  
  return 'Unknown'

# Generate an empty results array to store manipulated data
def generate_results_array():
  results = []
  for i in latiCode:
    for j in logiCode:
      results.append({"cell":i + j, "total_tweets" : 0, "number_of_languages": 0 , "languages": {}})
  return results

# Processing tiny twitter
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
      newDictionary={}
      sortedList=sorted(result["languages"].values())
      i = 0
      for sortedKey in sortedList:
        for key, value in result["languages"].items():
          if(i > 9):
            break
          i += 1
          if value==sortedKey:
              newDictionary[key]=value

      result["languages"] = newDictionary
      final_result.append(result)


  return final_result


# if comm_rank == 0 and comm_size < 2:
#   start_time = time.time()
#   results = getResult(f_list)
#   end_time = time.time()
#   print("total running time is: ", (end_time - start_time))
#   def print_result():
#     print('cell', '#Total Tweets', '#Number of Languages Used')
#     for result in results:
#       print(result['cell'],'\t', result['total_tweets'], '\t\t', result['number_of_languages'] ,'\t', result['languages'])
#   print_result()
#if comm_rank == 0 and comm_size < 2:
def print_result():
  print('cell', '#Total Tweets', '#Number of Languages Used')
  for result in results:
    print(result['cell'],'\t', result['total_tweets'], '\t\t', result['number_of_languages'] ,'\t', result['languages'])


if __name__ == "__main__":
  start_time1 = time.time()
  sydGrid = loadFileGrid('sydGrid.json')
  logi, lati = getGeoCodeSets(sydGrid)
  file = 'tinyTwitter.json'
  twitts, total_row = loadFile(file)
  end_time1 = time.time()
  print("total loading time is: ", (end_time1 - start_time1))

  start_time2 = time.time()
  results = getResult(twitts)
  print_result()
  end_time2 = time.time()
  print("total running time is: ", (end_time2 - start_time2))


