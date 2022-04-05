#--------------------------------------------------------------------
# Program name: MPI twitter user language analysis on Spartan
# Created By    : Tuohuang Li / Guangxing Si
# Created Date  : 05/04/2022
# Version =  '1.0'
#--------------------------------------------------------------------

#--------------------------------------------------------------------
#Package used in this script:
#  - json: Convert json string into python dictionary
#  - lang: Language tuple list to convert short language code into language name
#  - mpi4py: MPI tool for python
#--------------------------------------------------------------------
import json
from lang import languages
from mpi4py import MPI

#--------------------------------------------------------------------
# Initialize MPI communicator
#--------------------------------------------------------------------
comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

#--------------------------------------------------------------------
# Global variable:
# data is list of dictionary. [{"area_code": "Area_Code", "lang": "languages"},...]
# logiCode: logicode to generate area code
# latiCode: laticode to generate area code
# lati: sets of latitude in order
# logi: sets of logitude in order
#--------------------------------------------------------------------
data = []
logiCode=['1', '2', '3', '4']
latiCode = ['A', 'B', 'C', 'D']
logi = []
lati = []

#--------------------------------------------------------------------
# Method goal:
# Convert line read from file object into dictionary and add to the data list
#--------------------------------------------------------------------
# input: line read from file object
# output: None
#--------------------------------------------------------------------
def read_line(line):
  try:
    # read normal row end with ','
    if (line.decode("utf-8").strip()[-1] == ','):
      row = json.loads(line.decode('utf-8').strip()[:-1])
      if (row["doc"]["coordinates"] is not None):
        data.append({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"]})
    # read last row end with }}]
    else:
      row = json.loads(line.decode('utf-8').strip()[:-2])  #last row contains special charas cannot be parsed by JSON??
      if (row["doc"]["coordinates"] is not None):
        data.append({"coordinates": row["doc"]["coordinates"]["coordinates"], "lang": row["doc"]["lang"]})
  except:
    pass

#--------------------------------------------------------------------
# Method goal:
# Load bigTwitter.json data by multi-core based on the scatter algorithm
#--------------------------------------------------------------------
# input: number of cores which could be obtained from comm.Get_size()
# output: None
#--------------------------------------------------------------------
def loadData(x):
  with open("bigTwitter.json", 'rb') as f:
    i = 0
    for item in f:
      if(i != 0):
        # assign read task to different rank based on the module operation
        number = i % x
        # to assign the last core to read corresponding lines
        if(number == 0 and comm_rank == x - 1):
          read_line(item)
        # to assign the rank with corresponding item (e.g. core1 with line1, core2 with line2 ...)
        elif(number == comm_rank + 1):
          read_line(item)
      i += 1

#--------------------------------------------------------------------
# Method goal:
# Load sydGrid data and convert json into dictionary
#--------------------------------------------------------------------
# input: file name of the grid json file
# output: grid dictionary
#--------------------------------------------------------------------
def loadGrid(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

#--------------------------------------------------------------------
# Method goal:
# get all latitude and longtitude of the grid intersections
#--------------------------------------------------------------------
# input: Grid dictionary
# output: sets of logitude and latitude in order
#--------------------------------------------------------------------
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

#--------------------------------------------------------------------
# Method goal:
# get the corresponding areacode by given latitude and logitude
#--------------------------------------------------------------------
# input: x longitude and y latitude
# output: area code
#--------------------------------------------------------------------
def getAreaCode(x, y):
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

#--------------------------------------------------------------------
# Method goal:
# return the full language name by short code of language
#--------------------------------------------------------------------
# input: language abbrev.
# output: full language name
#--------------------------------------------------------------------
def get_languages(lan):
  for i in languages:
    if(i[0] == lan):
      return i[1]
  return 'Unknown'

#--------------------------------------------------------------------
# Method goal:
# generate empty result array of desired dictionary to store data
#--------------------------------------------------------------------
# output: results array with initialized value
#--------------------------------------------------------------------
def generate_results_array():
  results_array = []
  for i in latiCode:
    for j in logiCode:
      results_array.append({"cell":i + j, "total_tweets" : 0, "number_of_languages": 0 , "languages": {}})
  return results_array

#--------------------------------------------------------------------
# Method goal:
# processing the twitter data and get desired results of dictionary
#--------------------------------------------------------------------
# output: list of dictionary with results
#--------------------------------------------------------------------
def getResult(twitter_file):
  results = generate_results_array()
  for row in twitter_file:
    coordinates = row["coordinates"]
    area_code = getAreaCode(coordinates[0], coordinates[1])  #find twitter location
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
  # Remove area without any tweets and sort out the top 10 langauges
  for result in results:
    if(result["total_tweets"] != 0):
      newDic = dict(sorted(result["languages"].items(), key=lambda item: item[1], reverse=True)[:10])
      result["languages"] = newDic
      final_result.append(result)
  return final_result


#--------------------------------------------------------------------
# Method goal:
# print function to formatting the final results
#--------------------------------------------------------------------
# input: list of dictionary with results
#--------------------------------------------------------------------
def print_result(results):
  print('Cell', '\t', '#Total Tweets', '\t', '#Number of Languages Used', '\t', '#Top 10 Languages & #Tweets')
  for result in results:
    language_list = []
    keys = result['languages'].keys()
    for key in keys:
      language_list.append(key + '-' + str(result['languages'][key]))
    print(result['cell'],'\t', result['total_tweets'], '\t\t', result['number_of_languages'] ,'\t\t\t\t', language_list)


if __name__ == "__main__":
  loadData(comm_size)
  sydGrid = loadGrid('sydGrid.json')
  logi, lati = getGeoCodeSets(sydGrid)
  all_data = []
  # only the rank 0 can gather all data
  gathered = comm.gather(data, root = 0)
  # ignore None gathered data gained from non-root core
  if(gathered is not None):
    for item in gathered:
      for single_data in item:
        all_data.append(single_data)
    result = getResult(all_data)
    print_result(result)
