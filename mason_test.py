from contextlib import nullcontext
import json
from iso639 import languages
import pandas as pd
import time
from lang import languages

def loadFile(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

sydGrid = loadFile('sydGrid-2.json')

logiCode=['1', '2', '3', '4']
latiCode = ['A', 'B', 'C', 'D']

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


logi, lati = getGeoCodeSets(sydGrid)


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


def loadFile(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

tinyTwitter = loadFile('smallTwitter.json')

def get_languages(lan):
  for i in languages:
    if(i[0] == lan):
      return i[1]
  
  return 'Unknown'

# Generate results array to store manipulated data
def generate_results_array():
  results = []
  for i in latiCode:
    for j in logiCode:
      results.append({"cell":i + j, "total_tweets" : 0, "number_of_languages": 0 , "languages": {}})
  return results

# Processing tiny twitter
def getResult(tinyTwitter):
  results = generate_results_array()
  for row in tinyTwitter["rows"]:
    if(row["doc"]["coordinates"] != None):
      coordinates = row["doc"]["coordinates"]["coordinates"]
      area_code = getAreaCode(coordinates[0], coordinates[1], logi, lati)
      for result in results:
        if(result["cell"] == area_code):
          language = get_languages(row["doc"]["lang"])
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

          
start_time = time.time()
results = getResult(tinyTwitter)
end_time = time.time()
print("total running time is: ", (end_time - start_time))
def print_result():
  print('cell', '#Total Tweets', '#Number of Languages Used')
  for result in results:
    print(result['cell'],'\t', result['total_tweets'], '\t\t', result['number_of_languages'] ,'\t', result['languages'])
print_result()
