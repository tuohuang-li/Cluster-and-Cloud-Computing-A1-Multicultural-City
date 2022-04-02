from contextlib import nullcontext
import json
from re import A
from iso639 import languages
import pandas as pd
import time


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
          result["total_tweets"] += 1
          result[ "number_of_languages"] = len(result["languages"])
          if (row["doc"]["lang"] in result["languages"]):
            result["languages"][row["doc"]["lang"]] += 1
          else:
            result["languages"][row["doc"]["lang"]] = 1

  return results

          
start_time = time.time()
results = getResult(tinyTwitter)
end_time = time.time()
print("total running time is: ", (end_time - start_time))
print(results)
