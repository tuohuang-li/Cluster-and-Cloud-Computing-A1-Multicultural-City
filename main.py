import json
import os
import pandas as pd
import time
#from mpi4py import MPI
from iso639 import languages


os.chdir(r'C:\Users\Tuohuang\Documents\COMP90024-Cluster and cloud computing\A1\a1-code')
#file = open('sydGrid-2.json')
#sydGrid = json.load(file)  # type = dict
#print (type(sydGrid))

#print(len(sydGrid["features"][0]['geometry']['coordinates'][0]))

"""
To solve the following issue -
UnicodeDecodeError: 'charmap' codec can't decode byte 0x81 in position 
31510: character maps to <undefined>. 
have to add"encoding="utf8" when open the file


#file_tiny = open('tinyTwitter.json', encoding="utf8")
#tinyTwitter = json.load(file_tiny)

#for key in tinyTwitter['rows'][0]['doc']['coordinates'].keys():

#    print(key)
"""

"""
step 1: find coordinates
"""
def loadFile(file):
    with open(file, 'r', encoding="utf8") as f:
        gridData = json.load(f)
    return gridData

def getCoordList(grid):
    """
    :param grid: dictionary obtained via open file
    :return: two list stores statistics of the top left corner coordinates (latitude/longitude respectively)
    """
    long = []
    lat = []
    for item in grid["features"]:
        top_l_point = item["geometry"]["coordinates"][0][0]
        #bottom_r_point = item["geometry"]["coordinates"][0][2]
        long.append(top_l_point[0])
        lat.append(top_l_point[1])

    longitude = sorted(set(long))
    latitude = sorted(set(lat), reverse=True)

    return latitude, longitude

"""
Step 2: Assign Area code
"""

def getGrid(grid, latList, longList):
    """
    x1: longitude of the top left point eg.150.7655
    y1: latitude of the top left point eg. -33.55412
    x2: longitude of the bottom right point
    y2: latitude of the bottom right point
    LatCode: A,B,C,D
    LongCode: 1,2,3,4
    :param grid: A dictionary read from grid json file.
    :return: a pandas dataframe contains grid ID, two points.
    """
    global latCode, longCode
    codeMap = {0:'A', 1:'B', 2:'C', 3:'D'}
    grid_info_df = pd.DataFrame(columns=['id', 'latCode','longCode', 'x1', 'y1', 'x2', 'y2'])

    for item in grid['features']:
        properties = item['properties']
        top_l_point = item['geometry']['coordinates'][0][0]
        bottom_r_point = item['geometry']['coordinates'][0][2]
        id = properties['id']
        x1 = top_l_point[0]
        y1 = top_l_point[1]
        x2 = bottom_r_point[0]
        y2 = bottom_r_point[1]

        for i in range(len(latList)):
            if top_l_point[1] == latList[i]:
                latCode = codeMap[i]

        for j in range(len(longList)):
            if top_l_point[0] == longList[j]:
                longCode = j+1

        polygon_df = pd.DataFrame([[id, latCode, longCode, x1, y1, x2, y2]],
                                  columns=['id', 'latCode', 'longCode', 'x1', 'y1', 'x2', 'y2'], dtype='float')
        grid_info_df = grid_info_df.append(polygon_df, ignore_index=True)

    grid_info_df = grid_info_df.astype({'id': 'int64', 'longCode':'int64'})
    grid_info_df = grid_info_df.convert_dtypes()
    #print(grid_info_df.info())
    grid_info_df["cell"] = grid_info_df['latCode'] + grid_info_df['longCode'].astype(str)
    grid_info_df = grid_info_df.set_index('cell')
    grid_info_df = grid_info_df.drop(['id', 'latCode' ,'longCode'], axis=1)
    grid_info_df = grid_info_df.transpose()

    return grid_info_df


sydGrid = loadFile('sydGrid-2.json')
lat, long = getCoordList(sydGrid)
df = getGrid(sydGrid, lat, long)
print(df)
#print(list(df['x1']))


dictionaryObject = df.to_dict()  #index 0-15
#print(dictionaryObject)


"""
Step 3: cell allocator
"""
def cell_allocator(x, y, grid):
    """
    If a tweet occurs right on the border of two cells, keep left, keep down;
    If located on edge border, for example A1, then it can be regarded as being in cell A1.
    :param x: longitude of twitter sent
    :param y: latitude of twitter sent
    :param grid: grid dataframe contains 16 grids' information
    :return: cell code eg. A1, C4, D2
    """
    #grid  = grid.reset_index(drop = True, inplace = True)
    dictionaryGrid = grid.to_dict()  #{'C4': {'x1': 151.2155, 'y1': -33.85412, 'x2': 151.3655, 'y2': -34.00412}...}
    #situation 1: not on any line
    cells = []

    for cell, coordinates in dictionaryGrid.items():
        if (coordinates["x1"] <= x <= coordinates["x2"]) and (coordinates["y2"] <= y <= coordinates["y1"]):
            if (x != coordinates["x1"]) and (y != coordinates["y2"]):
                return cell
            cells.append(cell)

    #case: outside all cells
    if len(cells) == 0:
        return None
    #case: top & down cells or left & right cells
    if len(cells) == 2:
        #top & down
        if dictionaryGrid[cells[0]]['y1'] == dictionaryGrid[cells[1]]['y1']:
            if dictionaryGrid[cells[0]]['x1'] < dictionaryGrid[cells[1]]['x1']:
                return cells[0]
            else:

                return cells[1]
        #left & right
        elif dictionaryGrid[cells[0]]['x1'] == dictionaryGrid[cells[1]]['x1']:
            if dictionaryGrid[cells[0]]['y1'] < dictionaryGrid[cells[1]]['y1']:

                return cells[0]
            else:
                return cells[1]
    #case: point shared by 4 cells
    else:
        print(cells)
        return cells[-2] #??

#print(cell_allocator(150.9155, -33.85412,df))
"""
Step 4: Read twitter file
"""
twitts_dict = loadFile('smallTwitter.json')

def process_twitts (twitts):
    """
    Write a loop iterate through all data and discard items without geo/coordinates information
    then store in pandas dataframe
    :param twitts: A dictionary read from twitter json file.
    :return: a pandas dataframe contains language code, coordinates and cell info
    """

    # Define a dataframe
    twitts_info_df = pd.DataFrame(columns=['langCode', 'cell', 'x', 'y', 'numberOfTwitts'])
    for item in twitts["rows"]:
        if item["doc"]["coordinates"] is not None:
            #print (item)
            #print (item["doc"]["coordinates"]["coordinates"])    # type: list
            langCode = item["doc"]["lang"]
            x = item["doc"]["coordinates"]["coordinates"][0]
            y = item["doc"]["coordinates"]["coordinates"][1]
            cell = cell_allocator(x, y, df)
            numberOfTwitts = 1
            """
            need a function to decide the "cell" of this twitter
            """

            itemInfo_df = pd.DataFrame([[langCode, cell, x, y, numberOfTwitts]],
                                      columns=['langCode', 'cell', 'x', 'y', 'numberOfTwitts'], dtype='float')
            twitts_info_df = twitts_info_df.append(itemInfo_df, ignore_index=True)

    twitts_info_df['langName'] = twitts_info_df['langCode'].apply(lambda x: languages.get(alpha2=x).name)
    print(twitts_info_df)

    #.size().reset_index(name='counts') this part helps return a result as a DataFrame (instead of a Series)
    twitts_info_df = twitts_info_df.groupby(['cell','langName']).size().reset_index(name='counts')\
                     .sort_values(['counts'],ascending=False)

    print(twitts_info_df)

    return twitts_info_df

start_time = time.time()
process_twitts(twitts_dict)
end_time = time.time()
print("total running time is: ", (end_time - start_time))
#t['combined']= t.values.tolist()




