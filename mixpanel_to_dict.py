################ Takes a raw dump of mixpanel data (one GIANT string but approximately formatted as a sequence of python dictionaries) and 
################ selects out every instance of a particular event and formats into a python dictionary with the user ID's as keys and the data
################ from each action associated with that user as their values

import pickle
import re
import ast
import os

desiredProperty = '"Post Page Loaded"' ## Name of desired action

folders = [0, 1] ## First half of year in folder 0, remainder in 1
for folder in folders:
  rt = '/PathToDataHere/EventsRawData/' + str(folder) + '/' ## Root folder
  fls = os.listdir(rt) ## List of files in each folder

  count = 0
  for fl in range(0,len(fls)) :
      fl_name = rt + fls[fl]

      data = pickle.load( open( fl_name, "rb" ) )
      data = data[0]  ## Data stored here in 'data'
      print 'Loaded', fls[fl]

      propertyLocs = [(m.start(0), m.end(0)) for m in re.finditer(desiredProperty,data)] ## Locate the starting location for each instance of the desired property
      print len(propertyLocs) ## Print out the number of instances located 

      filteredEvents = {} ## Create python dictionary of requested event, with userIDs as keys
      perc = 0
      for i in range(0,len(propertyLocs)): ## Loop through each event
        if float(i)/float(len(propertyLocs)) > perc:
          perc = perc + .1
          print perc*100, '%', count ## Count off every 10% here

        testM = propertyLocs[i] ## Grab the individual event location in 'data'
        dmyStr =  data[testM[0]-150:testM[0]+1500] ## Need to clean up this line -- Grabs chunk before and after event name containing specifics of the event
        
        eventStart = dmyStr.find('"event":')
        eventEnd = dmyStr.find(',"properties')
        evName = dmyStr[eventStart+8:eventEnd]

        if evName == desiredProperty: ## Only accept event if the name matches what we're looking for
          endSect = [(m.start(0), m.end(0)) for m in re.finditer(r'(.)*:(.)*}}',dmyStr[eventEnd:])]
          dmyStr = dmyStr[eventStart-1:eventEnd + endSect[0][1]] ## Chop extra characters off the dummy string

          dmyDict = ast.literal_eval(dmyStr) ## Perform a literal evaluation of the string to create a dummy dictionary containing the specific event
          metaKeys = dmyDict.keys() 
          propKeys = dmyDict["properties"].keys()
          try:
            userID = dmyDict["properties"]["distinct_id"] ## If we can link a userID to the action save it, otherwise ignore
            save = True
          except:
            save = False

          if save:
            timeCalled = dmyDict["properties"]["time"]
            indDict = {} ## Create individual dictionary containing an inverted version of the dummy dictionary with the time of the event as key
            indDict[timeCalled] = dmyDict["properties"]
            try:
              allKeys = filteredEvents[userID].keys() ## If the user has been seen before, going to append the action to his dictionary value
            except:
              allKeys = [] ## If this is the first time seeing the user, initialize an empty value
            for key in allKeys:
              indDict[key] = filteredEvents[userID][key]
            filteredEvents[userID]  = indDict

   
    saveName = '/StorageLocationHere/' + desiredProperty + '-' + fls[fl] ### Save the dictionary from each file
    f = open(saveName, 'w')
    pickle.dump(filteredEvents,f)
    f.close()
    print 'Saved to', saveName
    count = count+1