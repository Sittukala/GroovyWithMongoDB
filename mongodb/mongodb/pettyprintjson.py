import  json, io, sys, os, fileinput, pprint


with io.open("src/main/resources/climate_change_tweets_all.json",'r') as jf:
    data = json.load(jf)
'pp.pprint(data)'
print(json.dumps(data,indent=2))

