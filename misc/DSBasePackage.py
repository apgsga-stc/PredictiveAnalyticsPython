import json
from os.path import expanduser
import sys
from pathlib import Path, PosixPath

home = expanduser("~")
home_path = PosixPath(home)
config_path = home_path.joinpath(".datascienceapg")
config_file = config_path.joinpath("config.json")

config = json.load(open(str(config_file)))

sys.path.append( config['toolboxpath'] )


print( "appending toolbox path %s" % config['toolboxpath'] )

def getPath( base ):
    if not base in listPaths():
      raise Exception("%s path not found in .datascienceapg/config.json file" % base )       
    return( PosixPath( config[ base ] ) ) 

def getToolboxPath():
    return( config['toolboxpath']  )

def listPaths():
  keys = list( config.keys())
  return( keys )                       
#
