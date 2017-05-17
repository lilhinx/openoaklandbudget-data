import csv
import argparse
import glob
import json
import os
import hashlib
import shutil
import time


def rawDataDir( ):
	return "./data/raw"
	
def outputDataDir( ):
	ret = "./data/process"
	if not os.path.exists( ret ):
		os.mkdir( ret )
	return ret
	
def encodeKey( key ):
	md5hash = hashlib.md5( )
	md5hash.update( key )
	return "k{0}".format( md5hash.hexdigest( ) )
	
def encodeValue( value ):
	md5hash = hashlib.md5( )
	md5hash.update( value )
	return "v{0}".format( md5hash.hexdigest( ) )

def outputDataDirDataset( dataset, subdirs ):
	dataset_dir = "/".join( [ outputDataDir( ), dataset, "/".join( subdirs ) ] )
	if not os.path.exists( dataset_dir ):
		os.makedirs( dataset_dir )
	return dataset_dir

def writeGlobalFrag( fragObj, filename ):
	outputFilename = "/".join( [ outputDataDir( ), filename ] ) 
	with open( outputFilename, 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )
		
def datasetFragFilename( dataset, subdirs, filename ):
	return "/".join( [ outputDataDirDataset( dataset, subdirs ), filename ] ) 
	
def writeDatasetFrag( fragObj, dataset, subdirs, filename ):
	with open( datasetFragFilename( dataset, subdirs, filename ), 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )

def genFrag_datasets( ):
	rawCSVFiles = glob.glob( "/".join( [ rawDataDir( ), "*.csv" ] ) )
	ret = [ ]
	for csvFile in rawCSVFiles:
		csvFile = csvFile.replace( rawDataDir( ), "" )
		csvFile = csvFile.replace( "/", "" )
		csvFile = csvFile.replace( ".csv", "" )
		dataSetDef = { "name":csvFile }
		dataSetDef[ "config" ] = loadDatasetConfig( csvFile )
		ret.append( dataSetDef )
	
	return ret

def getFrag_headers( dataset ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ dataset, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header = datareader.next( )
		ret = { }
		for idx, val in enumerate(header):
			ret.update( { encodeKey( val ):{ "label":val, "pos":idx } } )
		return ret

def genFrag_taxonomyRoot( dataset, header_item_index, key, label ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ dataset, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header_row = datareader.next( )
		values = set( )
		for row in datareader:
			values.add( row[ header_item_index ] )
		values_list = list( values )
		values = [ ]
		for value in values_list:
			values.append( { "key":encodeValue( value ), "value":value.decode( 'utf-8', 'ignore' ).encode( "utf-8" ).strip( ) } )
		return { "key":key, "label":label.encode( 'utf-8' ).strip( ), "pos":header_item_index, "values":values }

def subtract_taxon( keys, taxonomy ):
	return { k:v for k, v in taxonomy.items( ) if k not in keys and v[ "label" ] != AMOUNT_KEY }


class IndexNode( object ):
	def __init__( self, key, value, valueKey, pos, isRoot=False ):
		self.key = key
		self.value = value
		self.valueKey = valueKey
		self.pos = pos
		self.children = [ ]
		self.isRoot = isRoot
		self.parent = None
		
	@classmethod
	def root( cls ):
		return IndexNode( "", "", "", 0, True )
	
	def evaluate( self, row ):
		if self.parent is not None:
			if not self.parent.evaluate( row ):
				return False
		if self.isRoot:
			return True
		return row[ self.pos ] == self.value
	
	
	def path( self ):
		if self.isRoot:
			return "/"
		localPath = "/{0}/{1}".format( self.key, self.valueKey )
		# localPath = "/{0}/{1}".format( self.pos, self.value )
		if self.parent is None or self.parent.isRoot:
			return localPath
		return "{0}{1}".format( self.parent.path( ), localPath )
	
	def printGraph( self ):
		print( self.path( ) )
		for child in self.children:
			child.printGraph( )
	
	def addChild( self, node ):
		assert isinstance( node, IndexNode )
		self.children.append( node )
		node.parent = self
	
def generateChildNodes( dataset, key, descendentKeys, parent ):
	keyFrag = json.load( open( datasetFragFilename( dataset, [ "taxonomy" ], "{0}.json".format( key ) ), 'r' ) )
	for value in keyFrag[ "values" ]:
		node = IndexNode( keyFrag[ "key" ], value[ "value" ], value[ "key" ], keyFrag[ "pos" ] )
		parent.addChild( node )
		if len( descendentKeys ) > 0:
			generateChildNodes( dataset, descendentKeys[ 0 ], descendentKeys[1:], node )
	
def processNode( dataset, node, amt_pos ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ dataset, "csv" ] ) ] )
	amounts = [ ]
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header_row = datareader.next( )
		for row in datareader:
			if node.evaluate( row ):
				amounts.append( float( row[ amt_pos ] ) )
	subdirs = [ "index", node.path( ) ]
	writeDatasetFrag( { "sum":sum( amounts ) }, dataset, subdirs, "summary.json" )
	for childNode in node.children:
		processNode( dataset, childNode, amt_pos )

def scanDatasets( ):
	datasets = genFrag_datasets( )
	writeGlobalFrag( datasets, "datasets.json" )
	return datasets
	
def scanDataset( dataset ):
	header = getFrag_headers( dataset[ 'name' ] )
	writeDatasetFrag( header, dataset[ 'name' ], "", "header.json" )
	return header
	
def analyzeDataset( dataset, header ):
	amt_pos = -1
	for key, value in header.iteritems( ):
		if key == dataset[ 'config' ][ 'AMT' ]:
			amt_pos = value[ "pos" ]
			continue
		root = genFrag_taxonomyRoot( dataset[ 'name' ], value[ "pos" ], key, value[ "label" ] )
		root_filename = "{0}.json".format( key )
		writeDatasetFrag( root, dataset[ 'name' ], [ "taxonomy" ], root_filename )
	return amt_pos
	
def loadDatasetConfig( dataset ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ dataset, "config", "json" ] ) ] )
	if not os.path.exists( filename ):
		return None
	config = json.load( open( filename, 'r' ) )
	if config.get( 'AMT', None ) == None:
		return None
	return config

def main( ):
	parser = argparse.ArgumentParser( description='A utility to preprocess budget CSV data into JSON fragments' )
	parser.add_argument( '--scan', action='store_true', help='only scan datasets' )
	parser.add_argument( '--analyze', action='store_true', help='only scan and analyze datasets' )
	args = parser.parse_args( )
	shutil.rmtree( outputDataDir( ) )
	datasets = scanDatasets( )
	if args.scan:
		quit( )
	
	for dataset in datasets:
		header = scanDataset( dataset )
		
		if dataset[ 'config' ] is None:
			print dataset[ 'name' ], " has no valid config"
			continue
			
		amt_pos = analyzeDataset( dataset, header )
		
		
		if args.analyze:
			continue
		if amt_pos is None or amt_pos < 0:
			print dataset[ 'name' ], " has no amount!"
			continue
		
		root = IndexNode.root( )
		for keys in dataset[ 'config'][ "indices" ]:
			if len( keys ) == 0:
				continue
			generateChildNodes( dataset[ 'name' ], keys[ 0 ], keys[1:], root )
		processNode( dataset[ 'name' ], root, amt_pos )
	

if __name__ == "__main__":
	main( )