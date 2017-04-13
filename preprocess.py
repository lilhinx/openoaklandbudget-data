import csv
import argparse
import glob
import json
import os
import hashlib
import shutil
import time

AMOUNT_KEY = "AMT"

def rawDataDir( ):
	return "./data/raw"
	
def outputDataDir( ):
	ret = "./data/preprocess"
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

def outputDataDirYear( year, subdirs ):
	year_dir = "/".join( [ outputDataDir( ), year, "/".join( subdirs ) ] )
	if not os.path.exists( year_dir ):
		os.makedirs( year_dir )
	return year_dir

def writeGlobalFrag( fragObj, filename ):
	outputFilename = "/".join( [ outputDataDir( ), filename ] ) 
	with open( outputFilename, 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )
		
def yearFragFilename( year, subdirs, filename ):
	return "/".join( [ outputDataDirYear( year, subdirs ), filename ] ) 
	
def writeYearFrag( fragObj, year, subdirs, filename ):
	with open( yearFragFilename( year, subdirs, filename ), 'w+' ) as f:
		f.write( json.dumps( fragObj, sort_keys=True, indent=5 ) )

def genFrag_years( ):
	rawCSVFiles = glob.glob( "/".join( [ rawDataDir( ), "*.csv" ] ) )
	ret = [ ]
	for csvFile in rawCSVFiles:
		csvFile = csvFile.replace( rawDataDir( ), "" )
		csvFile = csvFile.replace( "/", "" )
		csvFile = csvFile.replace( ".csv", "" )
		ret.append( csvFile )
	return ret

def getFrag_headers( year ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ year, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header = datareader.next( )
		ret = { }
		for idx, val in enumerate(header):
			ret.update( { encodeKey( val ):{ "label":val, "pos":idx } } )
		return ret

def genFrag_taxonomyRoot( year, header_item_index, key, label ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ year, "csv" ] ) ] )
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header_row = datareader.next( )
		values = set( )
		for row in datareader:
			values.add( row[ header_item_index ] )
		values_list = list( values )
		values = [ ]
		for value in values_list:
			values.append( { "key":encodeValue( value ), "value":value } )
		return { "key":key, "label":label, "pos":header_item_index, "values":values }

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
	
def generateChildNodes( year, key, descendentKeys, parent ):
	keyFrag = json.load( open( yearFragFilename( year, [ "taxonomy" ], "{0}.json".format( key ) ), 'r' ) )
	for value in keyFrag[ "values" ]:
		node = IndexNode( keyFrag[ "key" ], value[ "value" ], value[ "key" ], keyFrag[ "pos" ] )
		parent.addChild( node )
		if len( descendentKeys ) > 0:
			generateChildNodes( year, descendentKeys[ 0 ], descendentKeys[1:], node )
	
def processNode( year, node, amt_pos ):
	filename = "/".join( [ rawDataDir( ), ".".join( [ year, "csv" ] ) ] )
	amounts = [ ]
	with open( filename, 'r' ) as datafile:
		datareader = csv.reader( datafile, delimiter=',' )
		header_row = datareader.next( )
		for row in datareader:
			if node.evaluate( row ):
				amounts.append( float( row[ amt_pos ] ) )
	subdirs = [ "index", node.path( ) ]
	writeYearFrag( { "sum":sum( amounts ) }, year, subdirs, "summary.json" )
	for childNode in node.children:
		processNode( year, childNode, amt_pos )

def main( ):
	parser = argparse.ArgumentParser( description='A utiltiy to preprocess budget CSV data into JSON fragments' )
	args = parser.parse_args( )
	shutil.rmtree( outputDataDir( ) )
	config = json.load( open( "config.json", 'r' ) )
	years = genFrag_years( )
	writeGlobalFrag( years, "years.json" )
	for year in years:
		header = getFrag_headers( year )
		writeYearFrag( header, year, "", "header.json" )
		amt_pos = -1
		for key, value in header.iteritems( ):
			if value[ "label" ] == AMOUNT_KEY:
				amt_pos = value[ "pos" ]
				continue
			root = genFrag_taxonomyRoot( year, value[ "pos" ], key, value[ "label" ] )
			root_filename = "{0}.json".format( key )
			writeYearFrag( root, year, [ "taxonomy" ], root_filename )
		if amt_pos < 0:
			return
		root = IndexNode.root( )
		for keys in config[ "indices" ]:
			generateChildNodes( year, keys[ 0 ], keys[1:], root )
		processNode( year, root, amt_pos )
	

if __name__ == "__main__":
	main( )