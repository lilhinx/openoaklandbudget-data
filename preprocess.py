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
		is_header = True
		values = set( )
		for row in datareader:
			if is_header:
				is_header = False
				continue
			values.add( row[ header_item_index ] )
		values_list = list( values )
		values = [ ]
		for value in values_list:
			values.append( { "key":encodeValue( value ), "value":value } )
		return { "key":key, "label":label, "pos":header_item_index, "values":values }

def subtract_taxon( keys, taxonomy ):
	return { k:v for k, v in taxonomy.items( ) if k not in keys and v[ "label" ] != AMOUNT_KEY }

def process_index_numeric_summary( year, locus, predicate ):
	subdirs = [ "index" ]
	subdirs.extend( locus[:] )
	subdirs.append( values[:0].get( "key" ) )
	writeYearFrag( {"foo":"bar" }, year, subdirs, "summary.json" )


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
	
def explodePredicateKeys( year, key, descendentKeys, parent ):
	keyFrag = json.load( open( yearFragFilename( year, [ "taxonomy" ], "{0}.json".format( key ) ), 'r' ) )
	for value in keyFrag[ "values" ]:
		node = IndexNode( keyFrag[ "key" ], value[ "value" ], value[ "key" ], keyFrag[ "pos" ] )
		parent.addChild( node )
		if len( descendentKeys ) > 0:
			explodePredicateKeys( year, descendentKeys[ 0 ], descendentKeys[1:], node )

def genPredicateTree( year, keys ):
	root = IndexNode.root( )
	explodePredicateKeys( year, keys[ 0 ], keys[1:], root )
	root.printGraph( )

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
		for key, value in header.iteritems( ):
			if value[ "label" ] == AMOUNT_KEY:
				continue
			root = genFrag_taxonomyRoot( year, value[ "pos" ], key, value[ "label" ] )
			root_filename = "{0}.json".format( key )
			writeYearFrag( root, year, [ "taxonomy" ], root_filename )
				
		for indexConfig in config[ "indices" ]:
			predicateTree = genPredicateTree( year, indexConfig )
	

if __name__ == "__main__":
	main( )