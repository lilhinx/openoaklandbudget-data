import boto3
import argparse
import os

def outputDataDir( ):
	return "./data/process"

def main( ):
	parser = argparse.ArgumentParser( description='A utiltiy to upload a raw static index of preprocessed financial data' )
	parser.add_argument( "-b", "--bucket", required=False, default='openoaklandbudget-data' )
	args = parser.parse_args( )
	client = boto3.client( 's3' )
	
	local_directory = outputDataDir( )
	for root, dirs, files in os.walk( local_directory ):
		for filename in files:
			if filename == ".DS_Store":
				continue
			# construct the full local path
			local_path = os.path.join( root, filename )
			# construct the full S3 path
			relative_path = os.path.relpath( local_path, local_directory )
			# s3_path = os.path.join( "/", relative_path )
			print( "uploading", relative_path )
			client.upload_file( local_path, args.bucket, relative_path )
	
	

if __name__ == "__main__":
	main( )