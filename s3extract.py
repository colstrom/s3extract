#!/usr/bin/env python3.1
from xml.dom import minidom
import os,pickle,shlex,subprocess,sys
class File:
	def __init__(self,xmlNode):
		self.key		= xmlNode.getElementsByTagName('Key').item(0).childNodes.item(0).nodeValue
		self.lastModified	= xmlNode.getElementsByTagName('LastModified').item(0).childNodes.item(0).nodeValue
		self.eTag		= xmlNode.getElementsByTagName('ETag').item(0).childNodes.item(0).nodeValue
		self.size		= int(xmlNode.getElementsByTagName('Size').item(0).childNodes.item(0).nodeValue)
class Bucket:
	def Output(self):
		for file in self.files:
			print(self.GetURL(file.key))
	def GetFiles(self):
		return len(self.files)
	def GetSize(self,units = 'B'):
		size = 0
		for file in self.files:
			size = size + file.size
		if ( ( units == 'K' ) or ( units == 'M' ) ):
			size = int(size / 1024)
		if ( units == 'M' ):
			size = int(size / 1024)
		return size
	def GetURL(self,file = ''):
		return "http://{name}.s3.amazonaws.com/{key}".format(name=self.name,key=file)
	def AddFiles(self,xmlNodeList):
		for node in xmlNodeList:
			file = File(node)
			if ( (file.key not in self.alreadyIndexed) and (file.key.find('_$folder$') == -1) ):
				self.files.append(file)
				self.alreadyIndexed.append(file.key)
	def Setup(self,xmlNodeList):
		self.name		= xmlNodeList.getElementsByTagName('Name').item(0).childNodes.item(0).nodeValue
#		self.prefix		= xmlNodeList.getElementsByTagName('Prefix').item(0).childNodes.item(0).nodeValue
#		self.marker		= xmlNodeList.getElementsByTagName('Marker').item(0).childNodes.item(0).nodeValue
		self.maxKeys		= xmlNodeList.getElementsByTagName('MaxKeys').item(0).childNodes.item(0).nodeValue
		self.isTruncated	= xmlNodeList.getElementsByTagName('IsTruncated').item(0).childNodes.item(0).nodeValue
	def __init__(self,xmlNodeList):
		self.alreadyIndexed = []
		self.files = []
		self.Setup(xmlNodeList)
		self.AddFiles(xmlNodeList.getElementsByTagName('Contents'))
if ( len(sys.argv) > 1 ):
	if ( (sys.argv[1] == '-V') or (sys.argv[1] == '--version') ):
		print('S3 Extract v2010.09.08 -- Copyright (c) 2010 Chris Olstrom')
	elif ( (sys.argv[1] == '-h') or (sys.argv[1] == '--help') ):
		print('Usage:')
		print("\t-V | --version     \tDisplay version information")
		print("\t-h | --help        \tDisplay this help message.")
		print("\t-r | --remote <url>\tParse contents of remote bucket at <url>, generate database for use with --list")
		print("\t-l | --list        \tRead database generated with --remote, and output a list of files.")
	elif ( (sys.argv[1] == '-l') or (sys.argv[1] == '--list') ):
		if os.path.exists('fetch.db'):
			state = open('fetch.db','rb')
			S3 = pickle.load(state)
			state.close()
			S3.Output()
		else:
			print("WARNING: No data found, please fetch with {command} -r <url>".format(command=sys.argv[0]))
	elif ( (sys.argv[1] == '-r') or (sys.argv[1] == '--remote') ):
		if ( len(sys.argv) > 2 ):
			subprocess.call(shlex.split('fetch -o fetch.out '+sys.argv[2]))
			S3 = Bucket(minidom.parse('fetch.out'))
			while(S3.isTruncated == 'true'):
				print("WARNING: Incomplete dataset, fetching next set of results...")
				known_files = S3.GetFiles()
				subprocess.call(shlex.split('fetch -o fetch.out ' + "{}?marker={}".format(S3.GetURL(),S3.files[len(S3.files)-1].key).replace(' ','%20')))
				S3.Setup(minidom.parse('fetch.out'))
				S3.AddFiles(minidom.parse('fetch.out').getElementsByTagName('Contents'))
				if ( S3.GetFiles() == known_files ):
					break
			print("This bucket contains ~{} known files using ~{}MB of storage.".format(S3.GetFiles(),S3.GetSize('M')))
			state = open('fetch.db','wb')
			pickle.dump(S3,state)
			state.close()
		else:
			print("Usage: {command} -r <url>".format(command=sys.argv[0]))
else:
	print("Usage:\t{command} -r <url>\n\t{command} -l".format(command=sys.argv[0]))
