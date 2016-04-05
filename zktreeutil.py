#!/usr/bin/env python

# NOTE: Requires Kazoo, SimpleJson be installed
from optparse import OptionParser, OptionGroup
import os
import string
import sys
import logging
from kazoo.client import KazooClient
try:
	import simplejson as json
except:
	import json

class ZNode(object):
	"""A simple container for ZNode data
	"""
	def __init__(self, path, stat, data):
		self.path = path
		self.stat = stat
		self.data = data

class Action:
	PRINT, COPY, EXPORT, IMPORT = range(4)

class Resolve:
	NO_CLOBBER, INTERACTIVE, OVERWRITE = range(3)


def parse_zk_string(zk_string):
	"""Separate a ZK connect string (e.g. zookeeper.foo.com:2181/znode1/subnode1)
	into the hostname:port and the ZK path.
	"""
	idx = string.find(zk_string, '/')
	if idx == -1:
		raise Exception('Invalid Zookeeper connect string')
	else:
		return (zk_string[:idx], zk_string[idx:])
	

def init_logger(logger_id, log_level):
	"""Initialize logging facility to specific logger ID and level.
	"""
	logger = logging.getLogger(logger_id)
	logger.setLevel(log_level)
	ch = logging.StreamHandler()
	ch.setLevel(log_level)
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	return logger


def get_opt_parse():
	usage = """
	PRINT ZNODES: %prog --print [source_zookeeper]
	Example:
	  # Print all ZNodes under /path/to/target on zookeeper1
	  %prog --print zookeeper1:2181/path/to/target

	COPY ZNODES: %prog --copy [--no-clobber|--interactive|--overwrite] [source_zookeeper] [destination_zookeeper]
	Example:
	  # Copy ZNodes under /path/to/src on zookeeper1 into /path/to/dst on zookeeper2. Skip any ZNodes that already exist.
	  %prog --copy --no-clobber zookeeper1:2181/path/to/src zookeeper2:2181/path/to/dst

	EXPORT ZNODES: %prog --export --file [target_file] [source_zookeeper]
	Example: 
	  # Export ZNodes under /path/to/src on zookeeper as JSON into [target_file]
	  %prog --export --file exported_znodes.json zookeeper1:2181/path/to/export

 	IMPORT ZNODES: %prog --import [--no-clobber|--interactive|--overwrite] --file [target_file] [destination_zookeeper]
 	Example:
	  # Import ZNodes from imported_znodes.json and write them into zookeeper2 under /path/to/write/to
	  # Overwrite any ZNodes that already exist in the path
	  %prog --import --overwrite --file imported_znodes.json zookeeper2:2181/path/to/write/to
 	"""
	parser = OptionParser(usage=usage)
	action_group = OptionGroup(parser, 'Action options: Specify what action to perform (Default: --print)')
	# Actions
	action_group.add_option('-p', '--print', action='store_const', const=Action.PRINT, 
		dest='action', help='Print out contents of target Zookeeper location.')
	action_group.add_option('-c', '--copy', action='store_const', const=Action.COPY,
		dest='action', help='Copy contents of source Zookeeper location to destination Zookeeper location.')
	action_group.add_option('-x', '--export', action='store_const', const=Action.EXPORT,
		dest='action', help='Write contents of target Zookeeper location to local JSON file.')
	action_group.add_option('-i', '--import', action='store_const', const=Action.IMPORT,
		dest='action', help='Read contents of target JSON file and write data to destination Zookeeper location.')
	parser.add_option_group(action_group)
	parser.set_defaults(action=Action.PRINT)

	# Resolution
	resolution_group = OptionGroup(parser, 'Resolution options: specify how to deal with conflicts. (Default: --no-clobber)')
	resolution_group.add_option('--no-clobber', action='store_const', const=Resolve.NO_CLOBBER,
		dest='resolve', help='Do not overwrite any existing ZNodes.')
	resolution_group.add_option('--interactive', action='store_const', const=Resolve.INTERACTIVE,
		dest='resolve', help='Prompt user for each conflict encountered.')
	resolution_group.add_option('--overwrite', action='store_const', const=Resolve.OVERWRITE,
		dest='resolve', help='Overwrite existing ZNodes without prompting user.')
	parser.add_option_group(resolution_group)
	parser.set_defaults(resolve=Resolve.NO_CLOBBER)

	parser.add_option('-f', '--file', action='store', dest='file_loc', metavar='FILE',
		help='Read from or write to FILE, depending on which action is specified.')
	parser.add_option('-v', '--verbose', action='store_true', dest='verbose', default=False,
		help='Enable verbose (debug) output')
	return parser


def join_paths(base_path, *relative_paths):
	"""Join Zookeeper paths by appending relative path(s) to the base path.
	"""
	rel_paths = [ x.strip('/') for x in relative_paths]
	result_path = base_path.rstrip('/')
	for rel_path in rel_paths:
		result_path = result_path + '/' + rel_path
	return result_path


def create_zk_client(zk_connect):
	zk_client = KazooClient(hosts=zk_connect)
	zk_client.start()
	return zk_client


class ZkTreeUtil(object):
	def __init__(self):
		parser = get_opt_parse()
		(self.option, self.args) = parser.parse_args()
		if len(self.args) == 0:
			parser.error('Invalid number of arguments')
		if self.option.file_loc and (self.option.action == Action.PRINT or self.option.action == Action.COPY):
			parser.error('--file should not be used with PRINT or COPY actions.')

		log_level = { True : logging.DEBUG, False: logging.INFO }[self.option.verbose]
		self.logger = init_logger('zk-util', log_level)

		
	def traverse_zk_tree(self, src_zk_client, path, process_znode, **process_znode_kwargs):
		"""Recursively traverse the directory tree at the target Zookeeper ensemble using depth-first
		search. When visiting each ZNode, call some function process_znode and accompanying args to 
		do something with that node (e.g. print it, copy it somewhere else, etc.)
		"""
		self.logger.debug('Processing ZNode located at %s' % path)
		data, stat = src_zk_client.get(path)
		znode = ZNode(path, stat, data)
		process_znode(znode, **process_znode_kwargs)

		for child in src_zk_client.get_children(path):
			child_path = join_paths(path, child)
			self.traverse_zk_tree(src_zk_client, child_path, process_znode, **process_znode_kwargs)


	def process_znode_print(self, znode):
		"""Print the ZNode's path, data, and metadata to stdout.
		"""
		print('ZNode path: %s' % znode.path)
		print('ZNode stat: %s' % str(znode.stat))
		if len(znode.data) == 0:
			print('ZNode data: (empty)\n')
		else:	
			print('ZNode data: %s\n' % znode.data)


	def process_znode_write_to_zk(self, znode, dest_zk_client, dest_zk_path, resolve):
		"""Copy the ZNode's data to the destination Zookeeper instance. All ZNodes are written
		under the directory specified by dest_zk_path. 
		"""
		dest_znode_path = join_paths(dest_zk_path, znode.path)
	
		if dest_zk_client.exists(dest_znode_path):
			if resolve == Resolve.INTERACTIVE:
				response = ''
				while string.lower(response) != 'y' and string.lower(response) != 'n':
					response = raw_input('ZNode at %s already exists at destination. Overwrite? (y/n)' % dest_znode_path)
					self.logger.debug('User response: %s.' % response)
				if response == 'y':
					self.logger.debug('Overwriting ZNode data at %s' % dest_znode_path)
					dest_zk_client.set(dest_znode_path, znode.data)	
				else:
					self.logger.debug('Skipping ZNode at %s' % dest_znode_path)

			elif resolve == Resolve.OVERWRITE:
				self.logger.debug('ZNode at %s already exists. Overwriting data due to --overwrite' % dest_znode_path)
				dest_zk_client.set(dest_znode_path, znode.data)

			else:
				self.logger.debug('ZNode at %s already exists. Skipping due to --no-clobber' % dest_znode_path)
		else:
			self.logger.info('Writing new ZNode at %s' % dest_znode_path)
			dest_zk_client.create(dest_znode_path, znode.data, makepath=True)


	def process_znode_write_dict(self, znode, znode_dict):
		znode_dict[znode.path] = dict()
		znode_dict[znode.path]['data'] = znode.data
		znode_dict[znode.path]['stat'] = znode.stat


	def run_copy(self, source_zk, source_zk_path, dest_zk, dest_zk_path, resolve):
		"""Copy a ZNode and all children in the source Zookeeper to some path in the destination Zookeeper.
		"""
		source_zk_client = create_zk_client(source_zk)
		dest_zk_client = create_zk_client(dest_zk)
		self.traverse_zk_tree(source_zk_client, source_zk_path, self.process_znode_write_to_zk, 
			dest_zk_client=dest_zk_client, dest_zk_path=dest_zk_path, resolve=resolve)


	def run_import(self, source_file, dest_zk, dest_zk_path, resolve):
		"""Read a Zookeeper's ZNode and all children and dump the path/data/metadata into a file as JSON.	
		"""
		f = open(source_file, 'r')
		dest_zk_client = create_zk_client(dest_zk)
		znode_dict = json.loads(f.read())
		for (path, znode_val) in znode_dict.items():
			znode = ZNode(path, znode_val['stat'], str(znode_val['data']))
			self.process_znode_write_to_zk(znode, dest_zk_client, dest_zk_path, resolve)	
		f.close()


	def run_export(self, source_zk, source_zk_path, dest_file):
		"""Read a Zookeeper's ZNode and all children and dump the path/data/metadata into a file as JSON.
		"""
		znode_dict = dict()
		source_zk_client = create_zk_client(source_zk)
		self.traverse_zk_tree(source_zk_client, source_zk_path, self.process_znode_write_dict, znode_dict=znode_dict)
		f = open(dest_file, 'w')
		f.write(json.dumps(znode_dict, sort_keys=True))


	def run_print(self, source_zk, source_zk_path):
		source_zk_client = create_zk_client(source_zk)
		self.traverse_zk_tree(source_zk_client, source_zk_path, self.process_znode_print)
		source_zk_client.stop()


	def run(self):
		if(self.option.action == Action.COPY):
			self.logger.info('Running action: COPY')
			source_zk, source_zk_path = parse_zk_string(self.args[0])
			dest_zk, dest_zk_path = parse_zk_string(self.args[1])
			resolve = self.option.resolve
			self.run_copy(source_zk, source_zk_path, dest_zk, dest_zk_path, resolve)
			self.logger.info('Completed action: COPY')
		elif(self.option.action == Action.IMPORT):
			self.logger.info('Running action: IMPORT')
			source_file = self.option.file_loc
			dest_zk, dest_zk_path = parse_zk_string(self.args[0])
			resolve = self.option.resolve
			self.run_import(source_file, dest_zk, dest_zk_path, resolve)
			self.logger.info('Completed action: IMPORT')
		elif(self.option.action == Action.EXPORT):
			self.logger.info('Running action: EXPORT')
			dest_file = self.option.file_loc
			source_zk, source_zk_path = parse_zk_string(self.args[0])
			self.run_export(source_zk, source_zk_path, dest_file)
			self.logger.info('Completed action: EXPORT')
		else:
			self.logger.info('Running action: PRINT')
			source_zk, source_zk_path = parse_zk_string(self.args[0])
			self.run_print(source_zk, source_zk_path)
			self.logger.info('Completed action: PRINT')


def main():
	util = ZkTreeUtil()
	util.run()


if __name__ == '__main__':
	main()