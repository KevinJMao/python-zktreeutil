#python-zktreeutil
---
**Author:** [Kevin Mao](https://www.linkedin.com/in/kevinjmao)

## Summary
---
Python version of the original [zktreeutil](https://github.com/apache/zookeeper/tree/trunk/src/contrib/zktreeutil), a tool for managing the Zookeeper metadata tree in a fast, straightforward manner.

Basic operations include:

  * **PRINT** - Recursively print data and metadata for all ZNodes under a given path.
  * **COPY** - Recursively copy ZNodes from one Zookeeper ensemble to another.
  * **EXPORT** - Export ZNode data to JSON.
  * **IMPORT** - Import JSON containing ZNode data into a Zookeeper ensemble.
  
## Usage
---
```
Usage:
	PRINT ZNODES: zktreeutil.py --print [source_zookeeper]
	Example:
	  # Print all ZNodes under /path/to/target on zookeeper1
	  zktreeutil.py --print zookeeper1:2181/path/to/target

	COPY ZNODES: zktreeutil.py --copy [--no-clobber|--interactive|--overwrite] [source_zookeeper] [destination_zookeeper]
	Example:
	  # Copy ZNodes under /path/to/src on zookeeper1 into /path/to/dst on zookeeper2. Skip any ZNodes that already exist.
	  zktreeutil.py --copy --no-clobber zookeeper1:2181/path/to/src zookeeper2:2181/path/to/dst

	EXPORT ZNODES: zktreeutil.py --export --file [target_file] [source_zookeeper]
	Example:
	  # Export ZNodes under /path/to/src on zookeeper as JSON into [target_file]
	  zktreeutil.py --export --file exported_znodes.json zookeeper1:2181/path/to/export

 	IMPORT ZNODES: zktreeutil.py --import [--no-clobber|--interactive|--overwrite] --file [target_file] [destination_zookeeper]
 	Example:
	  # Import ZNodes from imported_znodes.json and write them into zookeeper2 under /path/to/write/to
	  # Overwrite any ZNodes that already exist in the path
	  zktreeutil.py --import --overwrite --file imported_znodes.json zookeeper2:2181/path/to/write/to


Options:
  -h, --help            show this help message and exit
  -f FILE, --file=FILE  Read from or write to FILE, depending on which action
                        is specified.
  -v, --verbose         Enable verbose (debug) output

  Action options: Specify what action to perform (Default: --print):
    -p, --print         Print out contents of target Zookeeper location.
    -c, --copy          Copy contents of source Zookeeper location to
                        destination Zookeeper location.
    -x, --export        Write contents of target Zookeeper location to local
                        JSON file.
    -i, --import        Read contents of target JSON file and write data to
                        destination Zookeeper location.

  Resolution options: specify how to deal with conflicts. (Default: --no-clobber):
    --no-clobber        Do not overwrite any existing ZNodes.
    --interactive       Prompt user for each conflict encountered.
    --overwrite         Overwrite existing ZNodes without prompting user.
```

## Prerequisites
---
  * Python 2.6+
  * The following packages installed using pip:
    * kazoo 2.2.1+
    * simplejson 3.8.1+

## License
---
This project is license under the GNU GPLv3.
