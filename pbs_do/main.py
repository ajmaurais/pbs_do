
import sys
import os
import argparse
import subprocess
from math import ceil
import re

PBS_COUNT = 0

def makePBS(command, initial_args, mem, ppn, walltime, wd, arg_list):
    global PBS_COUNT
    pbsName = '{}_{}.pbs'.format(command, PBS_COUNT)
    _fileLists = getFileLists(ppn, fileList, check=False)

    if len(_fileLists) < ppn:
        ppn = len(_fileLists)

    with open(pbsName, 'w') as outF:
        outF.write("#!/bin/bash\n")
        outF.write('#PBS -l mem={}gb,nodes=1:ppn={},walltime={}\n\n'.format(mem, ppn, walltime))
        outF.write('cd {}\n'.format(wd))
        
        for i, l in enumerate(_fileLists):
            outF.write('; '.join(['{} -i {}'.format(RAW_EXTRACT_COMMAND, x) for x in l]))
            outF.write(' > stdout_{}_{}.txt &\n'.format(PBS_COUNT, i))
        outF.write('wait\n')

    PBS_COUNT += 1
    return pbsName


def getPlurality(num):
    if num > 1:
        return 's'
    else: return ''


def getFileLists(nProc, fileList, check=True):
    '''
    Get input file names and split into a list for each subprocess.

    Parameters
    ----------
    nProc: int
        Number of processes per job.
    fileList: list
        list of files.
    check: bool
        Should files in list be checked to see if they exist?

    Returns
    -------
        List of list containing file to run in each subprocess
    '''

    # check file list
    if check:
        _exit = False
        for f in fileList:
            if not os.path.exists(f):
                sys.stderr.write('{} does not exist.\n'.format(f))
                _exit = True
        if _exit:
            sys.exit(-1)

    #calculate number of files per thread
    nFiles = len(fileList)
    filesPerProcess = nFiles // nProc
    if nFiles % nProc != 0:
        filesPerProcess += 1

    #split up fileList
    ret = list()
    i = 0
    while(i < nFiles):
        # get beginning and end indecies
        begNum = i
        endNum = begNum + filesPerProcess
        if endNum > nFiles:
            endNum = nFiles

        ret.append(fileList[begNum:endNum])
        i += filesPerProcess

    fileSet = set()
    for i in ret:
        for j in i:
            fileSet.add(j)
    assert(len(fileSet) == len(fileList))

    return ret


def process_args(args: argparse.Namespace) -> list:
    '''
    Process args suplied by user.

    Parameters
    ----------
    args: argparse.Namespace
        Args namespace from argparse.

    Returns
    -------
    command_dict: dict
        Dict with 3 slots: command, initial_arguments, args
    '''

    # process command arguments
    if args.arg_file is not None:
        with open(args.arg_file, 'r') as inF:
            stdin = inF.read().strip()
    else:
        stdin = sys.stdin.read().strip()
    stdin = re.split(args.deliminator, stdin)
    _args = list()
    for arg in stdin:
        _arg = arg
        if args.resub is not None:
            _arg = re.sub(*args.resub, _arg)
        if args.regex is not None:
            match = bool(re.search(args.regex, _arg))
            if (args.regex_omit and not match) or (not args.regex_omit and match):
                _args.append(_arg)
        else:
            _args.append(_arg)

    command = args.command[0]
    initial_arguments = '' if len(args.command) == 1 else ' '.join(args.command[1:])
    # if args.replace_str is not None:
    #     initial_arguments.replace(args.replace_str, )

    return {'command': command, 'initial_arguments': initial_arguments, 'args': _args}

def main():

    parser = argparse.ArgumentParser(prog = 'pbs_do',
                                     description = 'Create PBS jobs from the standard input.')

    parser.add_argument("command", action="store", default=['/bin/echo'], type=str, nargs=argparse.REMAINDER)

    parser.add_argument("-a" '--arg-file', default=None, type=str, metavar='file', action='store', dest='arg_file',
                        help='Read items from file instead of standard input.')
    parser.add_argument("-I", action="store", type=str, default=None, metavar="replace-str", dest="replace_str",
                        help="replace occurrences of replace-str in the initial_arguments with input.")
    parser.add_argument("--resub", nargs=2, type=str, metavar=("pattern", "repl"), dest="resub", default=None,
                        help="replace occurrences of replace-str in the input with re.sub(patten, repl, input)")
    parser.add_argument("-r", type=str, default=None, metavar="regex", dest="regex",
                        help="only build commands from inputs matching regex")
    parser.add_argument("-o", action="store_true", dest="regex_omit", default=False,
                        help="omit inputs matching regex instead")
    parser.add_argument('--deliminator', default=r'\s+', metavar='regex',
                        help='Deliminator used to tokenize input. Default is the regex "\s+".')
    parser.add_argument('-n', '--max-args', default=0, type=int, action='store', dest='max_args',
                        help='Use at most max-args arguments per command line.')

    parser.add_argument('-g', '--go', action = 'store_true', default = False,
                        help = 'Should jobs be submitted? If this flag is not supplied, program will be a dry run. '
                               'Required system resources will be printed but jobs will not be submitted.')

    parser.add_argument('-v', '--verbose', action = 'store_true', default = False,
                        help = 'Verbose output.')

    parser.add_argument('-j', '--nJob', type=int, default = 1,
                        help='Specify number of jobs to split into.')

    parser.add_argument('-p', '--ppn', default=4, type=int,
                        help='Number of processors to allocate per PBS job. Default is 4.')

    parser.add_argument('-m', '--mem', default=None, type = int,
                        help = 'Amount of memory to allocate per PBS job in gb. '
                               'Default is 4 times the number of processors per job.')

    parser.add_argument('-w', '--walltime', default='12:00:00',
                        help = 'Walltime per job in the format hh:mm:ss. Default is 12:00:00.')

    parser.add_argument('--debug', choices=['none', 'pdb', 'pudb'], default='none',
                        help='Start the main method in the selected debugger.')

    args = parser.parse_args()

    if args.debug != 'none':
        assert(args.debug in ['pdb', 'pudb'])
        if args.debug == 'pdb':
            import pdb as db
        elif args.debug == 'pudb':
            try:
                import pudb as db
            except ModuleNotFoundError as e:
                sys.stderr.write('pudb is not installed.')
                return -1
        db.set_trace()

    process_args(args)

    # nFiles = len(args.raw_files)
    # fileLists = getFileLists(args.nJob, args.raw_files)

    # wd = os.getcwd()
    # ppn = args.ppn
    # mem = args.mem
    # if mem is None:
    #     mem = int(4 * ppn)

    # #print summary of resources needed
    # sys.stdout.write('\nRequested {} job{} with {} processor{} and {} gb memory each...\n'.format(args.nJob,
    #                                                                                               getPlurality(args.nJob),
    #                                                                                               args.ppn,
    #                                                                                               getPlurality(args.ppn),
    #                                                                                               mem))
    # # check that requested memory is valid
    # if mem > 180 or mem < 1:
    #     sys.stderr.write('{} is an invalid ammount of job memory!\nExiting...\n'.format(mem))
    #     exit()

    # sys.stdout.write('\t{} raw file{}\n'.format(nFiles, getPlurality(nFiles)))
    # if nFiles == 0:
    #     sys.stderr.write('No raw files specified!\nExiting...\n')
    #     exit()

    # filesPerJob = max([len(x) for x in fileLists])
    # sys.stdout.write('\t{} job{} needed\n'.format(len(fileLists), getPlurality(len(fileLists))))
    # sys.stdout.write('\t{} file{} per job\n'.format(filesPerJob, getPlurality(filesPerJob)))

    # if filesPerJob < ppn:
    #     ppn = filesPerJob
    # sys.stdout.write('\t{} processor{} per job\n'.format(ppn, getPlurality(ppn)))
    # filesPerProcess = int(ceil(float(filesPerJob) / float(ppn)))
    # sys.stdout.write('\t{} file{} per process\n'.format(ceil(filesPerProcess), getPlurality(filesPerProcess)))

    # for i, fileList in enumerate(fileLists):
    #     pbsName = makePBS(mem, args.ppn, args.walltime, wd, fileList)
    #     command = 'qsub {}'.format(pbsName)
    #     if args.verbose:
    #         sys.stdout.write('{}\n'.format(command))
    #     if args.go:
    #         proc = subprocess.Popen([command], cwd=wd, shell=True)
    #         proc.wait()

if __name__ == '__main__':
    main()

