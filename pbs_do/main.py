
import sys
import os
import argparse
import subprocess
from math import ceil
import re

PBS_COUNT = 0

def grouper(iterable, n):
    ''' Iterate through `iterable` in groups of `n` elements. '''
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def makePBS(command, initial_args, mem, ppn, walltime, wd, arg_list,
            nArgs=1, n_child_proc=None, replace_str=None, shell='/bin/tcsh',
            writeStdout=False, verbose=False, pbsName=None):
    '''
    Create PBS file for command.

    Parameters
    ----------
    command: str
        Command to execute.
    initial_args: str
        Initial arguments to supply to command.
    mem: int
        Memory to request in gb.
    ppn: int
        Processors per node.
    walltime: str
        Wall time in the format hh:mm:ss
    wd: str
        Parent directory for PBS file.
    arg_list: list
        List of arguments to supply to `command`
    nArgs: int
        Number of arguments to supply to each call to `command`
        Default is 1.
    n_child_proc: int
        Number of child procecies to spawn. If None, this is the same as `ppn`
    replace_str: str
        A String to replace with arguments in `initial_arguments` instead of
        appending to end of command. Default is None.
    shell: str
        Path to shell to use in PBS file. Default is /bin/tcsh
    writeStdout: bool
        Write stdout text file for each process? Default is False.
    verbose: bool
        Verbose output? Default is False'
    pbsName: str
        Basename for pbs files. Default is the command name.

    Returns
    -------
    pbsName: str
        Name of PBS file created.
    '''

    global PBS_COUNT
    pbsName = '{}_{}.pbs'.format(os.path.splitext(os.path.basename(command))[0] if pbsName is None else pbsName,
                                 PBS_COUNT)
    n_child_proc = ppn if n_child_proc is None else n_child_proc
    _arg_lists = getFileLists(n_child_proc, arg_list, check=False)

    if len(_arg_lists) < n_child_proc:
        n_child_proc = len(_arg_lists)

    with open(pbsName, 'w') as outF:
        outF.write('#!{}\n'.format(shell))
        outF.write('#PBS -l mem={}gb,nodes=1:ppn={},walltime={}\n\n'.format(mem, ppn, walltime))
        outF.write('cd {}\n'.format(wd))
        
        _command_sep = '' if initial_args == '' else ' '
        _command_new_line = '; ' if n_child_proc > 1 else '\n'
        for i, _arg_list in enumerate(_arg_lists):
            _commands = list()
            for arg_group in grouper(_arg_list, nArgs):
                _initial_args = initial_args
                args = ' '.join(arg_group)
                if replace_str:
                    _initial_args = _initial_args.replace(replace_str, args)
                    _commands.append('{} {}'.format(command, _initial_args))
                else:
                    _commands.append('{} {}{}{}'.format(command, _initial_args, _command_sep, args))
            _command = _command_new_line.join(_commands)
            if writeStdout:
                _command += ' > stdout_{}_{}.txt'.format(PBS_COUNT, i)
            _command += ' &\n' if n_child_proc > 1 else '\n'
            outF.write(_command)

            if verbose:
                sys.stdout.write(_command)

        if n_child_proc > 1:
            outF.write('wait\n')

    PBS_COUNT += 1
    return pbsName


def getPlurality(num):
    if num > 1:
        return 's'
    else: return ''


def getFileLists(nProc: int, arg_list: list, check=True) -> list:
    '''
    Get input file names and split into a list for each subprocess.

    Parameters
    ----------
    nProc: int
        Number of processes per job.
    arg_list: list
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
        for f in arg_list:
            if not os.path.exists(f):
                sys.stderr.write('{} does not exist.\n'.format(f))
                _exit = True
        if _exit:
            sys.exit(-1)

    #calculate number of files per thread
    nArgs = len(arg_list)
    filesPerProcess = nArgs // nProc
    if nArgs % nProc != 0:
        filesPerProcess += 1

    #split up arg_list
    ret = list()
    i = 0
    while(i < nArgs):
        # get beginning and end indecies
        begNum = i
        endNum = begNum + filesPerProcess
        if endNum > nArgs:
            endNum = nArgs

        ret.append(arg_list[begNum:endNum])
        i += filesPerProcess

    fileSet = set()
    for i in ret:
        for j in i:
            fileSet.add(j)
    if len(fileSet) != len(arg_list):
        raise RuntimeError('Non-unique args in input!')

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
                                     description = 'Create PBS jobs from the standard input.',
                                     usage='%(prog)s [options] command [initial-arguments ...]')

    parser.add_argument("command", action="store", type=str, nargs=argparse.REMAINDER)

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
    parser.add_argument('-n', '--max-args', default=1, type=int, action='store', dest='max_args',
                        help='Use at most max-args arguments per command line.')
    parser.add_argument('-f', '--dontCheck', action='store_false', default=True, dest='check_files',
                        help='Skip check that each argument is a file that exists.')
    parser.add_argument('--pbsName', default=None, type=str,
                        help='Basename for pbs files. Default is the command name.')

    parser.add_argument('-g', '--go', action = 'store_true', default = False,
                        help='Should jobs be submitted? If this flag is not supplied, program will be a dry run. '
                             'Required system resources will be printed but jobs will not be submitted.')

    parser.add_argument('--writeStdout', action = 'store_true', default=False,
                        help = 'Write text file with stdout for each process?')
    parser.add_argument('-v', '--verbose', action = 'store_true', default=False,
                        help = 'Verbose output.')

    parser.add_argument('-j', '--nJob', type=int, default=1,
                        help='Specify number of jobs to split into.')
    parser.add_argument('--shell', default=os.environ['SHELL'],
                        help='Specify the shell to use in PBS files. Default is the value of $SHELL')

    parser.add_argument('-P', '--nProc', default=None, type=int, dest='n_child_proc',
                        help='Number of child procecies to create in each PBS job. Unless specified, '
                             'this is the same as the value used for ppn.')
    parser.add_argument('-p', '--ppn', default=None, type=int,
                        help='Number of processors to request per PBS job. Default is the smaller of '
                             '4 and the number of args.')

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

    command_dict = process_args(args)

    nArgs = len(command_dict['args'])
    arg_lists = getFileLists(args.nJob, command_dict['args'], check=args.check_files)

    wd = os.getcwd()
    ppn = min(4, nArgs) if args.ppn is None else args.ppn
    n_child_proc = ppn if args.n_child_proc is None else args.n_child_proc
    mem = int(4 * ppn) if args.mem is None else args.mem

    #print summary of resources needed
    sys.stdout.write('\nRequested {} job{} with {} processor{} and {} gb memory each...\n'.format(args.nJob,
                                                                                                  getPlurality(args.nJob),
                                                                                                  ppn,
                                                                                                  getPlurality(ppn),
                                                                                                  mem))
    # check that requested memory is valid
    if mem > 180 or mem < 1:
        sys.stderr.write('{} is an invalid ammount of job memory!\nExiting...\n'.format(mem))
        sys.exit(-1)

    sys.stdout.write('\t{} argument{}\n'.format(nArgs, getPlurality(nArgs)))
    if nArgs == 0:
        sys.stderr.write('No arguments specified!\nExiting...\n')
        sys.exit(-1)

    argPerJob = max([len(x) for x in arg_lists])
    sys.stdout.write('\t{} job{}\n'.format(len(arg_lists), getPlurality(len(arg_lists))))
    sys.stdout.write('\t{} file{} per job\n'.format(argPerJob, getPlurality(argPerJob)))

    sys.stdout.write('\t{} processor{} per job\n'.format(ppn, getPlurality(ppn)))
    nPerProcess = float(argPerJob) / float(ppn)
    if nPerProcess > 1:
        firstS = 'argument'
        secondS = 'process'
    else:
        firstS = 'process'
        secondS = 'argument'
        nPerProcess = 1 / nPerProcess
    nPerProcess = int(ceil(nPerProcess))
    sys.stdout.write('\t{} {}{} per {}\n'.format(nPerProcess, firstS, getPlurality(nPerProcess), secondS))

    for i, arg_list in enumerate(arg_lists):
        pbsName = makePBS(command_dict['command'], command_dict['initial_arguments'],
                          mem, ppn, args.walltime, wd, arg_list,
                          nArgs=args.max_args, n_child_proc=n_child_proc, replace_str=args.replace_str,
                          writeStdout=args.writeStdout, verbose=args.verbose, pbsName=args.pbsName)
        command = 'qsub {}'.format(pbsName)
        if args.verbose:
            sys.stdout.write('{}\n'.format(command))
        if args.go:
            proc = subprocess.Popen([command], cwd=wd, shell=True)
            proc.wait()


if __name__ == '__main__':
    main()

