# pbs_do
Create PBS jobs from the standard input.

The inspiration for  `pbs_do` is the unix `xargs` command. `pbs_do` reads a list of arguments from the standard input, deliminated by blanks and creates zPBS file(s) to process each argument with a user specified `command`. Various options are available to specify the compute resources to request for each job as well as to manage parallel processing whithin jobs.

# Usage
```
usage: pbs_do [options] command [initial-arguments ...]

Create PBS jobs from the standard input.

positional arguments:
  command

optional arguments:
  -h, --help            show this help message and exit
  -a--arg-file file     Read items from file instead of standard input.
  -I replace-str        replace occurrences of replace-str in the
                        initial_arguments with input.
  --resub pattern repl  replace occurrences of replace-str in the input with
                        re.sub(patten, repl, input)
  -r regex              only build commands from inputs matching regex
  -o                    omit inputs matching regex instead
  --deliminator regex   Deliminator used to tokenize input. Default is the
                        regex "\s+".
  -n MAX_ARGS, --max-args MAX_ARGS
                        Use at most max-args arguments per command line.
  --noArgs              Don't read arguments from stdin. Just construct pbs
                        file from command.
  -f --dontCheck        Skip check that each argument is a file that exists.
  -g, --go              Should jobs be submitted? If this flag is not
                        supplied, program will be a dry run. Required system
                        resources will be printed but jobs will not be
                        submitted.
  --writeStdout         Write text file with stdout for each process?
  -v, --verbose         Verbose output.
  -j NJOB, --nJob NJOB  Specify number of jobs to split into.
  --shell SHELL         Specify the shell to use in PBS files. Default is the
                        value of $SHELL
  -P N_CHILD_PROC, --nProc N_CHILD_PROC
                        Number of child procecies to create in each PBS job.
                        Unless specified, this is the same as the value used
                        for ppn.
  -p PPN, --ppn PPN     Number of processors to request per PBS job. Default
                        is the smaller of 4 and the number of args.
  -m MEM, --mem MEM     Amount of memory to allocate per PBS job in gb.
                        Default is 4 times the number of processors per job.
  -w WALLTIME, --walltime WALLTIME
                        Walltime per job in the format hh:mm:ss. Default is
                        12:00:00.
  --debug {none,pdb,pudb}
                        Start the main method in the selected debugger.
```

# Examples

Read a list of links from `links.txt` and create a PBS job to retrieve them with `wget`.
```
$ cat links.txt | pbs_do -f wget
```
