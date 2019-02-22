import time
import logging
import os
import sys
from configobj import ConfigObj
import subprocess
import shutil
import socket
import re
import gzip
#import pysam
import numpy
#==============================

def get_host_name():
    """
    Return machine hostname
    @return:
    """
    return socket.gethostname()
    pass


def move_files(src, dest, overwrite=True):
    """
    Move all files with src to dest
    """
    #print src, dest
    if os.path.isdir(src):
        for f in os.listdir(src):
            print os.path.join(src, f)
            if os.path.isfile(os.path.join(src, f)) and file_exists(os.path.join(dest, f)) and overwrite:
                os.remove(os.path.join(dest, f))
            if os.path.isdir(os.path.join(src, f)) and dir_exists(os.path.join(dest, f)) and overwrite:
                shutil.rmtree(os.path.join(dest, f))
            shutil.move(os.path.join(src, f), dest)

        #Cleanup src
        if 'tmp' in os.path.dirname(src) and dir_exists(src):
            print 'Removing directory {0}'.format(src)
            try:
                os.removedirs(src)
            except OSError:
                print 'Could not remove directory {0} on machine {1}'.format(src, get_host_name())

    elif os.path.isfile(src):
        if os.path.isfile(src) and file_exists(os.path.join(dest)) and overwrite:
            print 'Removing destination file {0}'.format(dest)
            os.remove(dest)
        shutil.move(src, dest)

        #Cleanup src
        if 'tmp' in os.path.dirname(src) and file_exists(src):
            print 'Removing file {0}'.format(src)
            try:
                shutil.rmtree(os.path.dirname(src))
            except OSError:
                print 'Could not remove {0}.'.format(os.path.dirname(src))
    pass


def error(e, sysexit=True):
    """
    print given error and exit
    """
    print '[{0}] SYNAPSE_ERROR: {1}.'.format(ftime(), e)
    if sysexit: sys.exit(1)


def ftime():
    """
    Formatted time.
    @return: formatted time
    """
    return time.strftime('%d %b %Y %H:%M:%S')


def file_exists(fname, ignorefilesize=False):
    """Check if a file exists and is non-empty.
    """
    # Remove prefixes if any
    if ':' in fname:
        fname = fname.split(':')[1]

    if ignorefilesize: return os.path.exists(fname)
    return os.path.exists(fname) and os.path.getsize(fname) > 0


def dir_exists(dname):
    """Check if a directory exists and is non-empty.
    """
    return os.path.exists(dname)


def path_writeable(path):
    """Check if a path is writeable
    """
    if path == '': path = '.'
    return os.access(path, os.W_OK)


def get_config(config_file):
    """
    Read global config file
    """
    if not file_exists(config_file):
        print 'config file {0} not found'.format(config_file)
        sys.exit(1)
    config = ConfigObj(config_file)
    return config


def _parse_opts(o, config, argloc='stem', splitarg=None):
    """
    Match options with global config
    """
    prefix = ''
    #o1 = o.translate(None, '{}')
    o1 = re.findall('{(\S+)}', o)[0]
    if splitarg is not None:
        o1_split = o1.split(splitarg)[1]
        prefix = o1.split(splitarg)[0]
        o1 = o1_split
    o2 = config['Global_Parameters'][o1]
    #if argloc == 'body' and o == '{MAXMEM}':
    #    o2 = o2.replace('g', '_GB_MEMORY')
    o2 = o.replace('{'+ o1 +'}', o2)
    return prefix + o2


def launch_cmd(obj, tool=None, cmd=None, returnerr=True, sysexit=False, checkerror=True, outfile=None):
    """
    Command helper
    """
    if tool is None or cmd is None:
        error('Error in launching command.', sysexit=True)
    obj.log.info('START {0}'.format(tool))
    obj.log.info(cmd)
    if returnerr and outfile is None:
        o, e = execute_job(cmd, return_error=returnerr, sysexit=sysexit, outfile=None)
    else:
        o = execute_job(cmd, return_error=returnerr, sysexit=sysexit, outfile=outfile)
    if checkerror:
        if o is not None and 'error' in o:
            obj.cleanup_files()
            error('Process {0} returned with exit code 1.'.format(tool))
    obj.log.info('END {0}'.format(tool))
    pass


def get_tool(tool, global_config, **kwargs):
    """
    Contruct tool command line
    """
    if global_config is None:
        raise ValueError('Must provide global_config argument')

    if isinstance(global_config, basestring):
        global_config = get_config(global_config)
    elif not isinstance(global_config, ConfigObj):
        error('global_config parameter must be either basestring or ConfigObj', sysexit=True)

    modifiers = ['stem', 'constructor', 'sticky', 'argprefix','app', 'splitarg', 'optslast']
    args = global_config[tool]
    stem = global_config[tool]['stem']

    optslast = False
    if 'optslast' in args and args['optslast'] == 'TRUE':
        optslast = True

    splitarg = None
    if 'splitarg' in global_config[tool]:
        splitarg = global_config[tool]['splitarg']

    # #Update stem from apps section
    # if global_config['apps'][tool] != stem:
    #     stem = global_config['apps'][tool]

    cargs = list(set(args).difference(modifiers))
    xargs = filter(lambda x: 'XARG' in x, cargs)
    repeatargs = filter(lambda x: 'RPARGS' in x, cargs)
    ordargs = filter(lambda x: 'ordered_' in x, cargs)

    if not optslast:
        reg_args = filter(lambda x: 'XARG' not in x and 'ordered_' not in x and 'RPARGS' not in x, cargs)
        last_args = []
    else:
        reg_args = []
        last_args = filter(lambda x: 'XARG' not in x and 'ordered_' not in x and 'RPARGS' not in x, cargs)

    # Replace arguments in stem
    if '{MAXMEM}' in stem:
        stem = stem.replace('{MAXMEM}', _parse_opts('{MAXMEM}', global_config, argloc='stem'))
    if '{JAVA_TMPDIR}' in stem:
        stem = stem.replace('{JAVA_TMPDIR}', _parse_opts('{JAVA_TMPDIR}', global_config, argloc='stem'))

    # Construct command line
    if '{{' in stem:
        o1 = re.findall('{{(\S+)}}', stem)
        for t in o1:
            try:
                toolname = global_config['all_tools'][t]
            except KeyError:
                error('Tool {0} does not exist in config file'.format(t), sysexit=True)
            stem = stem.replace('{{'+t+'}}', toolname)

    cmd = [stem]
    if len(stem.split()) > 0:
        stem = stem.split()
        cmd = stem

    # Regular arguments hard coded with numerical values.
    if len(reg_args) > 0:
        for r in reg_args:
            opt0 = global_config[tool][r]
            if isinstance(opt0, list):
                opt = ','.join(opt0)
            else:
                opt = opt0
            if opt == 'keep':
                opt = ''
            elif opt == 'omit':
                continue
            usesplitarg = None
            if splitarg is not None and splitarg in opt: usesplitarg = splitarg
            if opt.startswith('{') or '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if isinstance(opt0, list) and '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if 'sticky' not in args:
                cmd.extend([args['argprefix'] + r, opt])
            else:
                cmd.extend([args['argprefix'] + r + '=' + opt])

    # Repeat arguments
    if len(repeatargs) > 0:
        for r in repeatargs:
            opt0 = global_config[tool][r]
            if isinstance(opt0, list):
                opt = ','.join(opt0)
            else:
                opt = opt0
            usesplitarg = None
            if splitarg is not None and splitarg in opt: usesplitarg = splitarg
            if opt.startswith('{') or '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if isinstance(opt0, list) and '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)

            xg, command_arg = r.split('_')
            if 'sticky' not in args:
                cmd.extend([args['argprefix'] + command_arg, opt])
            else:
                cmd.extend([args['argprefix'] + command_arg + '=' + opt])

    # Ordered arguments
    if len(ordargs) > 0:
        for r in sorted(ordargs):
            o1, o2, o3 = r.split('_')
            opt0 = global_config[tool][r]
            if isinstance(opt0, list):
                opt = ','.join(opt0)
            else:
                opt = opt0
            if opt == 'keep':
                opt = ''
            elif opt == 'omit':
                continue
            usesplitarg = None
            if splitarg is not None and splitarg in opt: usesplitarg = splitarg
            if opt.startswith('{') or '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if isinstance(opt0, list) and '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if 'sticky' not in args:
                cmd.extend([o3, opt])
            else:
                cmd.extend([o3+'='+opt])

    #Xargs are eXternal arguments that have format xargs1_carg_input. xargs1 indicates the order in which they appear
    # in the command line. carg indicates the argument name and input indicates the value being passed

    for x in sorted(xargs):
        if len(x.split('_')) == 3:
            xg, command_arg, command_input = x.split('_')
        else: # e.g. XARG1_PER_TARGET_COVERAGE_tcov
            x1 = x.split('_')
            xg = x1[0]
            command_input = x1[-1]
            command_arg = '_'.join(x1[1:-1])
        if command_arg != 'NOINP':
            if command_input not in kwargs.keys() and 'opt' not in command_input:
                raise KeyError('Argument {0} not found'.format(command_input))
            if 'opt' in command_input and command_input.replace('-opt', '') not in kwargs.keys():
                continue
            if 'sticky' not in args:
                cmd.extend([command_arg, kwargs[command_input.replace('-opt', '')]])
            else:
                cmd.extend(['{0}={1}'.format(command_arg, kwargs[command_input.replace('-opt','')])])
        else:
            if 'sticky' not in args:
                opt = global_config[tool][x]
                if opt.startswith('{') or '{' in opt:
                    opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
                cmd.extend([command_input, opt])
            else:
                cmd.extend(['{0}={1}'.format(command_input, opt)])

    # If optslast, add regular arguments last
    if len(last_args) > 0:
        for r in last_args:
            opt0 = global_config[tool][r]
            if isinstance(opt0, list):
                opt = ','.join(opt0)
            else:
                opt = opt0
            if opt == 'keep':
                opt = ''
            elif opt == 'omit':
                continue
            usesplitarg = None
            if splitarg is not None and splitarg in opt: usesplitarg = splitarg
            if opt.startswith('{') or '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if isinstance(opt0, list) and '{' in opt:
                opt = _parse_opts(opt, global_config, argloc='body', splitarg=usesplitarg)
            if 'sticky' not in args:
                cmd.extend([args['argprefix'] + r, opt])
            else:
                cmd.extend([args['argprefix'] + r + '=' + opt])


    cmd = [item for item in cmd if item is not None]

    # Replacement tags
    #print cmd
    outcmd = ' '.join(cmd)
    outcmd = outcmd.replace(' EQUAL', '=') # GATK
    outcmd = outcmd.replace(' NOSPACE', '') # GATK
    outcmd = outcmd.replace('\\"', '"')     # GATK Variant Filtration


    return outcmd
    pass


def convert_vcf_to_oncotator(vcf, ofile, sname=None):
    """
    Convert vcf file to oncotator format
    :param vcf:
    :param ofile:
    :return:
    """
    if not file_exists(vcf):
        error('File not found: {0}'.format(vcf), sysexit=True)

    with open(ofile, 'w') as ofh:
        with open(vcf) as vfh:
            vls = vfh.readlines()
            vli = [item.rstrip('\n') for item in vls if not item.startswith('#')]
            for v in vli:
                vspli = v.split('\t')
                ofh.write('{0}\t{1}\t{1}\t{2}\t{3}\t{4}\n'.format(vspli[0], vspli[1],
                                                                vspli[3], vspli[4], sname))
    pass


def get_fastqc_data_section(fastqdir, section_name):
    """
    http://pydoc.net/Python/bcbio-nextgen/0.2/bcbio.pipeline.qcsummary/
    """
    out = []
    in_section = False
    data_file = os.path.join(fastqdir, "fastqc_data.txt")
    if os.path.exists(data_file):
        with open(data_file) as in_handle:
            for line in in_handle:
                if line.startswith(">>%s" % section_name):
                    in_section = True
                elif in_section:
                    if line.startswith(">>END"):
                        break
                    out.append(line.rstrip("\r\n"))
    return out


def subset_fastq(file, outfile, nreads=1000):
    """
    Subset Fastq file and get first nreads
    :param file:
    :param nreads:
    :return:
    """
    if not file_exists(file):
        error('Could not open file {0}'.format(file))

    if file.endswith('.gz'):
        infile = gzip.open(file)
    else:
        infile = open(file)

    c = 0
    with open(outfile, 'w') as ofh:
        for l in infile:
            if c < int(nreads):
                ofh.write(l)
            else:
                break
            c += 1

    pass


def get_read_length(file):
    """
    Get read length from first read
    :param file:
    :param nreads:
    :return:
    """
    if not file_exists(file):
        error('Could not open file {0}'.format(file))

    if file.endswith('.gz'):
        infile = gzip.open(file)
    else:
        infile = open(file)
    c = 0
    for l in infile:
        if c == 1:
            read = len(l.rstrip('\n'))
        else:
            c = c+1

    return read
    pass


def read_picard_metrics_file(file, tag="## METRICS"):
    """
    Read picard style metrics file and report results
    """
    if not file_exists(file):
        error('File {0} not found/not readable.'.format(file))

    outdict = {}
    with open(file) as fh:
        fhls = [item.rstrip('\n') for item in fh.readlines()]
        if not any(map(lambda x: tag in x, fhls)):
            return outdict
        tag_ids = int([fhls.index(item) for item in fhls if item.startswith(tag)][0])
        header = fhls[tag_ids + 1].split('\t')
        values = fhls[tag_ids + 2].split('\t')

    for tup in zip(header, values):
        k, v = tup
        outdict.update({k: v})
    return outdict
    pass


def generate_symlinks(link, target):
    """
    Generate symlinks
    @param link:
    @param target:
    @return:
    """
    cmd = 'ln -s {0} {1}'.format(target, link)
    if not os.path.exists(link):
        o = execute_job(cmd)
    pass


def merge_config(global_config, instance_config):
    """
    Merge global config parameters with instance parameters. In case of conflict, instance parameters are retained.
    """
    if not file_exists(global_config): raise IOError('{0} not found'.format(global_config))
    if not file_exists(instance_config): raise IOError('{0} not found'.format(instance_config))

    gc = get_config(global_config)
    ic = get_config(instance_config)

    # Merge global config with instance config
    gc.merge(ic)
    return gc


def format_star_junction_file(infile, outfile):
    """
    Formats STAR junction output to create junction db file for genome rebuild
    awk 'BEGIN {OFS="\t"; strChar[0]="."; strChar[1]="+"; strChar[2]="-";} {if($5>0){print $1,$2,$3,strChar[$4]}}'
    https://code.google.com/p/rna-star/issues/detail?id=7
    :return:
    """
    if not file_exists(infile): raise IOError('{0} not found'.format(infile))
    strli = ['.', '+', '-']
    with open(infile) as ifh:
        inls = ifh.readlines()
    with open(outfile, 'w') as ofh:
        for inl in inls:
            inli = inl.rstrip('\n').split('\t')
            if inli[4] > 0:
                strchar = strli[int(inli[3])]
                ofh.write('{0}\t{1}\t{2}\t{3}\n'.format(inli[0], inli[1], inli[2], strchar))
    pass


def scan_file(filename=None, text=None, returnlogical=True):
    """
    Scans file for presence of text and returns logical reply
    :param file:
    :param text:
    :return:
    """
    with open(filename) as fh:
        lc = 1
        for l in fh.readlines():
            if text in l.rstrip('\n'):
                if returnlogical:
                    return True
                    break
                else:
                    return lc
            lc += 1
        return False

        pass


def setup_logger(logfile, name=__name__, formattext='%(asctime)s:%(module)s_%(levelname)s:%(message)s'):
    """
    Set up logging instance
    Returns:Logging instance
    """
    if not file_exists(logfile):
        if dir_exists(os.path.dirname(logfile)):
            open(logfile, 'w').close()
        else:
            error('Parent directory {0} does not exist'.format(os.path.dirname(logfile)), sysexit=True)
    logging.basicConfig(filename=logfile, format=formattext, level=logging.DEBUG,
                        datefmt='[%m-%d-%Y %H:%M:%S]')
    logger = logging.getLogger(name)
    handle = logging.StreamHandler()
    handle.setLevel(logging.ERROR)
    logger.addHandler(handle)
    return logger

    pass


def create_dir(dirname, permissions=0775):
    """
    Create directories
    """
    parentdir = os.path.dirname(dirname)
    if not path_writeable(parentdir):
        error('Parent directory {0} not writeable'.format(parentdir), sysexit=True)

    if dir_exists(dirname):
        if not path_writeable(dirname):
            error('Directory {0} exists, but not writeable'.format(dirname), sysexit=True)
    else:
        os.makedirs(dirname, permissions)
    pass


def print_or_log(msg, logger=None):
    """
    """
    if logger is None:
        print msg
    elif logger is not None:
        try:
            logger.error(msg)
        except AttributeError:
            raise AttributeError('logger is not properly defined')


def execute_job(cmd, flag_shell=False, outfile=None, logger=None, return_error=False, sysexit=True):
    """
    Run subprocess job
    """
    o = ''
    e = ''
    if isinstance(cmd, list):
        flag_shell = False
    elif isinstance(cmd, str):
        flag_shell = True
    else:
        error('Argument cmd to execute_job must be either string or list')

    if outfile is not None:
        if return_error:
            with(open(outfile, 'w')) as out:
                proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=out, shell=flag_shell, bufsize=4096)
        else:
            with(open(outfile, 'w')) as out:
                proc = subprocess.Popen(cmd, stderr=open(os.devnull, 'w'), stdout=out, shell=flag_shell, bufsize=4096)

    else:
        if return_error:
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=flag_shell, bufsize=4096)
        else:
            proc = subprocess.Popen(cmd, stderr=open(os.devnull, 'w'), stdout=subprocess.PIPE, shell=flag_shell, bufsize=4096)

    #print proc.pid
    exitcode = proc.wait()
    o, e = proc.communicate()

    if exitcode != 0:
        if isinstance(cmd, list): cmd = ' '.join(cmd)
        print_or_log('[{0}] Process {1} returned with exit code {2}'.format(ftime(), cmd, exitcode), logger)
        if sysexit:
            sys.exit(exitcode)
        else:
            if return_error:
                print e
                return ['error_{0}'.format(exitcode), e]
            else:
                print e
                return 'error_{0}'.format(exitcode)
    else:
        if return_error:
            return o, e
        else:
            return o
    pass



def get_approximate_read_number(f):
    """
    Check file size
    @param f:
    @return:
    """
    # Check for files with "normal:" or "tumor:" prefix
    if ':' in f:
        f = f.split(':')[1]

    if not file_exists(f):
        error('File {0} not found.'.format(f), sysexit=True)

    size = os.path.getsize(f)
    if not f.endswith('.gz'):
        size /= 3.5

    nreads = size/75
    return int(nreads)
    pass


def check_sge_functions():
        """
        check_sge_functions : checks that qsub and qstat functions are accessible on this platform.
        """

        flag_not_found = 0
        # . Look for qsub :
        buf = os.popen('which qsub').read().rstrip('\n')
        qsubpath = buf
        if 'which: no qsub' in buf:
            flag_not_found = 1

        # . Look for qstat :
        buf = os.popen('which qstat').read().rstrip('\n')
        qstatpath = buf
        if 'which: no qstat' in buf:
            flag_not_found = 1

        # . Package final message :
        if flag_not_found == 0:
            print 'qsub, qstat functions found'
        else:
            error('Could not find qsub, qstat')

        pass


def remove_contents(outdir, pattern=None, exceptionlist=None, getcmd=False):
    """
    Remove contents of a directory
    """
    if outdir == '~':
        print 'You must be joking, right?'
        return

    if getcmd:
        return 0

    if exceptionlist is not None:
        if isinstance(exceptionlist, basestring): exceptionlist = [exceptionlist]
    else:
        exceptionlist = []

    if pattern is None:
        pattern = ''

    for root, dirs, files in os.walk(outdir, topdown=False):
        for name in files:
            fname = os.path.join(root, name)
            if pattern in fname and fname not in exceptionlist and os.path.basename(fname) not in exceptionlist:
                print 'Removing file {0}'.format(fname)
                os.remove(fname)
        for name in dirs:
            dname = os.path.join(root, name)
            if pattern in dname and dname not in exceptionlist and os.path.basename(dname) not in exceptionlist \
                    and not os.path.islink(dname):
                print 'Removing directory {0}'.format(dname)
                os.rmdir(dname)


def check_file_manifest(manifest, directory=None, prefix=False):
    """
    Check presence of files in directory
    @param m:
    @return:
    """
    if manifest is None: error('No manifest provided for check_file_manifest')
    if directory is None: directory = os.path.curdir()

    if not isinstance(manifest, list): manifest = [manifest]

    missing_files = []
    nmissing = 0
    if not prefix:
        for f in manifest:
            f = os.path.basename(f)
            path = os.path.join(directory, f)
            if not file_exists(path):
                missing_files.append(path)
        nmissing = len(missing_files)
        return missing_files, nmissing
    else:
        files = os.listdir(directory)
        for f in manifest:
            f = os.path.basename(f)
            f = '.'.join(f.split('.')[:-1])
            if not any([f in item for item in files]):
                missing_files.append(f)
        nmissing = len(missing_files)
        return missing_files, nmissing


def check_log_file(tool, logfile, starttag='START', endtag='END', phrase=None):
    """
    Check logfile for tool run status
    @param logfile:
    @param starttag:
    @param endtag:
    @return:
    """
    out = False
    if tool is None: error('Tool name must be provided')
    if logfile is None: error('Log file must be provided')
    if not file_exists(logfile): error('Log file {0} cannot be opened'.format(logfile))

    if phrase is not None:
        with open(logfile) as lfh:
            lflines = lfh.readlines()[:-1] # read in reverse
            for lfl in lflines:
                lfl = lfl.rstrip('\n')
                if phrase in lfl:
                    out = True
    else:
        stag = '{0} {1}'.format(starttag, tool).rstrip()
        etag = '{0} {1}'.format(endtag, tool).rstrip()

        toolend = False
        toolstart = False

        with open(logfile) as lfh:
            lflines = reversed(lfh.readlines())  # read in reverse
            for lfl in lflines:
                lfl = lfl.rstrip('\n')
                if etag in lfl:
                    toolend = True
                if toolend and stag in lfl:
                    toolstart = True
                if toolstart and toolend:
                    out = True
    return out

    pass


def uncompress_file(src, dest, getcmd=False, targz=False, tar=False):
    """
    Run gunzip uncompression src.gz --> dest
    @param src:
    @param dest:
    @return:
    """
    if getcmd:
        return ' '.join(['gunzip', src, '-c'])

    if not file_exists(src):
        error('File {0} does not exist'.format(src), sysexit=True)

    if not path_writeable(os.path.dirname(dest)):
        error('Path {0} not writeable'.format(os.path.dirname(dest)), sysexit=True)

    if targz:
        cmd = ['tar', 'xvzf', src, '-C', dest]
        o = execute_job(cmd)
    elif tar:
        cmd = ['tar', 'xvf', src, '-C', dest]
        o = execute_job(cmd)
    else:
        cmd = ['gunzip', src, '-c']
        o = execute_job(cmd, outfile=dest)
    return cmd
    pass


def chomp(li):
    """
    Remove \n from all elements in a list
    @param li:
    @return:
    """
    if not isinstance(li, list):
        error('from chomp. Not a list')

    li = map(lambda s: s.strip(), li)
    return li

    pass


def count_reads(f):
    """
    Count number of fastq reads (number of total lines divided by 4)
    @param f:
    @return:
    """
    if not file_exists(f):
        error('File {0} not found or accessible', sysexit=True)

    if f.endswith('.gz'):
        cmd = 'zcat {0} | wc -l'.format(f)
        o = execute_job(cmd, flag_shell=True)
        return int(o)/4
    elif f.endswith('.fq') or f.endswith('.fastq'):
        cmd = 'wc -l {0}'.format(f)
        o = execute_job(cmd, flag_shell=True)
        return int(o)/4

    pass


def convert_xenomeformat_to_fastq(src, dest, getcmd=False):
    """
    Convert xenome formatted fastq file to regular fastq files
    @return:
    """

    if getcmd:
        return 0

    if not file_exists(src):
        error('File {0} does not exist'.format(src), sysexit=True)

    if not path_writeable(os.path.dirname(dest)):
        error('Path {0} not writeable'.format(os.path.dirname(dest)), sysexit=True)

    cmd = ['awk']
    cmd.extend(['\'{if (NR % 4 == 1) print \"\\@\"$0}; '
                '{if (NR % 4 == 2) print $0} { if(NR % 4 == 3) print "+"}; { if(NR % 4 == 0) print $0}\''])
    cmd.extend([src])
    cmd.extend(['>', dest])
    cmd = ' '.join(cmd)
    print cmd
    o = execute_job(cmd, flag_shell=True)
    pass


def complement(s):
    basecomplement = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A', 'N':'N'}
    letters = list(s)
    letters = [basecomplement[base] for base in letters]
    return ''.join(letters)


def revcom(s):
    return complement(s[::-1])


def readfq(fp): # this is a generator function
    """
    Source https://github.com/lh3/readfq/blob/master/readfq.py
    @param fp:
    @return:
    """
    last = None # this is a buffer keeping the last unprocessed line
    while True: # mimic closure; is it a bad idea?
        if not last: # the first record or a record following a fastq
            for l in fp: # search for the start of the next record
                if l[0] in '>@': # fasta/q header line
                    last = l[:-1] # save this line
                    break
        if not last: break
        name, seqs, last = last[1:].partition(" ")[0], [], None
        for l in fp: # read the sequence
            if l[0] in '@+>':
                last = l[:-1]
                break
            seqs.append(l[:-1])
        if not last or last[0] != '+': # this is a fasta record
            yield name, ''.join(seqs), None # yield a fasta record
            if not last: break
        else: # this is a fastq record
            seq, leng, seqs = ''.join(seqs), 0, []
            for l in fp: # read the quality
                seqs.append(l[:-1])
                leng += len(l) - 1
                if leng >= len(seq): # have read enough quality
                    last = None
                    yield name, seq, ''.join(seqs) # yield a fastq record
                    break
            if last: # reach EOF before reading enough quality
                yield name, seq, None # yield a fasta record instead
                break


class ArgClass:
    def __init__(self, adict):
        self.__dict__.update(adict)


def softclip_cigar(tuplist=[(0,1)], hf=0, hr=0, lseq=0, queryname=''):
    """
    Depends on PySam module
    :param tuplist: List of CIGAR tuples from pySam read
    :param hf: forward overhang
    :param hr: reverse overhang
    :return: hf, hr, modified cigar string
    """
    seqs = range(1, lseq)
    cigs = []
    snum = 1
    for i in tuplist:
        cigtype, cignum = i
        if cigtype == 0:
            cigs.extend(map(str, range(snum, snum + cignum)))
            snum += cignum
        elif cigtype == 4:
            cigs.extend('S'*cignum)
        elif cigtype == 1:
            cigs.extend('I'*cignum)
            #snum += cignum
        elif cigtype == 2:
            cigs.extend('D'*cignum)
            snum += cignum
        elif cigtype == 5:
            cigs.extend('H'*cignum)

    # Trimming of H from 3'end does not affect overlap at 3; end
    while cigs[-1] == 'H':
        cigs.pop(-1)

    while cigs[0] == 'H':
        cigs.pop(0)

    IcountF = 0
    DcountF = 0
    IcountR = 0
    DcountR = 0

    if hf > 0:
        hfc = 0
        for i in range(0, len(cigs)):
            if cigs[i] == 'I':
                IcountF += 1
            elif cigs[i] == 'D':
                DcountF += 1
            elif cigs[i].isdigit() or cigs[i] == 'S':
                hfc += 1
            if hfc == hf:
                break

        if IcountF > 0 and DcountF > 0:
            cigs = cigs[(hf+IcountF+DcountF):]
        elif IcountF > 0:
            cigs = cigs[(hf+IcountF):]
        elif DcountF > 0:
            cigs = cigs[(hf+DcountF):]
        else:
            cigs = cigs[hf:]

    #hr -= IcountF
    if hr > 0:
        hrc = 0
        for i in reversed(range(0, len(cigs))):
            if cigs[i] == 'I':
                IcountR += 1
            elif cigs[i] == 'D':
                DcountR += 1
            elif cigs[i].isdigit() or cigs[i] == 'S':
                hrc += 1
            if hrc == hr:
                break

        if IcountR > 0 and DcountR > 0:
            cigs = cigs[:len(cigs)-hr-IcountR-DcountR]
        elif IcountR > 0:
            cigs = cigs[:len(cigs)-hr-IcountR]
        elif DcountR > 0:
            cigs = cigs[:len(cigs)-hr-DcountR]
        else:
            cigs = cigs[:len(cigs)-hr]

    try:
        while cigs[0] == 'S' or cigs[0] == 'H':
            cigs.pop(0)
            hf += 1
        while cigs[-1] == 'S' or cigs[-1] == 'H':
            cigs.pop(-1)
            hr += 1
    except IndexError:
        return None, None, None, None

    if len(cigs) == 1:
        return None, None, None, None

    def get_subtype(item):
        if item.isdigit():
            ift = 0
        elif item == 'I':
            ift = 1
        elif item == 'D':
            ift = 2
        elif item == 'S':
            ift = 4
        elif item == 'H':
            ift = 5
        return ift

    ocig = []
    cigc = 1
    litem = ''
    Ccount = 1
    oldft = ''
    for item in cigs:
        ft = get_subtype(item)
        if ft == oldft:
            Ccount += 1
        else:
            if cigc > 1:
                ocig.append((oldft, Ccount))
            Ccount = 1

        if cigc == len(cigs):
            ocig.append((ft, Ccount))
        oldft = ft
        cigc += 1

    if hf > 0: ocig.insert(0, (4, hf+IcountF))
    if hr > 0: ocig.append((4, hr+IcountR))
    return hf, hr, ocig, sum(item[1] for item in ocig if item[0] != 2)
    pass


def zip_lists(l1=['a','c','e'], l2=['b','d']):
    """
    Merge two lists in a special case
    :param l1: [a,c,e]
    :param l2: [b,d] # shorter than l1 by 1 element
    :return: combined list [a,b,c,d,e]
    """
    if len(l1) - len(l2) != 1:
        print 'Error in list lengths'
    else:
        newli = [str(l1[0])]
        for k in range(1, len(l1)):
            newli.append(str(l2[k-1]))
            newli.append(str(l1[k]))
    return newli


def count_dot(s='..'):
    """
    Count number of dots in string
    :param s: string
    :return: integer number
    """
    if s.count('.') > 0:
        return str(s.count('.'))
    else:
        return s


def parse_mdtag(nm=['1'], md=['101'], hf=0, hr=0):
    """
    Parse NM and MD tags from pySam read
    :param nm: NM tag. List of length 1 with integer
    :param md: MD tag. List of length 1 with string
    :param hf: Forward overhang
    :param hr: Reverse overhang
    :return:
    """

    nmout = ''
    mdout = ''
    if nm[0] == 0 and not md[0].isdigit:
        raise ValueError('NM and MD tag do not match')

    if hf == 0 and hr == 0:
        return nm, md
    else:
        mds = md[0]
        nms = nm[0]
        matches = re.split(r'[ACGT\^]+', mds)
        mismatches0 = re.split(r'\d+', mds)
        mismatches = [item for item in mismatches0 if item != '']

        mdstring = ''
        if len(mismatches) == 0:
            mdstring = '.' * int(matches[0])
        else:
            mdstringli = zip_lists(matches, mismatches)
            for k in range(0, len(mdstringli)):
                if mdstringli[k].isdigit():
                    mdstring += '.' * int(mdstringli[k])
                else:
                    mdstring += mdstringli[k]

        mdstring = mdstring.replace('^A', 'W').replace('^C', 'X').replace('^G', 'Y').replace('^T', 'Z')
        delcount = len(re.findall(r'[WXYZ]', mdstring[:hf]))
        mdout = mdstring[(hf+delcount):]

        if hr > 0:
            delcount = len(re.findall(r'[WXYZ]', mdout[-hr:]))
            mdout = mdout[:-(hr + delcount)]
        mdout = mdout.replace('W', '^A').replace('X', '^C').replace('Y', '^G').replace('Z', '^T')

        matches = re.split(r'[ACGT\^]+', mdout)
        mismatches0 = re.split(r'[\.]+', mdout)
        mismatches = [item for item in mismatches0 if item != '']
        if len(mismatches) == 0:
            nmout = [0]
            mdout = [str(len(matches[0]))]
        else:
            nmout = [len(mismatches)]
            mdout0 = zip_lists(matches, mismatches)
            mdout = map(count_dot, mdout0)

        return nmout, [''.join(mdout)]


def trim_bam_to_fastq(bamf, outprefix, paired=True):
    """
    Remove matched bases from primer aligned bam files and write fastq files
    :param samf:
    :param outprefix:
    :param paired:
    :return:
    """

    def count_left_match_in_cigar(cs):
        r = re.findall('^\d+M', cs)
        l = 0
        if len(r) > 0:
            l = int(r[0].strip('M'))
        return l
        pass

    def trim_matches_from_seq(ir1, ir2):
        """

        :param ir1:
        :param ir2:
        :return:
        """
        c1 = ir1[5]
        c2 = ir2[5]

        if c1 == '*' and c2 == '*':
            l1 = 0
            l2 = 0
        else:
            l1 = count_left_match_in_cigar(c1)
            l2 = count_left_match_in_cigar(c2)

        seq1 = ir1[9][l1:]
        qual1 = ir1[10][l1:]
        seq2 = ir2[9][l2:]
        qual2 = ir2[10][l2:]

        return seq1, qual1, seq2, qual2
        pass


    # Convert BAM to SAM
    samf = bamf.replace('.bam', '.sam')
    cmd = 'samtools view {0} -o {1}'.format(bamf, samf)
    execute_job(cmd, flag_shell=True, return_error=False)


    with open(samf) as sfh:
        sfhl = sfh.readlines()
        nr = len(sfhl)

        if paired:
            with open(outprefix+'_R1.fastq', 'w') as r1fh:
                with open(outprefix+'_R2.fastq', 'w') as r2fh:
                    for i in [item for item in xrange(nr) if item %2 == 0]:
                        r1 = sfhl[i].rstrip('\n').split('\t')
                        r2 = sfhl[i+1].rstrip('\n').split('\t')

                        s1, q1, s2, q2 = trim_matches_from_seq(r1, r2)
                        r1fh.write('@{0}\n{1}\n+\n{2}\n'.format(r1[0], s1, q1))
                        r2fh.write('@{0}\n{1}\n+\n{2}\n'.format(r2[0], s2, q2))

        else:
            with open(outprefix+'_R1.fastq', 'w') as r1fh:
                for i in xrange(nr):
                    r1 = sfhl[i].rstrip('\n').split('\t')
                    r2 = sfhl[i].rstrip('\n').split('\t')

                    s1, q1, s2, q2 = trim_matches_from_seq(r1, r2)
                    r1fh.write('@{0}\n{1}\n+\n{2}\n'.format(r1[0], s1, q1))

    pass


def softclip_amplicon(inbam, outbam, targetfile):
    #targetfile = '/site/ne/home/wings/ref_data/seq-amplicon/CHP2/CHP2_target_regions_hg19_20120806.interval_list'
    #bamfile = 'M004/bam/M004_mapped_sorted_md_rh.bam'

    bf = pysam.AlignmentFile(inbam, "rb")
    outbam_temp = outbam.replace('.bam', '_temp.bam')
    pairedreads = pysam.AlignmentFile(outbam_temp, "wb", template=bf)
    hardclipcount = 0
    with open(targetfile) as tfh:
        c = 0
        for tfhl in tfh.readlines():
            tfhl = tfhl.rstrip('\n')
            if tfhl.startswith('@'):
                continue
            else:
                chr, sreg, ereg = tfhl.split('\t')[0:3]
                sreg = int(sreg) - 1
                ereg = int(ereg) - 1
                lreg = ereg - sreg + 1
                for read in bf.fetch(chr, sreg, ereg):
                    c += 1

                    if read.is_unmapped:
                        pairedreads.write(read)
                        continue
                        
                    seq = read.seq
                    lseq = len(seq)
                    qual = read.qual
                    fwd = read.is_reverse == False
                    sseq = read.pos
                    eseq = sseq + lseq - 1
                    lovl = read.get_overlap(sreg, ereg+1)
                    lfclip = max(0, sreg - sseq)
                    lrclip = max(0, eseq - ereg)

                    if lfclip == 0 and lrclip == 0:
                        pairedreads.write(read)
                        continue

                    tag = read.get_tags()
                    nmtag = [item[1] for item in tag if item[0] == 'NM']
                    mdtag = [item[1] for item in tag if item[0] == 'MD']

                    cig = read.cigar
                    cigs = read.cigarstring

                    #ciglen = sum([item[1] for item in cig if item[0] not in [2,3,5]])
                    # if ciglen != lseq:
                    #     print 'Error', ciglen, lseq, read
                    #     sys.exit(1)

                    ohf, ohr, cigout, ciglen = softclip_cigar(cig, lfclip, lrclip, lseq, read.query_name)
                    if ciglen != lseq and ciglen is not None:
                        print 'Error from softclip amplicon'
                        print c, ohf, ohr, cig, cigout, ciglen, lseq, read.query_name
                        sys.exit(1)

                    if cigout is None:
                        hardclipcount += 1
                        continue

                    read.cigar = cigout
                    read.pos = int(read.pos) + int(ohf)
                    nmout, mdout = parse_mdtag(nmtag, mdtag, ohf, ohr)
                    read.set_tag('NM', int(nmout[0]), value_type = 'i', replace=True)
                    read.set_tag('MD', mdout[0], replace=True)

                    pairedreads.write(read)

    print '[{0}] SoftClip: Skipping {1} hard clipped reads'.format(ftime(), hardclipcount)
    print '[{0}] SoftClip: Processed {1} reads'.format(ftime(), c)
    pairedreads.close()
    pysam.sort(outbam_temp, outbam.replace('.bam', ''))
    bf.close()
    return None

