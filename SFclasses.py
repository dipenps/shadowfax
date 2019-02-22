import argparse
from collections import defaultdict
from ngsutils import *
import socket


class Herd:
    """
    Sequence Analysis Environment
    """
    def __init__(self, args=None, rootpath=None, pipelineset=None):
        """
        Initialize class by running basic methods
        """

        # Internal
        global PIPELINESET; PIPELINESET = pipelineset
        global CONFIGPATH; CONFIGPATH = rootpath + '/__file__/config'
        global SFPATH; SFPATH = rootpath+'/run_synapse.py'

        if not file_exists(SFPATH):
            error('File {0} not found'.format(SFPATH))
        if not dir_exists(CONFIGPATH):
            error('Config directory {0} not found'.format(os.path.abspath(CONFIGPATH)))

        # Get arguments
        if args is None:
            self.get_args()
        else:
            self.args = args

        # Parse config file
        self.parse_config()

        # Parse pipeline specific config
        self.parse_pipeline_parameters()

        # Check SGE environment
        check_sge_functions()

        # Check apps
        self.check_apps()

        # Check previous analysis and delete if necessary
        self.check_previous_analysis()

        # Create output directories and log files
        self.create_dir()

        # Parse fastq list
        self.parse_fastq_series()
        self.check_fastq_files_exist()

        # Create sample specific sub directories
        self.create_subdirs()

        # Create fastq lists for input
        self.create_fastq_list()

        # Prepare config file
        self.prepare_config_file()

        # Submit job
        self.submit_jobs()
        print 'Submitted jobs'

        pass

    #===================================================================================================================
    def check_apps(self):
        """
        Check if apps present
        """
        apps = self.conf['all_tools']
        apps_found = []
        for a in apps.keys():
            apath = apps[a]
            status, err = execute_job(['ls', '-lth',apath], return_error=True)
            if len(status) == 0 and len(err) > 0:
                status = err
            if 'cannot' in status:
                error('App {0} not found at path {1}'.format(a, apath), sysexit=False)
            apps_found.append(a)
        print 'All apps found: {0}'.format(','.join(apps_found))

        pass

    #===================================================================================================================
    def parse_config(self):
        """
        Check user supplied config file
        """
        args = self.args

        #################
        # Configuration files
        #################

        # Get LOCAL configuration file
        jobconf = get_config(args.config)
        jobconf['Pipeline_Info'].update({'workflow': jobconf['Pipeline_Info']['pipeline']})

        # Get WORKFLOW configuration file
        if 'version' in jobconf['Pipeline_Info'].keys():
            version = jobconf['Pipeline_Info']['version']
            if '.' in version:
                version = version.split('.')[0]
        else:
            version = '1'
        gconfname = os.path.join(CONFIGPATH+'/workflows', jobconf['Pipeline_Info']['workflow'].lower()+
                                 '_config_v' + version + '.conf')
        if file_exists(gconfname):
            globalconfig = gconfname
        else:
            error('File {0} does not exist.'.format(gconfname))
        print 'Using global config {0} for pipeline {1} ' \
              'version {2}'.format(globalconfig, jobconf['Pipeline_Info']['workflow'], version)

        conf = get_config(globalconfig)

        # Get TOOL configuration files
        wrapperconfig = os.path.join(CONFIGPATH, 'master/MASTER_tool_wrappers.conf')
        if 'appregistry' in conf['Pipeline_Info'].keys():
            vconfnum = conf['Pipeline_Info']['appregistry'].strip()
        else:
            vconfnum = 'current'
        versionconfig = os.path.join(CONFIGPATH, 'master/MASTER_tool_versions_{0}.conf'
                                     .format(vconfnum))
        vconf = get_config(versionconfig)
        reginfo = 'Version: {0}; Date: {1}; File: {2}'.format(vconf['info_versions']['version'], vconf['info_versions']['date'],
                                                              versionconfig)
        conf['Pipeline_Info']['appregistry'] = reginfo
        if not file_exists(versionconfig):
            error('Tool version file {0} does not exist'.format(args.toolversion), sysexit=True)
        print 'Using global tool version {0}'.format(vconfnum)
        conf.merge(vconf)
        conf.merge(get_config(wrapperconfig))

        # Get GLOBAL parameter file
        if 'REFERENCE' not in conf['Global_Parameters']:
            print 'No reference provided. Using UCSC_hg19'
            reference = 'repos_UCSC_hg19'
        else:
            reference = 'repos_'+conf['Global_Parameters']['REFERENCE']
        parameterconfig = os.path.join(CONFIGPATH, 'master/MASTER_global_parameters.conf')
        pconf = get_config(parameterconfig)
        if reference not in pconf.keys():
            error('Reference {0} not found in {1}'.format(reference.replace('repos_') , parameterconfig), sysexit=True)
        conf['Global_Parameters'].update(pconf[reference])
        conf['Global_Parameters'].update(pconf['CPU_Parameters'])
        conf['Global_Parameters'].update(pconf['Shell_environment'])

        # Re-merge with job config and workflow config
        conf.merge(get_config(globalconfig))
        conf.merge(jobconf)


        pipelineinfo = conf['Pipeline_Info']
        seriesinfo = conf['Series_Info']
        #conf = get_config(args.config)
        if 'workflow' in pipelineinfo.keys():
            pipeline = pipelineinfo['workflow']
        elif 'pipeline' in pipelineinfo.keys():
            pipeline = pipelineinfo['pipeline']
            conf['Pipeline_Info'].update({'workflow': pipeline})

        # Checks
        req_sections = ['Series_Info', 'Pipeline_Info']
        if any([item not in conf.keys() for item in req_sections]):
            error('User config file must have the following sections: {0}'.format(','.join(req_sections)))

        # Jobinfo
        req_sections = ['job_name', 'dir_out', 'fastq_list']
        if any([item not in conf['Series_Info'].keys() for item in req_sections]):
            error('User config file must have the following sections: {0}'.format(','.join(req_sections)))

        # Pipeline info
        req_sections = ['workflow', 'read_type', 'task']
        if any([item not in conf['Pipeline_Info'].keys() for item in req_sections]):
            error('User config file must have the following sections: {0}'.format(','.join(req_sections)))


        #ARGUMENTS
        self.param = {'library_stranded': False,
                      'strand': 'both',
                      'include_duplicates': True,
                      'jobname': seriesinfo['job_name'],
                      'newanalysis': False,
                      'outdir': os.path.abspath(seriesinfo['dir_out']),
                      'logfile': os.path.join(os.path.abspath(seriesinfo['dir_out']) + '/job_' + seriesinfo['job_name']+'.LOG'),
                      'paired': False,
                      'read_type': 'paired',
                      'paired_samples': False,
                      'input_filetype': '',
                      'modify_cpu': True,
                      'workflow': pipelineinfo['workflow'],
                      'sample_location': 'local'}

        # Strandedness for rna-seq:
        if 'library_stranded' in pipelineinfo.keys() and pipelineinfo['library_stranded'].lower() \
                in ['yes', 'true', 'second', 'first']:
            print 'Stranded library found'
            self.param['library_stranded'] = True
            if pipelineinfo['library_stranded'].lower() in ['second', 'first']:
                self.param['strand'] = pipelineinfo['library_stranded'].lower()
            else:
                self.param['strand'] = 'first'
            print 'Library strand: {0}'.format(self.param['strand'])
        else:
            self.param['library_stranded'] = False

        # Ignore duplicates
        if 'include_duplicates' in pipelineinfo.keys() and pipelineinfo['include_duplicates'].lower() \
                in ['no', 'false']:
            print 'Will not include duplicate fastq files'
            self.param['include_duplicates'] = False


        # Sample library type
        if 'library_type' in conf['Series_Info']:
            if conf['Series_Info']['library_type'].lower() == 'rna':
                print 'Setting Library Type: RNA'
                conf['app_optitype']['dna'] = 'omit'
                conf['app_optitype']['rna'] = 'keep'
                conf['app_stampy']['hash'] = '{HLASTAMPYRNA}'
                conf['app_stampy']['genome'] = '{HLASTAMPYRNA}'
        else:
            conf['Series_Info']['library_type'] = 'DNA'
            print 'Setting Library Type: DNA'

        # Newanalysis?
        if 'newanalysis' in seriesinfo.keys():
            if seriesinfo['newanalysis'].lower() in ['true', 'yes']:
                self.param['newanalysis'] = True

        # fastq list exists
        if not file_exists(seriesinfo['fastq_list']):
            error('Could not find or access {0}'.format(seriesinfo['fastq_list']), sysexit=True)
        else:
            self.param['fqlist'] = seriesinfo['fastq_list']
            conf['Series_Info']['fastq_list'] = os.path.abspath(seriesinfo['fastq_list'])

        # parent directory writeable
        parentdir = os.path.dirname(seriesinfo['dir_out'])
        if not path_writeable(parentdir):
            error('Cannot write to path {0}'.format(parentdir))
        else:
            #self.param['outdir'] = os.path.abspath(seriesinfo['dir_out'])
            conf['Series_Info']['dir_out'] = os.path.abspath(seriesinfo['dir_out'])

        # Set up log file


        # pipeline
        #ADDPIPELINE
        if pipelineinfo['workflow'].lower() not in PIPELINESET:
            error('Pipeline must be one of {0}'.format(','.join(PIPELINESET)))
        else:
            self.param['workflow'] = pipelineinfo['workflow']


        # Paired reads

        if pipelineinfo['read_type'].lower() not in ['single', 'paired']:
            error('Pipeline readtype must be either single or paired')
        else:
            self.param['read_type'] = pipelineinfo['read_type']

        if pipelineinfo['read_type'].lower().strip() == 'paired':
            self.param['paired'] = True

        # Paired samples

        if 'paired_samples' in pipelineinfo.keys():
            if pipelineinfo['paired_samples'].lower() in ['yes', 'true']:
                print 'Samples in paired mode'
                self.param['paired_samples'] = True

        # Fastq compressed
        if 'input_filetype' in pipelineinfo.keys():
            if pipelineinfo['input_filetype'].lower() in ['compressed']:
                print 'Input files are compressed'
                self.param['input_filetype'] = 'compressed'

        # Modify CPU resources
        if 'MODIFY_CPU' in conf['Global_Parameters'].keys():
            if conf['Global_Parameters']['MODIFY_CPU'].lower() in ['false', 'no']:
                self.param['modify_cpu'] = False


        self.conf = conf
        pass

    #===================================================================================================================
    def parse_pipeline_parameters(self):
        """
        Check pipeline specific parameters
        @return:
        """

        conf = self.conf
        pipeline = conf['Pipeline_Info']['workflow']

        # SNATCH
        # ======================================================
        if pipeline.lower() == 'snatch':
            # Check accessory files
            if 'baits' not in conf['Pipeline_Info'].keys() or 'targets' not in conf['Pipeline_Info'].keys():
                error('For SNATCH, Pipeline_Info section must specify baits and targets', sysexit=True)

                if not file_exists(conf['Pipeline_Info']['baits']):
                    error('Bait file {0} not found'.format(conf['Pipeline_Info']['baits']), sysexit=True)

                if not file_exists(conf['Pipeline_Info']['targets']):
                    error('Target file {0} not found'.format(conf['Pipeline_Info']['targets']), sysexit=True)


        # Catch
        # ======================================================
        # if pipeline.lower() == 'catch':
        #     # Check accessory files
        #     if 'sample_amplicon_library' not in conf['Job_Info'].keys():
        #         error('For CATCH, Job_Info section must specify amplicon_library (deprecated)', sysexit=True)

        pass

    #===================================================================================================================
    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--config', help='Configfile', required=True)
        parser.add_argument('--globalconfig', help='Global Configfile')
        parser.add_argument('--test', help='If used, only Pipeline commands printed. No jobs submitted',
                            action='store_true', default=False)
        args = parser.parse_args()

        if not file_exists(args.config):
            error('Could not read config file {0}'.format(args.config), sysexit=True)

        # Convert to absolute paths
        args.config = os.path.abspath(args.config)
        args.globalconfig = os.path.abspath(args.globalconfig)
        self.args = args
        pass

    # #===================================================================================================================
    # def check_inputs(self):
    #     """
    #     Check merged config file
    #     * presence of fastq file
    #     * results directory
    #     """
    #
    #     # Job info
    #     conf = self.conf
    #     seriesinfo = conf['Series_Info']
    #     pipelineinfo = conf['Pipeline_Info']
    #
    #     # fastq list exists
    #     if not file_exists(seriesinfo['fastq_list']):
    #         error('Could not find or access {0}'.format(seriesinfo['fastq_list']), sysexit=True)
    #     else:
    #         self.param['fqlist'] = seriesinfo['fastq_list']
    #         self.conf['Series_Info']['fastq_list'] = os.path.abspath(seriesinfo['fastq_list'])
    #
    #     # parent directory writeable
    #     parentdir = os.path.dirname(seriesinfo['dir_out'])
    #     if not path_writeable(parentdir):
    #         error('Cannot write to path {0}'.format(parentdir))
    #     else:
    #         self.param['outdir'] = os.path.abspath(seriesinfo['dir_out'])
    #         self.conf['Series_Info']['dir_out'] = os.path.abspath(seriesinfo['dir_out'])
    #
    #     # Set up log file
    #     self.param['logfile'] = os.path.join(self.param['outdir'], 'job_'+self.param['jobname']+'.LOG')
    #
    #     # pipeline
    #     #ADDPIPELINE
    #     if pipelineinfo['workflow'].lower() not in PIPELINESET:
    #         error('Pipeline must be one of {0}'.format(','.join(PIPELINESET)))
    #     else:
    #         self.param['workflow'] = pipelineinfo['workflow']
    #     self.param['workflow'] = pipelineinfo['workflow']
    #
    #     # Paired reads
    #     self.param['paired'] = False
    #     self.param['read_type'] = 'paired'
    #     if pipelineinfo['read_type'].lower() not in ['single', 'paired']:
    #         error('Pipeline readtype must be either single or paired')
    #     else:
    #         self.param['read_type'] = pipelineinfo['read_type']
    #
    #     if pipelineinfo['read_type'].lower().strip() == 'paired':
    #         self.param['paired'] = True
    #
    #     # Paired samples
    #     self.param['paired_samples'] = False
    #     if 'paired_samples' in pipelineinfo.keys():
    #         if pipelineinfo['paired_samples'].lower() in ['yes', 'true']:
    #             print 'Samples in paired mode'
    #             self.param['paired_samples'] = True
    #
    #     # Fastq compressed
    #     self.param['input_filetype'] = ''
    #     if 'input_filetype' in pipelineinfo.keys():
    #         if pipelineinfo['input_filetype'].lower() in ['compressed']:
    #             print 'Input files are compressed'
    #             self.param['input_filetype'] = 'compressed'
    #
    #     # Modify CPU resources
    #     self.param['modify_cpu'] = True
    #     if 'MODIFY_CPU' in self.conf['Global_Parameters'].keys():
    #         if self.conf['Global_Parameters']['MODIFY_CPU'].lower() in ['false', 'no']:
    #             self.param['modify_cpu'] = False
    #
    #     pass

    #===================================================================================================================
    def check_previous_analysis(self):
        """
        Check previous analysis and delete directory if necessary
        """
        outdir = self.param['outdir']
        newanalysis = self.param['newanalysis']

        print 'New Analysis is {0}'.format(newanalysis)
        if not newanalysis:
            if dir_exists(outdir):
                #error('Specified directory {0} already exists. Specify new directory or '
                #      'set newanalysis to be True'.format(outdir), sysexit=True)
                print 'WARNING: {0} already exists'.format(outdir)
            else: return
        else:
            if dir_exists(outdir):
                print 'Found pre-existing directory {0}. Checking log file'.format(outdir)
                # Check if log file is present and has size > 0
                if not file_exists(self.param['logfile']):
                    print 'Log file {0} is empty or not found.'.format(self.param['logfile'])
                    print 'Deleting directory {0}'.format(outdir)
                    remove_contents(outdir)

                    return
                else:
                    #Monitor jobs by PID
                    running_jobs = 0
                    completed_jobs = 0
                    total_jobs = 0

                    print 'Reading pre-existing log file {0}'.format(self.param['logfile'])
                    with open(self.param['logfile']) as lfh:
                        lfhlines = lfh.readlines()

                        for lfhl in lfhlines:
                            lfhl = lfhl.rstrip('\n').split('\t')
                            if lfhl[0] == 'JobName':
                                continue
                            total_jobs += 1
                            pid = lfhl[1].strip()

                            status, temp = execute_job(['qstat', '-j', str(pid)], flag_shell=False,
                                                       return_error=True, sysexit=False)
                            if len(status) == 0 and len(temp) > 0:
                                status = temp
                            status_line1 = status.split('\n')[0]

                            if status_line1.split()[0] == 'Following':
                                completed_jobs += 1
                            elif '=' in list(status_line1):
                                running_jobs += 1

                        print 'Found {0} completed and {1} running jobs'.format(completed_jobs, running_jobs)
                        if completed_jobs != total_jobs:
                            error('Found {0} completed and {1} total jobs'.format(completed_jobs, total_jobs))
                        else:
                            print 'All jobs are completed and New Analysis is requested. ' \
                                  'Deleting directory {0}'.format(outdir)
                            remove_contents(outdir)

        pass

    #===================================================================================================================
    def create_dir(self):
        """
        Create result parentdir
        """
        outdir = self.param['outdir']
        sample_dir = os.path.join(outdir, 'job_sample_logs')

        if not dir_exists(outdir):
            create_dir(outdir)
        if not dir_exists(sample_dir):
            create_dir(sample_dir)

        # Create PID log file
        pidlog = os.path.join(outdir, self.param['logfile'])
        if not file_exists(pidlog):
            with open(pidlog, 'a') as pid:
                pid.write('{0}\t{1}\t{2}\t{3}\n'.format('JobName', 'PID', 'SGE_LOG', 'JOB_LOG'))

        self.param['pidlog'] = pidlog
        pass

    #===================================================================================================================
    def parse_fastq_series(self):
        """
        Parse fastq_list
        """
        self.ffastq = defaultdict()
        self.paired_samples = defaultdict()
        self.matching_normal = {}
        self.nsamp = int()

        flist = self.param['fqlist']

        # Read lines
        with(open(flist, 'r')) as file_handle:
            fl = file_handle.readlines()

        # Check file contents
        if len(fl) == 0:
            error("Input file {0} does not have any lines\n".format(flist))

        # . Look for the header : this is the first non-empty, non-commented line.
        header_found = False

        for line in fl:
            line = line.rstrip('\n').replace('\r', '')
            line = line.replace('"', '')                           # Get rid of double quotes injected by Excel.

            if len(line) == 0 or len(line.replace(' ', '')) == 0 \
                    or len(line.strip()) == 0 or len(line.rstrip('\t')) == 0:  # Skip comments and empty lines
                continue
            if line[0] == '#':
                continue

            hli = line.split('\t')
            hlen = len(hli)
            header = '\t'.join(hli[0:4])
            header_expected = "parameterType\tshortName\tparameter1\tparameter2"
            header_lc = header.lower()
            header_expected_lc = header_expected.lower()

            if header_lc != header_expected_lc:
                err = "Error from SeqAnalysis.parse_fastq_series:\n"
                err += "Input file does not start with expected header (to within case) :\n"
                err += "Input file = {0}\n".format(flist)
                err += "Header in file = {0}\n".format(header)
                err += "Header expected = {0}\n".format(header_expected)
                err += "\n"
                error(err, sysexit=True)
            else:
                header_found = True
                break

        if not header_found:
            err = 'Error from SeqAnalysis.parse_fastq_series: \n'
            err += 'No header found in input list file.\n'
            error(err)

        # optional arguments in header
        self.otherparam = defaultdict()
        self.opthead = False
        ohlen = hlen - 4
        supported_headers = ['amplicon_panel', 'DESIGN_'] #Supported Extra Columns
        if hlen > 4:
            ohli = hli[4:]
            print 'Optional headers found: {0}'.format('\t'.join(ohli))
            #if any([item for item in ohli if item not in supported_headers]):
            #    print 'One of the extra column names provided is incompatible: {0}'.format('\t'.join(ohli))
            #    print 'Supported extra columns:{0}'.format('\t'.join(supported_headers))
            #    sys.exit(1)
            self.opthead = True

        # . Read the body :
        fqline_count = 0
        fq1list = []
        fq2list = []

        for line in fl:
            line = line.rstrip('\n').replace('\r','')
            line = line.replace('"', '')                           # Get rid of double quotes injected by Excel.

            if len(line) == 0 or len(line.replace(' ', '')) == 0 \
                    or len(line.strip()) == 0 or len(line.rstrip('\t')) == 0:  # Skip comments and empty lines
                continue
            if '#' in line:
                continue
            if 'parameterType' in line:                            # remove header line (??)
                continue

            fqline_count += 1
            lineli = line.split('\t')
            line_clean = [item.strip() for item in lineli[0:4]]

            if len(line_clean) == 3:
                parametertype, shortname, param1 = line_clean
                param2 = 'NA'
            else:
                parametertype, shortname, param1, param2 = line_clean

            # Fill in extra parameters
            if self.opthead:
                if len(lineli) - 4 != ohlen:
                    print lineli
                    error('Expecting {0} extra columns from header information. Found only {1}'
                          .format(ohlen, len(lineli)-4),sysexit=True)
                olineli = lineli[4:]
                ohdict = {}
                for i in xrange(0, ohlen):
                    ohdict.update({ohli[i]: olineli[i]})
                if shortname in self.otherparam:
                    # Check for mismatched parameters
                    if self.otherparam[shortname] != ohdict:
                        error('Input optional parameters for same sample name have differing values', sysexit=True)
                else:
                    self.otherparam[shortname] = ohdict

            # Check items
            if len(parametertype) == 0:
                err = 'Error from SeqAnalysis.parse_fastq_series:\n'
                err += 'parameterType entry in Table is empty on line {0}.'.format(fqline_count)
                error(err, sysexit=True)
            if len(shortname) == 0:
                err = 'Error from SeqAnalysis.parse_fastq_series::\n'
                err += 'shortName entry in Table is empty on line {0}.'.format(fqline_count)
                error(err, sysexit=True)
            if len(param1) == 0:
                err = 'Error from SeqAnalysis.parse_fastq_series::\n'
                err += 'parameter1 entry in Table is empty on line {0}.'.format(fqline_count)
                error(err, sysexit=True)
            # if len(param2) == 0:
            #     err = 'Error from SeqAnalysis.parse_fastq_series::\n'
            #     err += 'parameter2 entry in Table is empty on line {0}. Enter NA if not present.'.format(fqline_count)
            #     error(err, sysexit=True)
            if os.path.basename(param1) in fq1list:
                err = 'WARNING from SeqAnalysis.parse_fastq_series::'
                err += 'Name {0} is present more than once'.format(os.path.basename(param1))
                print(err)
                if not self.param['include_duplicates']:
                    continue
            else:
                fq1list.append(os.path.basename(param1))

            # Check if fastqfiles are online
            #self.param['sample_location'] = 'local'
            if parametertype == 'fastqFTP': self.param['sample_location'] = 'ftp'

            if parametertype in ['fastqFile', 'bamFile', 'fastqFTP']:
                if param2 == 'NA' and self.param['paired']:
                    err = 'Error from SeqAnalysis.parse_fastq_series::\n'
                    err += 'parameter2 entry in Table is NA but read_type is paired.'.format(fqline_count)
                    error(err, sysexit=True)
                if param2 != 'NA' and not self.param['paired']:
                    err = 'Error from SeqAnalysis.parse_fastq_series::\n'
                    err += 'parameter2 entry in Table is not NA but read_type is single.'.format(fqline_count)
                    error(err, sysexit=True)
                if self.param['paired'] and param2 != 'NA':
                    if os.path.basename(param2) in fq2list:
                        err = 'WARNING from SeqAnalysis.parse_fastq_series::'
                        err += 'Fastq file name {0} is present more than once'.format(os.path.basename(param2))
                        print(err)
                        if not self.param['include_duplicates']:
                            continue
                    else:
                        fq2list.append(os.path.basename(param2))
                        # Process parameter type
                if shortname not in self.ffastq.keys():
                    self.ffastq[shortname] = {'file1': [param1], 'file2': [param2], 'nfiles': 1}
                else:
                    newdic = self.ffastq[shortname]
                    newdic['file1'].append(param1)
                    newdic['file2'].append(param2)
                    newdic['nfiles'] += 1
                    self.ffastq[shortname] = newdic

            if parametertype == 'fastqCompressed':
                if shortname not in self.ffastq.keys():
                    self.ffastq[shortname] = {'file1': [param1], 'file2': [param1], 'nfiles': 1}
                else:
                    newdic = self.ffastq[shortname]
                    newdic['file1'].append(param1)
                    newdic['file2'].append(param1)
                    newdic['nfiles'] += 1
                    self.ffastq[shortname] = newdic

            if parametertype == 'pairedSample':
                if len(line_clean) < 4:
                    error('For pairedSample, must provide 2 parameters.')
                if shortname in self.paired_samples.keys():
                    error('Must provide unique sample pair names for pairedSample')

                if 'normal' in param1 and 'tumor' in param2:
                    normal = param1.replace('normal:','')
                    tumor = param2.replace('tumor:','')
                elif 'normal' in param2 and 'tumor' in param1:
                    normal = param2.replace('normal:','')
                    tumor = param1.replace('tumor:','')
                else:
                    error('Normal/tumor not specified correctly.')

                self.paired_samples[shortname] = {'normal': normal, 'tumor': tumor}
                self.matching_normal.update({tumor: normal})
                pass

        print 'Processed {0} fastq file lines'.format(fqline_count)
        self.nsamp = len(self.ffastq.keys())

        if len(self.paired_samples) > 0:
            if len(self.paired_samples) != int(len(self.ffastq))/2:
                error('Paired Sample names do not equal number of individual samples.', sysexit=True)

            set1 = set(self.matching_normal.keys() + self.matching_normal.values())
            set2 = set(self.ffastq.keys())
            if sorted(set1) != sorted(set2):
                error('Paired Sample names do not equal number of individual samples.', sysexit=True)

        # Get file sizes
        for sample in self.ffastq:
            if self.param['sample_location'] == 'local':
                self.ffastq[sample]['filesize'] = sum(map(get_approximate_read_number, self.ffastq[sample]['file1']))
                if self.param['paired']:
                    self.ffastq[sample]['filesize'] += sum(map(get_approximate_read_number, self.ffastq[sample]['file1']))
            else:
                self.ffastq[sample]['filesize'] = 0

        print 'Found {0} unique samples: {1}'.format(self.nsamp, ','.join(self.ffastq.keys()))

        pass

    #===================================================================================================================
    def check_fastq_files_exist(self):
        """
        Check if fastq files exist
        """
        short_names = self.ffastq.keys()
        print 'Found {0} samples: {1}'.format(self.nsamp, ','.join(short_names))

        # Counters and lists
        files_missing = []
        nf_missing = 0
        ns_missing = 0

        if self.param['sample_location'] == 'local':
            for sample in short_names:
                # Check if present
                file_found = filter(file_exists, self.ffastq[sample]['file1'])
                file_not_found = set(self.ffastq[sample]['file1']).difference(set(file_found))
                files_missing.extend(list(file_not_found))

                if len(file_not_found) > 0:
                    ns_missing += 1
                nf_missing += len(file_not_found)

                print 'Checking sample {0} fastq1. Found {1} of {2} files'. \
                    format(sample, len(file_found), self.ffastq[sample]['nfiles'])
                # if len(file_not_found) > 0:
                #     print 'Missing files are: {0}'.format('\n'.join(files_missing))

                if self.param['paired']:
                    file_found = filter(file_exists, self.ffastq[sample]['file2'])
                    file_not_found = set(self.ffastq[sample]['file2']).difference(set(file_found))
                    files_missing.extend(list(file_not_found))
                    if len(file_not_found) > 0: ns_missing += 1
                    nf_missing += len(file_not_found)
                    print 'Checking sample {0} fastq2. Found {1} of {2} files'. \
                        format(sample, len(file_found), self.ffastq[sample]['nfiles'])
                    #if len(file_not_found) > 0:
                    #    print 'Missing files are: {0}'.format('\n'.join(files_missing))

            if ns_missing > 0:
                print '{0} samples are missing at least one file'.format(ns_missing)
                print 'Total missing files: {0}'.format(nf_missing)
                print 'Missing files are:\n {0}'.format('\n'.join(files_missing))
                error('Exit SeqAnalysis', sysexit=True)
        else:
            print 'No file checks for FTP samples.'

        pass

    #===================================================================================================================
    def create_subdirs(self):
        """
        Create sample sub directories
        """
        outdir = self.param['outdir']
        samples = self.ffastq.keys()

        # Sample directory names
        sample_dirs = [os.path.join(outdir, item) for item in samples]

        # Create sample directories
        map(create_dir, sample_dirs)

        pass

    #===================================================================================================================
    def create_fastq_list(self):
        """
        Create sample specific fastq list from fastq_series
        Format: fq1 [TAB] fq2
        Can have multiple pairs of fastq files
        """
        short_names = self.ffastq.keys()

        for sample in short_names:
            fq_name = os.path.join(self.param['outdir'], sample+'/'+sample+'_fqlist.txt')
            if not self.param['newanalysis']:
                if file_exists(fq_name):
                    print 'Removing pre-existing fqlist file {0}'.format(fq_name)
                    os.remove(fq_name)
            with open(fq_name, 'a') as fqh:
                for i in range(0, self.ffastq[sample]['nfiles']):
                    if self.param['paired']:
                        fqh.write('{0}\t{1}\n'.format(self.ffastq[sample]['file1'][i], self.ffastq[sample]['file2'][i]))
                    else:
                        fqh.write('{0}\n'.format(self.ffastq[sample]['file1'][i]))

        print 'Created sample fastqlists'
        pass

    #===================================================================================================================
    def prepare_config_file(self):
        """
        Prepare config file. Adds the [jobinfo] section with sample name and sample output directories to the config file
        and places the config files in the sample_log directory
        """

        jobinfo = defaultdict()
        short_names = self.ffastq.keys()
        sampleinfo = defaultdict()

        for sample in short_names:
            conf = self.conf

            sample_config = os.path.join(self.param['outdir'], 'job_sample_logs/'+sample+'.conf')
            sample_sge_err = os.path.join(self.param['outdir'], 'job_sample_logs/'+sample+'.sge.err')
            sample_dir = os.path.join(self.param['outdir'], sample)
            sample_log = os.path.join(self.param['outdir'], 'job_sample_logs/'+sample+'.log')

            # Paired samples
            # --------------
            sample_paired = 'no;;'
            if self.param['paired_samples']:
                if sample in [self.paired_samples[item]['tumor'] for item in self.paired_samples.keys()]:
                    sample_type = 'tumor'
                    matching_sample = [self.paired_samples[item]['normal'] for
                                       item in self.paired_samples.keys() if self.paired_samples[item]['tumor'] == sample][0]
                else:
                    sample_type = 'normal'
                    matching_sample = [self.paired_samples[item]['tumor'] for
                                       item in self.paired_samples.keys() if self.paired_samples[item]['normal'] == sample][0]

                sample_paired = 'yes;{0};{1}'.format(sample_type, matching_sample)

            # Compressed fastq
            # -----------------
            inputfile_compressed = 'no'
            if self.param['input_filetype'] == 'compressed': inputfile_compressed = 'yes'

            # Fastq location
            sampleloc = 'local'
            if self.param['sample_location'] == 'ftp': sampleloc = 'ftp'

            fq_name = os.path.join(self.param['outdir'], sample+'/'+sample+'_fqlist.txt')

            jobinfo = {'sample_name': sample, 'sample_dir': sample_dir, 'sample_log': sample_log,
                       'sample_fastq': fq_name, 'sample_sge_err': sample_sge_err, 'sample_config': sample_config,
                       'sample_size': self.ffastq[sample]['filesize'], 'sample_paired': sample_paired,
                       'sample_location': sampleloc,
                       'sample_filecompressed': inputfile_compressed,
                       'sample_library_stranded': self.param['library_stranded'],
                       'sample_library_strand': self.param['strand'],
                       'sample_library_type': self.conf['Series_Info']['library_type']}

            if self.opthead:
                # Make list of unique parameters
                exparamlist = []
                for item in self.otherparam[sample]:
                    jobinfo.update({'sample_'+item.lower(): self.otherparam[sample][item]})
                    if item not in exparamlist: exparamlist.append(item.lower())
                conf['Series_Info'].update({'opt_param': ';'.join(exparamlist)})

            sampleinfo[sample] = jobinfo

            conf['Job_Info'] = jobinfo
            conf.filename = sample_config

            # Check file sizes
            if self.param['modify_cpu']:
                if float(self.ffastq[sample]['filesize']) > 100000000 and conf['Global_Parameters']['NUMTHREADS'] == '2':
                    print 'Setting NUMTHREADS to 4; MAXMEM to 24 GB'
                    conf['Global_Parameters']['NUMTHREADS'] = '4'
                    conf['Global_Parameters']['MAXMEM'] = '24g'
                    # if float(self.ffastq[sample]['filesize']) > 100000000 and conf['Global_Parameters']['NUMTHREADS'] == '2':
                    #     print 'Setting NUMTHREADS to 4; MAXMEM to 32 GB'
                    #     conf['Global_Parameters']['NUMTHREADS'] = '4'
                    #     conf['Global_Parameters']['MAXMEM'] = '32g'

            # Check for library strandedness
            if self.param['library_stranded']:
                if 'app_tophat' in conf.keys():
                    conf['app_tophat']['library-type'] = 'fr-firststrand'
                    if self.param['strand'] == 'second':
                        conf['app_tophat']['library-type'] = 'fr-secondstrand'
                if 'app_tophatfusion' in conf.keys():
                    conf['app_tophatfusion']['library-type'] = 'fr-firststrand'
                    if self.param['strand'] == 'second':
                        conf['app_tophatfusion']['library-type'] = 'fr-secondstrand'
                if 'app_cufflinks' in conf.keys():
                    conf['app_cufflinks']['-library-type'] = 'fr-firststrand'
                    if self.param['strand'] == 'second':
                        conf['app_cufflinks']['-library-type'] = 'fr-secondstrand'
                if 'app_htseq' in conf.keys():
                    conf['app_htseq']['s'] = 'yes'
                if 'app_star' in conf.keys():
                    conf['app_star']['outSAMstrandField'] = 'omit'
                    conf['app_star']['outFilterIntronMotifs'] = 'RemoveNoncanonical'
                if 'app_featurecount' in conf.keys():
                    conf['app_featurecount']['s'] = '1'
                    if self.param['strand'] == 'second':
                        conf['app_featurecount']['s'] = '2'
                if 'app_rsem_calculate_expression' in conf.keys():
                    #conf['app_rsem_calculate_expression']['strand-specific'] = 'keep'
                    conf['app_rsem_calculate_expression']['forward-prob'] = '1'
                    if self.param['strand'] == 'second':
                        conf['app_rsem_calculate_expression']['forward-prob'] = '0'
                if 'app_qualimap_rnaseq' in conf.keys():
                    conf['app_qualimap_rnaseq']['p'] = 'strand-specific-forward'
                    if self.param['strand'] == 'second':
                        conf['app_qualimap_rnaseq']['p'] = 'strand-specific-reverse'
                if 'app_qualimap_bamqc' in conf.keys():
                    conf['app_qualimap_bamqc']['p'] = 'strand-specific-forward'
                    if self.param['strand'] == 'second':
                        conf['app_qualimap_bamqc']['p'] = 'strand-specific-reverse'

            if self.param['paired']:
                if 'app_featurecount' in conf.keys():
                    conf['app_featurecount']['p'] = 'keep'
            else:
                if 'app_rnaseqc' in conf.keys():
                    conf['app_rnaseqc']['singleEnd'] = 'keep'

            conf.write()

        self.sampleinfo = sampleinfo
        print 'Created sample config files'

        pass

    #===================================================================================================================
    def submit_jobs(self):
        """
        Call individual pipeline for analysis
        """
        short_names = self.ffastq.keys()
        sampinfo = self.sampleinfo
        for sample in short_names:
            sconffile = sampinfo[sample]['sample_config']
            job_shell_file = os.path.join(self.param['outdir'], 'job_sample_logs/'+sample+'.job')

            if 'dscigend' in socket.gethostname():
                PE='OpenMP'
            else:
                PE='node'

            with open(job_shell_file, 'w') as jsh:
                jsh.write('#!/bin/sh\n')
                jsh.write('#$ -S /bin/bash\n')
                jsh.write('#$ -j y\n')
                jsh.write('#$ -cwd\n')
                #jsh.write('#$ -V\n')
                jsh.write('#$ -m a\n')
                jsh.write('#$ -M dipen.sangurdekar@biogen.com\n')
                #jsh.write('#$ -l s_rt=48:00:00\n')
                jsh.write('#$ -N {0};SampleID={1}\n'.format(self.conf['Series_Info']['job_name'], sample))
                jsh.write('#$ -pe {1} {0}\n'.format(self.conf['Global_Parameters']['NUMTHREADS'], PE))
                jsh.write('#$ -R y\n')
                jsh.write('#$ -o {0}\n'.format(sampinfo[sample]['sample_sge_err']))
                jsh.write('source /etc/profile.d/set_modules.sh\n')
                jsh.write('module use -a {0}\n'.format(self.conf['Global_Parameters']['MODULEPATH']))
                jsh.write('module load synapse\n')
                jsh.write('echo "** Job Execution on Host: `hostname`"\n')
                cmd = '{0} --pipeline {1} --config {2} --globalconfig NULL\n' \
                    .format(SFPATH, self.conf['Pipeline_Info']['workflow'], sconffile)
                jsh.write(cmd)

            Job(jobfile=job_shell_file, config=sconffile, pidlogfile=self.param['pidlog'], testmode=self.args.test)
        pass

    pass
