# First of all get modules present in repository

import os, sys, shutil
import copy
import config
import logging
import subprocess
import datetime
import math

python_module_path = os.path.normpath(
                      os.path.join(
                       os.path.dirname(os.path.abspath(__file__)), '../'
                          ))

class ScriptError(Exception):
    pass


if not hasattr(logging, 'NullHandler'):
    # From Python 2.7 code
    class NullHandler(logging.Handler):
        """
        This handler does nothing. It's intended to be used to avoid the
        "No handlers could be found for logger XXX" one-off warning. This is
        important for library code, which may contain code to log events. If a user
        of the library does not configure logging, the one-off warning might be
        produced; to avoid this, the library developer simply needs to instantiate
        a NullHandler and add it to the top-level logger of the library module or
        package.
        """
        def handle(self, record):
            pass

        def emit(self, record):
            pass

        def createLock(self):
            self.lock = None
else:
    NullHandler = logging.NullHandler

logging.basicConfig(level = logging.DEBUG)
logger = logging.getLogger()

def __file(typ):
    def func(path):
        if path == None:
            return None
        with open(path) as f:
            return typ(f.read()).strip()
    return func

str_file = __file(str)
float_file = __file(float) 
int_file = __file(int)

def list_file(path):
    if path == None:
        return []
    with open(path) as f:
        return list(f.read().split())

def flag_file(path):
    if path != None:
        return True
    else:
        return None

class PdbRepoModule:
    _config_filenames_required = [
                                  ('name', str_file),  
                                  ('description', str_file),
                                  ('version', str_file),
                                  ('maintainer', str_file),
                                  ('dependencies', list_file),
                                  ('job_type', str_file),
                                 ]

    _config_filenames_optional = [
                                  ('authors', str_file, ''),
                                  ('estimatedcputime', float_file, config.default['wall_time']),
                                  ('timeout', int_file, config.default['timeout']),
                                  ('exceptions', list_file, []),
                                  ('processed', list_file, []),
                                  #('debug', flag_file, 0), #??? no used currently
                                  #('test', flag_file, 0),
                                  ]
    
    _script_filenames_required = [
                                  'get_pdb_codes',
                                  'calculate',
                                 ]

    _job_type_to_script = {
            ('pbs',  'serial')  : '_create_pbs_serial',
            ('local','serial')  : '_create_local_serial',
            }

    def __init__(self, config_dir):
        # Check if config dir is complete
        # Separating file checking and parsing for 
        # better performance (no parsing if module is incomplete)
        
        for name, file_parser in self._config_filenames_required:
            path = os.path.join(config_dir, name)
            if not os.path.exists(path):
                raise TypeError, "Missing '%s' file in config dir '%s'" % \
                                                    (name, config_dir)
            else:
                try:
                    setattr(self, name, file_parser(path))
                except Exception as err:
                    raise TypeError, "Error parsing '%s' file in config dir '%s':"\
                                  + " %s" % (name, config_dir, err)
                #setattr(self, '_fn_%s' % name, path)
        
        for name, file_parser, default in self._config_filenames_optional:
            path = os.path.join(config_dir, name)
            if not os.path.exists(path):
                setattr(self, name, default)
            else:
                try:
                    setattr(self, name, file_parser(path))
                except Exception as err:
                    raise TypeError, "Error parsing '%s' file in config dir '%s':"\
                                  + " %s" % (name, config_dir, err)
        
        #for name, file_parser in self._config_filenames_required \
        #                         + self._config_filenames_optional:
        #    path = getattr(self, '_fn_%s' % name)
        #    try:
        #        setattr(self, name, file_parser(path))
        self.path = os.path.dirname(config_dir)
        scripts_dir = os.path.join(self.path, 'scripts')
        for name in self._script_filenames_required:
            path = os.path.join(scripts_dir, name)
            if not os.path.exists(path):
                raise TypeError, "Missing '%s' file in scripts dir '%s'" % \
                                                    (name, scripts_dir)
        
        self.logger = logging.getLogger('module.%s' % self.name)
        # Setup logging
        date_prefix = datetime.datetime.today()\
                                       .replace(microsecond=0)\
                                       .isoformat('_')
        log_dir_name = date_prefix + '_' + str(os.getpid())
        self.logdir = os.path.join(self.path, 'logs', log_dir_name)
        os.makedirs(self.logdir)
        self.logger.info('Created log dir %s', self.logdir)

        # Setup temporary directory
        tmp_dir_name = date_prefix + '_' + str(os.getpid())
        self.tempdir = os.path.join(self.path, 'tmp', tmp_dir_name)
        os.makedirs(self.tempdir)
        self.logger.info('Created tmp dir %s', self.tempdir)
        ###
        
        self.outputdir = os.path.join(self.path, 'data')

        # Set environment variables
        PYTHONPATH = os.environ.get('PYTHONPATH', '')
        if PYTHONPATH: PYTHONPATH += ':'
        PYTHONPATH += python_module_path

        env = {
                'PDB_REPOSITORY' : config.pdb_repository_dir,
                'PDB_TEMPDIR'    : self.tempdir,
                'PDB_LOGDIR'     : self.logdir,
                'PDB_OUTPUTDIR'  : self.outputdir,
                'PDB_MODULE'     : self.name,
                'PDB_MODULE_DIR' : self.path,
                'PDB_TIMEOUT'    : str(self.timeout),
                'PYTHONPATH'     : PYTHONPATH 
                }

        self._env = env
        self.environment = copy.copy(os.environ)
        self.environment.update(env)
                
        # Get list of files to be processed
        out = self._execute('get_pdb_codes')
        if out:
            self.to_process = set(out.strip().lower().split())
        else:
            self.to_process = set()
        
        self.exceptions = set([i.lower() for i in self.exceptions])
        self.processed = set([i.lower() for i in self.processed])
        # Skip codes in processed and execptions
        self.to_process = self.to_process.difference(self.processed, 
                                                     self.exceptions)
        # Save pdb codes to be processed 
        self._save_temp_file('code_list', '\n'.join(self.to_process))

    def _save_temp_file(self, name, content, mode=0644):
        fn = os.path.join(self.tempdir, name)
        f = open(fn, 'w')
        f.write(content)
        f.close()
        os.chmod(fn, mode)

    def print_config(self):
        for name, file_type in self._config_filenames_required \
                                 + self._config_filenames_optional:
            print name
            print ' ' + '\n '.join(str(getattr(self, name)).split('\n'))
            print

    def _get_script_path(self, script_name):
            return os.path.join(self.path, 'scripts', script_name)

    def _execute(self, script_name, path=None):
        if path == None:
            fn = self._get_script_path(script_name)
        else:
            fn = os.path.join(path, script_name)

        logger = logging.getLogger('module.%s.%s' % (self.name, script_name))
        logger.propagate = False
        logger_out = logging.getLogger('module.%s.%s.out' % (self.name, script_name))
        logger_err = logging.getLogger('module.%s.%s.err' % (self.name, script_name))

        logfile_out = os.path.join(self.logdir, script_name+'.out')
        logfile_err = os.path.join(self.logdir, script_name+'.err')
        handler_out = logging.FileHandler(logfile_out)
        handler_err = logging.FileHandler(logfile_err)

        logger_out.addHandler(handler_out)
        logger_err.addHandler(handler_err)

        if os.path.exists(fn):
            self.logger.info('Running %s script', script_name)
            p = subprocess.Popen(fn, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environment)
            out, err_out = p.communicate()
            logger_err.info(err_out)
            logger_out.info(out)
            if p.returncode != 0:
                raise ScriptError, 'Script %s exited with status %i' % (script_name, p.returncode)
            return out
        else:
            return None
    def submit_qsub(self, script):
        p = subprocess.Popen('qsub', stdin=subprocess.PIPE, 
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
        ######
        out, err_out = p.communicate(script)
        if p.returncode != 0:
            raise ScriptError, 'Job submission failed: %s' % err_out 
        
        self.qsub_job_id = out
    #def submit_qsub(self, script):
    #    print script
    def create_local_script(self, dependencies):
        # Local jobs are executed one by one, no need to check pids
        deps = []
        return getattr(self, self._job_type_to_script[('local', self.job_type)])(deps)
    
    def create_pbs_script(self, dependencies):
        deps = []
        for item in dependencies:
            if not item.pbs_job_id == None:
                deps.append(item)
        return getattr(self, self._job_type_to_script[('pbs', self.job_type)])(deps)

    gnu_parallel_template = """
cd %(workdir)s
# Create propper sshloginfile from node file
NODEFILE=${PBS_JOBID}.nodefile
sort $PBS_NODEFILE | uniq -c | awk '{print $1"/"$2}' > ${NODEFILE}
cat %(code_list)s | parallel -L1 -j%(cores)i --sshloginfile ${NODEFILE} -W%(workdir)s %(script)s {}
"""
    gnu_parallel_local_template = """
cd %(workdir)s
cat %(code_list)s | parallel -L1 -j%(cores)i -W%(workdir)s %(script)s {}
    """
    
    def _create_pbs_serial(self, dependencies):
        kw = {}
        kw['queue_name'   ] = config.default['queue_name']
        kw['job_ids'      ] = ':'.join([d.pbs_job_id for d in dependencies])
        kw['cores'        ] = self.cores
        kw['workdir'      ] = self.tempdir
        kw['code_list'    ] = os.path.join(self.tempdir, 'code_list')
        kw['script'       ] = os.path.join(self.tempdir, 'script_wrapper.sh') + ' ' + self._get_script_path('calculate')
        kw['env_variables'] = ','.join(['%s=%s' % item for item in self._env.iteritems()])

        output = []
        output.append('#!/bin/bash')
        output.append('#')
        output.append('#PBS -q %(queue_name)s')
        if dependencies:
            output.append('PBS -W depend=afterok:%(job_ids)s')
        output.append('#PBS -l nodes=%(cores)i')
        output.append('#PBS -o %(workdir)s/log')
        output.append('#PBS -v %(env_variables)s')
	# Create wrapper script to export env variables (parallel is not dealing with that)
       	 
        output.append(self.gnu_parallel_template)
        wrapper_output = []
        wrapper_output.append('#!/bin/bash')
        wrapper_output.extend(["export %s=%s" % item for item in self._env.iteritems()])
        wrapper_output.append('$1')
        self._save_temp_file('script_wrapper.sh', '\n'.join(wrapper_output), mode=0700)
        return '\n'.join(output) % kw
    
    def _create_local_serial(self, dependencies):
        kw = {}
        kw['cores'     ] = self.cores
        kw['workdir'   ] = self.tempdir
        kw['code_list' ] = os.path.join(self.tempdir, 'code_list')
        kw['script'    ] = self._get_script_path('calculate')
        
        output = []
        output.append('#!/bin/bash')
        output.append('#')
        output.append(self.gnu_parallel_local_template)
        
        return '\n'.join(output) % kw

    def run(self, dependencies=[], mode='local'):
        ## Lets estimate total cputime for processing
        numfiles = len(self.to_process)
        if numfiles == 0:
            self.logger.info('No pdbs to process. Finishing')
            self.pbs_job_id = None
            return
        self.logger.info('%i pdbs to process.', numfiles)

        est = self.estimatedcputime        
        if est == None:
            est = config.pbs_default_wall_time # generous 5min per pdb file
        self.est_total = int(est*numfiles)
        self.cores     = int(math.ceil(self.est_total / 900.)) # Try to fit it in 15 min run time
        self.cores     = min(self.cores, numfiles) # No more cores than files
        if self.cores > config.default['max_cores']: self.cores = config.default['max_cores'] # But no more than  cores
        self.logger.debug('Estimated walltime = %i sec', self.est_total)
        self.logger.debug('Requested cores = %i (max %i)', self.cores, config.default['max_cores'])
        self.logger.debug('Single job timeout = %s s', self.timeout)
        # Submit job
        ##
        if mode == 'local':
            script = self.create_local_script(dependencies)
            self._save_temp_file('run', script, mode=0700)
            self._execute('run', self.tempdir)
        elif mode == 'pbs':
            script  = self.create_pbs_script(dependencies) #, self.est_total, numfiles)
            self.submit_qsub(script)
        ###

def extract(modules, dirname, fnames):
    logger.debug('Walking into %s', dirname)
    if dirname == config.repo_dir:
        fnames.remove('common')
    elif os.path.basename(dirname) == 'config':
        # Check if legitimate config and create module
        logger.debug('Checking %s', dirname)
        try:
            m = PdbRepoModule(dirname)
        except (TypeError, ScriptError) as err:
            logger.exception(err)
        else:
            if m.name in [_.name for _ in modules]:
                logger.error("Duplicate module name '%s'", m.name)
                logger.info("Conflicting module:\n\t%s", m.path)
            else:
                logger.debug("Adding module '%s' from %s", m.name, m.path)
                modules.append(m)
    else:
        if dirname in config.dir_blacklist:
            fnames[:] = []
        if 'config' in fnames:
            fnames[:] = ['config']

def sort_execution_order(modules):
    sorted = []
    names = {}
    waiting = []
    for module in modules:
        if len(module.dependencies) == 0:
            sorted.append((module, []))
            names[module.name] = module
        else:
            waiting.append(module)
            logger.debug("Postponing execution of '%s' due to dependencies: %s",
                          module.name, module.dependencies)

    changes = True
    while len(waiting) > 0 and changes:
        changes = False
        for module in waiting:
            logger.debug("Checking dependencies for '%s'", module.name)
            is_met = True
            for name in module.dependencies:
                if name not in names:
                    logger.debug("Unmet dependency: '%s'. Postponing.", name)
                    is_met = False
            if is_met:
                logger.debug('All dependencies met')
                sorted.append((module, 
                               list([names[d] for d in module.dependencies])))
                names[module.name] = module
                waiting.remove(module)
                changes = True
    for module in waiting:
        logger.error('Unable to resolve dependencies for %s' % module.name)
    return sorted
