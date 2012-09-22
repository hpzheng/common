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

class _dummy_pbs_job(object):
    def __init__(self, job_id, strict=False):
        self.qsub_job_id = job_id
        self.strict = strict

class PdbRepoModule:
    __modules__ = {}
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
                                  ('debug', flag_file, 0), #??? no used currently
                                  ('test', flag_file, 0),
                                  ('disabled', flag_file, 0),
                                  ('strict', flag_file, 0) # Whether it is a strict dependecy
                                  ]
    
    _script_filenames_required = [
                                  'get_pdb_codes',
                                  'calculate',
                                 ]

    _job_type_to_script = {
            ('pbs',  'serial')  : '_create_pbs_serial',
            ('local','serial')  : '_create_local_serial',
            ('pbs',  'local')   : '_create_pbs_local'
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
        if self.disabled:
            raise TypeError, 'Module is marked as disabled'        
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
       	self.logger.debug("Initializing")

        self.__modules__[self.name] = self
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
                'PDB_REPOSITORY_UNZIPPED' : config.pdb_repository_unzipped_dir,
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
                                                     self.exceptions, set(['']))
        # If test then use first file
        if self.test:
            self.to_process = tuple(self.to_process)[:1]
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

    def _get_script_path(self, script_name, path=None):
        if path == None:
            fn = os.path.join(self.path, 'scripts', script_name)
        else:
            fn = os.path.join(path, script_name)
        return fn

    def _execute(self, script_name, path=None, args=[], input=None):
        fn = self._get_script_path(script_name, path)

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
            a = [fn]
            a.extend(args)
            p = subprocess.Popen(a, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.environment)
            out, err_out = p.communicate(input)
            logger_err.info(err_out)
            logger_out.info(out)
            if self.test or self.debug:
                    self.logger.debug("Script stdout:\n%s", out)
                    self.logger.debug("Script stderr:\n%s", err_out)
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
        
        return out
    #def submit_qsub(self, script):
    #    print script
    def create_local_script(self, scrpt, dependencies, **kwargs):
        # Local jobs are executed one by one, no need to check pids
        deps = []
        job_type = kwargs.pop('job_type', self.job_type)
        return getattr(self, self._job_type_to_script[('local', job_type)])(scrpt, deps, **kwargs)
    
    def create_pbs_script(self, scrpt, dependencies, **kwargs):
        deps = []
        for item in dependencies:
            if not item.qsub_job_id == None:
                deps.append(item)
        job_type = kwargs.pop('job_type', self.job_type)
        return getattr(self, self._job_type_to_script[('pbs', job_type)])(scrpt, deps, **kwargs)

    gnu_parallel_template = """
cd %(workdir)s
# Create propper sshloginfile from node file
NODEFILE=${PBS_JOBID}.nodefile
sort $PBS_NODEFILE | uniq -c | awk '{print $1"/"$2}' > ${NODEFILE}
cat %(code_list)s | parallel -L1 --nice 19 --sshloginfile ${NODEFILE} --wd %(workdir)s %(script)s {}
"""
    gnu_parallel_local_template = """
cd %(workdir)s
cat %(code_list)s | parallel -L1 --nice 19 -j%(cores)i --wd %(workdir)s %(script)s {}
    """
    
    def _create_pbs(self, scrpt, dependencies, **kwargs):
        kw = {}
        job_ids_strict = [d.qsub_job_id for d in dependencies if d.strict]
        job_ids_other  = [d.qsub_job_id for d in dependencies if not d.strict]
        job_ids = []
        if job_ids_strict:
            job_ids.append('afterok:%s' % (':'.join(job_ids_strict)))
        if job_ids_other:
            job_ids.append('afterany:%s' % (':'.join(job_ids_other)))
        kw['job_ids'] = ','.join(job_ids)

        kw['queue_name'   ] = config.default['queue_name']
        kw['cores'        ] = self.cores
        kw['workdir'      ] = self.tempdir
        kw['logdir'       ] = self.logdir
        kw['code_list'    ] = os.path.join(self.tempdir, 'code_list')
        kw['script'       ] = os.path.join(self.tempdir, 'script_wrapper.sh') + ' ' + self._get_script_path(scrpt)
        kw['env_variables'] = ','.join(['%s=%s' % item for item in self._env.iteritems()])
        kw['core_properties'] = ''  
              
        kw.update(kwargs)
        
        output = []
        output.append('#!/bin/bash')
        output.append('#')
        output.append('#PBS -q %(queue_name)s')
        if dependencies:
            output.append('#PBS -W depend=%(job_ids)s')
        output.append('#PBS -l nodes=%(cores)i'+kw['core_properties'])
        output.append('#PBS -o %(logdir)s/pbs.'+scrpt+'.out')
        output.append('#PBS -e %(logdir)s/pbs.'+scrpt+'.err')
        output.append('#PBS -v %(env_variables)s')
        return output, kw

    def _create_pbs_local(self, scrpt, dependencies, **kwargs):
        output, kw = self._create_pbs(scrpt, dependencies, **kwargs)
        output.append('%(script)s %(code_list)s')
        out = '\n'.join(output) % kw
        self._save_temp_file('pbs.%s.in' % scrpt, out)
        return out
 
    def _create_pbs_serial(self, scrpt, dependencies, **kwargs):
        output, kw = self._create_pbs(scrpt, dependencies, **kwargs)
        output.append(self.gnu_parallel_template)
	# Create wrapper script to export env variables (parallel is not dealing with that)
        wrapper_output = []
        wrapper_output.append('#!/bin/bash')
        wrapper_output.extend(["export %s=%s" % item for item in self._env.iteritems()])
        wrapper_output.append('$@')
        self._save_temp_file('script_wrapper.sh', '\n'.join(wrapper_output), mode=0700)
        out = '\n'.join(output) % kw
        self._save_temp_file('pbs.%s.in' % scrpt, out)
        return out
    
    def _create_local_serial(self, scrpt, dependencies, **kwargs):
        kw = {}
        kw['cores'     ] = self.cores
        kw['workdir'   ] = self.tempdir
        kw['code_list' ] = os.path.join(self.tempdir, 'code_list')
        kw['script'    ] = self._get_script_path(scrpt)
        
        kw.update(kwargs)
        
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
            self.qsub_job_id = None
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
            
            script = self.create_local_script('calculate', dependencies)
            self._save_temp_file('run', script, mode=0700)
            if self.script_exists('prerun'): self._execute('prerun')
            try:
                self._execute('run', self.tempdir)
            except ScriptError as e:
                self.logger.error(e.args[0])
            if self.script_exists('postrun'): self._execute('postrun')
        elif mode == 'pbs':
            if self.script_exists('prerun'):
                prerun_script = self.create_pbs_script('prerun', dependencies, cores=1, core_properties=':local', job_type='local')            
                qsub_id = self.submit_qsub(prerun_script)
                dependencies = [_dummy_pbs_job(qsub_id, strict=True)]
            script  = self.create_pbs_script('calculate', dependencies) #, self.est_total, numfiles)
            qsub_id = self.submit_qsub(script)
            self.qsub_job_id = qsub_id
            dependencies = [_dummy_pbs_job(qsub_id, strict=False)]
            if self.script_exists('postrun'):
                postrun_script = self.create_pbs_script('postrun', dependencies, cores=1, core_properties=':local', job_type='local')            
                qsub_id = self.submit_qsub(postrun_script)
                self.qsub_job_id = qsub_id
            
        ###
    def script_exists(self, name, path=None):
        fpath = self._get_script_path(name, path)
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
            
        
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

def sort_execution_order(modules, check_deps=True):
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
        logger.warning('Unable to resolve dependencies for %s' % module.name)
        if not check_deps:
            logger.warning('Checking dependencies overriden. Adding module %s for execution', module.name)
            sorted.append((module, list([names[d] for d in module.dependencies if d in names])))
        else:
            logger.error('Skipping module %s due to unmet dependencies', module.name)
    return sorted

def get_all_dependencies(modules):
    r = []
    for m in modules:
        deps = list(PdbRepoModule.__modules__[name] for name in m.dependencies)
        r.extend(deps)
        r.extend(get_all_dependencies(deps))
    return r      
