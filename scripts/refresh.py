# First of all get modules present in repository

import os
import config
import logging
import subprocess
import datetime
import math

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




def str_file(path):
    if path == None:
        return None
    with open(path) as f:
        return str(f.read()).strip()

def float_file(path):
    if path == None:
        return None
    with open(path) as f:
        return float(f.read().strip())

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
                                  ('authors', str_file),
                                  ('estimatedcputime', float_file),
                                  ('exceptions', list_file),
                                  ('processed', list_file),
                                  ('debug', flag_file),
                                  ]
    _job_type_to_script = {
            'serial': '_create_pbs_serial',
            }

    def __init__(self, config_dir):
        # Check if config dir is complete
        # Separating file checking and parsing for 
        # better performance (no parsing if module is incomplete)
        
        for name, file_type in self._config_filenames_required:
            path = os.path.join(config_dir, name)
            if not os.path.exists(path):
                raise TypeError, "Missing '%s' file in config dir '%s'" % \
                                                    (name, config_dir)
            else:
                setattr(self, '_fn_%s' % name, path)
        
        for name, file_parser in self._config_filenames_optional:
            path = os.path.join(config_dir, name)
            if not os.path.exists(path):
                setattr(self, '_fn_%s' % name, None)
            else:
                setattr(self, '_fn_%s' % name, path)
        
        for name, file_parser in self._config_filenames_required \
                                 + self._config_filenames_optional:
            path = getattr(self, '_fn_%s' % name)
            try:
                setattr(self, name, file_parser(path))
            except Exception as err:
                raise TypeError, "Error parsing '%s' file in config dir '%s':"\
                                  + " %s" % (name, config_dir, err)
        self.path = os.path.dirname(config_dir)
        
        # Setup logging
        if self.debug: 
            log_dir_name = datetime.datetime.today()\
                                            .replace(microsecond=0)\
                                            .isoformat('_')
            log_dir_name += '_' + str(os.getpid())
            self.logdir = os.path.join(self.path, 'logs', log_dir_name)
            os.makedirs(self.logdir)
        else:
            self.logdir = None
        
        self.logger = logging.getLogger('module.%s' % self.name)
                
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

    def print_config(self):
        for name, file_type in self._config_filenames_required \
                                 + self._config_filenames_optional:
            print name
            print ' ' + '\n '.join(str(getattr(self, name)).split('\n'))
            print

    def _execute(self, script_name):
        fn = os.path.join(self.path, 'scripts', script_name)
        logger = logging.getLogger('module.%s.%s' % (self.name, script_name))
        logger.propagate = False
        logger_out = logging.getLogger('module.%s.%s.out' % (self.name, script_name))
        logger_err = logging.getLogger('module.%s.%s.err' % (self.name, script_name))

        if self.logdir:
            logfile_out = os.path.join(self.logdir, script_name+'.out')
            logfile_err = os.path.join(self.logdir, script_name+'.err')
            handler_out = logging.FileHandler(logfile_out)
            handler_err = logging.FileHandler(logfile_err)
        else:
            handler_out = NullHandler()
            handler_err = NullHandler()

        logger_out.addHandler(handler_out)
        logger_err.addHandler(handler_err)

        if os.path.exists(fn):
            self.logger.info('Running %s script', script_name)
            p = subprocess.Popen(fn, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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
        out, err_out = p.communicate()
        if p.returncode != 0:
            raise ScriptError, 'Job submission failed: %s' % err_out 
        
        self.qsub_job_id = qsub_job_id

    def create_pbs_script(self, dependencies):
        deps = []
        for item in dependencies:
            if not item.pbs_job_id == None:
                deps.append(item)

        return getattr(self, self._job_type_to_script[self.job_type])(deps)

    gnu_parallel_template = """
cd %(workdir)s
parallel -L1 -j%(cores)i --sshloginfile $PBS_NODEFILE -W%(workdir)s
"""
    
    def _create_pbs_serial(self, dependencies):
        kw = {}
        kw['queue_name'] = 'pdb'
        kw['job_ids'   ] = ':'.join([d.pbs_job_id for d in dependencies])
        kw['cores'     ] = self.cores
        kw['workdir'   ] = self.path
        output = []
        output.append('#!/bin/bash')
        output.append('#')
        output.append('#PBS -q %(queue_name)s')
        if dependencies:
            output.append('PBS -W depend=afterok:%(job_ids)s')
        output.append('#PBS -l cores=%(cores)i')
        output.append('#PBS -o %(workdir)s/log')
        output.append(self.gnu_parallel_template % kw)

        return '\n'.join(output) % kw

    def run(self, dependencies=[]):
        ## Lets estimate total cputime for processing
        numfiles = len(self.to_process)
        if numfiles == 0:
            self.logger.info('No pdbs to process. Finishing')
            self.pbs_job_id = None
            return
        self.logger.info('%i pdbs to process.', numfiles)

        est = self.estimatedcputime        
        if est == None:
            est = 300 # generous 5min per pdb file
        self.est_total = int(est*numfiles)
        self.cores     = int(math.ceil(self.est_total / 1800.)) # Try to fit it in 30min run time
        self.cores     = min(self.cores, numfiles) # No more cores than files
        if self.cores > 16: self.cores = 16 # But no more than 16 cores
        self.logger.debug('Estimated walltime = %i sec', self.est_total)
        
        # Submit job to PBS
        ##
        script  = self.create_pbs_script(dependencies) #, self.est_total, numfiles)
        self.submit_qsub(script)
        ###

def extract_modules(modules, dirname, fnames):
    if dirname == config.repo_dir:
        fnames.remove('common')
    elif os.path.basename(dirname) == 'config':
        # Check if legitimate config and create module
        logging.debug('Checking %s', dirname)
        try:
            m = PdbRepoModule(dirname)
        except (TypeError, ScriptError) as err:
            logging.error(err)
        else:
            if m.name in [_.name for _ in modules]:
                logger.error("Duplicate module name '%s'", m.name)
                logger.info("Conflicting module:\n\t%s", m.path)
            else:
                logging.debug("Adding module '%s' from %s", m.name, m.path)
                modules.append(m)
    else:
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

modules = []
os.path.walk(config.repo_dir, extract_modules, modules)
modules_to_run = sort_execution_order(modules)

for module, dependencies in modules_to_run:
    try:
        module.run(dependencies)
    except Exception as err:
        module.logger.error(err)
        raise
