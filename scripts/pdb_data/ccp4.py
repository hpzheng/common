import subprocess, logging, re
import collections
from threading import Thread
l = logging.getLogger('ccp4')

from os import environ

def source(script, update=1):
    p = subprocess.Popen(". %s; env" % script, stdout=subprocess.PIPE, shell=True)
    data = p.communicate()[0]

    env = dict((line.split("=", 1) for line in data.splitlines()))
    if update:
        environ.update(env)

    return env

def setup():
    source('/usr/local/xtalprogs/setup.sh')
    if 'PDB_TIMEOUT' in environ:
        e = { 
            'WARNTIME' : environ['PDB_TIMEOUT'],
            'KILLTIME' : '10'
        }
        environ.update(e)

class CCP4Program(Thread):
    prg_name = ''
    io       = []
    keywords = []

    _suppres_errors = False
    
    def __init__(self, cmdline={}, input={}, dry_run=False):
        # Generate command line
        # TODO: Count spawned programs
        ios = []
        for n in self.io:
            io = cmdline.get(n, None)
            if io:
                ios.extend([n, io])
        self.cmdline_kws = cmdline
        self.input_kws   = input
        self.cmd = [ 'timelimit', self.prg_name ] 
        self.cmd.extend(ios)
        self.logger  = logging.getLogger('ccp4.%s' % (self.prg_name))

        # Generate input file
        inpt = []
        for line in self._kw_parse(self.keywords, input):
            inpt.append(' '.join(line))
        inpt.append('end')
        self.input = inpt
        
        super(CCP4Program, self).__init__()
        self.name = self.prg_name 
        
        self.logger.debug("Cmdline: %s", self.cmd)
        self.logger.debug("Input  : %s", self.input)
        self.dry_run = dry_run
   
    def _kw_parse(self, kw, input):
        input = dict((key.lower(), value) for key, value in input.iteritems())
        
        self.logger.debug("Keyword parsing : %s", kw)
        self.logger.debug("Keyword parsing input: %s", input)

        for k in kw:
            self.logger.log(1, "  Checking kw: %s", k)
            if type(k) == str:
                self.logger.log(1, "    Regular")
                if k.lower() in input:
                    self.logger.log(1, "      Set to: %s", str(input[k.lower()]))
                    yield (k, str(input[k.lower()]))
            else:
                # So we have either keyword with default or with subkeywords
                k, d = k
                if type(d) == tuple or type(d) == list:
                    self.logger.log(1, "    Subkeywords")
                    # we have subkeywords then
                    out = [k]
                    for item in self._kw_parse(d, input.get(k.lower(), {})):
                        out.extend(item)
                    yield out
                else:
                    # default
                    self.logger.log(1, "    With defaults")
                    if k.lower() in input:
                        self.logger.log(1, "      Set to: %s", str(input[k]))
                        yield (k, str(input[k]))
                    else:
                        self.logger.log(1, "      Default: %s", str(d))
                        yield (k, str(d))

    def run(self):
        if not self.dry_run:
            self.p1 = subprocess.Popen(self.cmd, 
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, 
                             stderr=subprocess.PIPE)
            input = '\n'.join(self.input) + '\n'
            
            self.logger.debug('Started')
            self.stdout, self.stderr = self.p1.communicate(input)
            self.output = self.stdout
            status = self.p1.returncode
            self.logger.debug('Finished with status %s', status)
            if status != 0: 
                if not self._suppres_errors:
                    self.logger.critical("ERROR OCCURED:")
                    self.logger.critical(self.stderr)
                    self.logger.critical("OUTPUT:")
                    self.logger.critical(self.stdout)
                return False
            self.parse()
        return True

    def parse(self):
        self.data = self.output

    def timeout(self, timeout, add_timeout=10):
        # This is intended to be called outside thread
        self.join(timeout)
        if self.is_alive():
            self.p1.terminate()
            self.join(add_timeout)
            if self.is_alive():
                self.p1.kill()
                self.join()

class CCP4_FFT(CCP4Program):
    prg_name = 'fft'
    io       = ['HKLIN',
                'MAPOUT',
                'ABCOEFFS'
                ]
    # Not finished....
    keywords = ['LABIN']
    
class CCP4_MAPMASK(CCP4Program):
    prg_name = 'mapmask'
    io       = [
                'MAPIN',
                'MAPOUT',
                'XYZIN'
                ]
        
    # Not finished....
    keywords = [
                'BORDER',
                ]
 
class CCP4_SFALL(CCP4Program):
    prg_name = 'sfall'
    io       = [ 
                'XYZIN',
                'MAPOUT',
                ]
    keywords = [
                'MODE',
                'GRID',
                'SYMMETRY'
                ]

class CCP4_MAPDUMP(CCP4Program):
    prg_name = 'mapdump'
    io       = [ 
                'MAPIN',
                ]
    
    def parse(self):
        self.data = {}
        self.logger.debug("Parsing output")
        self.data['grid'] = re.findall("Grid sampling on x, y, z.*?(\d+)\s*?(\d+)\s*?(\d+)", self.output)[0]
        self.logger.debug("Grid: %s", self.data['grid'])
        self.data['cell'] = re.findall("Cell dimensions.*?(\d+\.\d+)\s+?(\d+\.\d+)\s+?(\d+\.\d+)\s+?(\d+\.\d+)\s+?(\d+\.\d+)\s+?(\d+\.\d+)", self.output)[0]
        self.logger.debug("Cell: %s", self.data['cell'])
        self.data['axes'] = re.findall("Fast, medium, slow axes.*?([XYZxyz])\s+?([XYZxyz])\s+?([XYZxyz])", self.output)[0]
        self.logger.debug("Axes: %s", self.data['axes'])
        self.data['spacegroup'] = re.findall("Space-group.*?(\d+)", self.output)[0]
        self.logger.debug("Spacegroup: %s", self.data['spacegroup'])
        # TODO: mumbojumbo with xyzlim" 
        #self.data['xyzlim'] = re.findall("Start and stop points on columns, rows, sections   (.*)", self.output)[0]
        # TODO: convert to numeric valuesi


class CCP4_OVERLAPMAP(CCP4Program):
    prg_name = 'overlapmap'
    io       = [
                'MAPIN1',
                'MAPIN2',
                'MAPIN3',
                'MAPOUT'
                ]
    keywords = [
                'CORRELATE'
                ]

    def parse(self):
        self.data = {}
        section = re.findall('\$GRAPHS.*\$\$(.*)\$\$', self.output, re.DOTALL)[0]
        d = re.findall('\s+(\d+)\s+([-\d]+\.\d+)\s+(\d+)', section)
      
        for i in d:
            self.data[int(i[0])] = float(i[1])

class CCP4_DISTANG(CCP4Program):
    prg_name = 'distang'
    io       = [
                'XYZIN'
               ]
    keywords = [
                'SYMMETRY',
                'DIST',
                'FROM',
                'TO'
               ]

    def parse(self):
        section = re.findall("Atom I  Atom J   Dij                Sym TX TY TZ occ\(i\)\*occ\(j\)  av_bi\+bj(.*?)Symmetry matrix    1", self.output, re.DOTALL)[0]
        d = re.findall('\s+(.)\s+([-\d]+)\s+([^\s]*)\s+(.)\s+(.)\s+([-\d]+)\s+([^\s]*)\s+(.)\s+(\d+\.\d+)\s+(.*?)\s+(\d+\.\d+)\s+(\d+\.\d+)', self.output)
        # Right now only distance is interesting

        self.data = [ (int(1), float(i[8])) for i in d ]

class CCP4_ACT(CCP4Program):
    prg_name = 'act'
    io       = [
                'XYZIN'
                ]
    keywords = [
                'SYMM',
                'CONTACT',
                'SHORT',
                ]

    def parse(self):
        section = re.findall(" ANALYSIS OF INTRA- AND INTERMOLECULAR CONTACTS(.*)ACT:  NORMAL TERMINATION", self.output, re.DOTALL)[0]
        d = re.findall("(\d+)\s+(.)\s+(.*?)\s+(.*?)\s+\.\.\.\.\s+(\d+)\s+(.)\s+(.*?)\s+(.*?)(\d+\.\d+)$", section, re.MULTILINE)
        self.data = [ (( i[1], int(i[0]) ), float(i[-1])) for i in d]
        #self.data = dict(self.data)

class CCP4_REFMAC(CCP4Program):
    prg_name = 'refmac5'
    io       = [
            'XYZIN',
            'HKLIN',
            'XYZOUT',
            'HKLOUT',
            'LIBIN',
            'LIBOUT',
            'TLSIN',
            'TLSOUT'
            ]
    keywords = [
            ('MAKE CHECK', 'none'),
            ('MAKE', [
                ('hydrogen', 'YES'),
                ('hout'    , 'NO' ),
                ('peptide' , 'NO' ),
                ('cispeptide','YES'),
                ('ssbridge'  ,'YES'),
                ('symmetry'  ,'YES'),
                ('sugar'     ,'YES'),
                ('connectivity', 'NO'),
                ('link', 'NO'),
                ]),
            ('REFI', [
                ('type', 'REST'),
                ('resi', 'MLKF'),
                ('meth', 'CGMAT'),
                ('bref', 'ISOT'),

                ]),
            ('NCYC', 0),
            ('SCAL', [
                ('type' , 'SIMP'),
                ('LSSC', ''),
                ('ANISO', ''),
                ('EXPE', ''),
                ]),
            ('SOLVENT', 'YES'),
            ('WEIGHT', 'AUTO'),
            ('MONITOR', 'NONE'),
            'LABIN',
            'LABOUT',
            ('NOHARVEST', ''),
            ]

    def parse(self):
        table = re.findall('\$TABLE: Rfactor analysis, stats vs cycle[^$]*\$GRAPHS[^$]*.*?([^$]+)[$\n]*([^$]+)',
                           self.output, re.DOTALL)[0]
        header = table[0].strip().split()
        data   = [l.split() for l in table[1].strip().split('\n')]
        self.data = []
        for line in data:
            self.data.append(dict(zip(header, line)))

class CCP4_REFMAC_RESTRAINS(CCP4_REFMAC):
    io = ['XYZIN', 'XYZOUT', 'LIBIN', 'LIBOUT']
    keywords = [
        ('MAKE CHECK', 'ALL'),
        ('MAKE', [
                ('hydrogen', 'YES'),
                ('hout'    , 'NO' ),
                ('peptide' , 'YES' ),
                ('cispeptide','YES'),
                ('ssbridge'  ,'YES'),
                ('symmetry'  ,'YES'),
                ('sugar'     ,'YES'),
                ('connectivity', 'YES'),
                ('link', 'YES'),
                ('format', 'f'),
                ('exit', 'Y')
                ]),
        ]
    _suppres_errors = True

    def parse(self):
        pass

class CCP4_CIF2MTZ(CCP4Program):
    prg_name = 'cif2mtz'
    io       = [ 'HKLIN', 'HKLOUT' ]
    keywords = [ 
                 'SYMMETRY',
                 'CELL'
                ]

    def parse(self):
        # Check if we have structure factors or intensities
        self.has_fp    = False
        self.has_sigfp = False
        self.has_i     = False
        self.has_sigi  = False
        #And free reflections
        self.has_free  = False
        columns = dict(re.findall('Found column.*with label *([^ ]*) *and type (.)', self.output))
        if 'FP' in columns and columns['FP'] == 'F':
            self.has_fp = True
        if 'SIGFP' in columns and columns['SIGFP'] == 'Q':
            self.has_sigfp = True
        if 'I' in columns and columns['I'] == 'J':
            self.has_i = True
        if 'SIGI' in columns and columns['SIGI'] == 'Q':
            self.has_sigi = True
        if 'FREE' in columns and columns['FREE'] == 'I':
            self.has_free = True

class CProgram(CCP4Program):
    arguments = []

    def __init__(self, cmdline={}):
        self.io = ['-%s' % a for a in arguments]
        c = dict(('-%s' % k, v) for k,v in cmdline.items())
        super(CProgram, self).__init__(cmdline=c)

class C_TRUNCATE(CCP4Program):
    prg_name = 'ctruncate'
    arguments = [ 'mtzin', 'mtzout', 'colin', 'colano' ]

class CCP4_TLSEXTRACT(CCP4Program):
    prg_name = 'tlsextract'
    io = ['XYZIN', 'TLSOUT']

    def parse(self):
        # Read file
        data = open(self.cmdline_kws['TLSOUT']).read()
        if re.search('^RANGE', data, re.MULTILINE):
            self.has_tls = True
        else:
            self.has_tls = False

if __name__ == '__main__':
    logging.basicConfig(level=0,)
    CCP4_REFMAC({'XYZIN':'A'}, {'REFI' : {'bref':'MIXE'}}, dry_run=True)
