from collections import defaultdict
import re, os
from pdb_data import config


class pdbObjectFactory(defaultdict):
    def __missing__(self, key):
        if len(key) == 4:
            return PdbRepoObject(key)
        skey = key.split('.')
        if skey[-1] == 'pdb':
            return PdbPdbFileObject(key)
        if skey[-1] == 'cif':
            return PdbCifFileObject(key)


class PdbCifFileObject(object):

    def __init__(self, fn):
        
        self.cif  = open(fn).read()

        self.symmetry = re.search("_symmetry\.space_group_name_H-M *'{0,1}([^']*)'{0,1} *", self.cif).group(1)
        self.cell     = (
            re.search('_cell\.length_a *(.*) *', self.cif).group(1),
            re.search('_cell\.length_b *(.*) *', self.cif).group(1),
            re.search('_cell\.length_c *(.*) *', self.cif).group(1),
            re.search('_cell\.angle_alpha *(.*) *', self.cif).group(1),
            re.search('_cell\.angle_beta *(.*) *', self.cif).group(1),
            re.search('_cell\.angle_gamma *(.*) *', self.cif).group(1),
            )
        self.resolution = None
        try:
            self.resoultion = float(re.search('_reflns.d_resolution_high *(.*) *', self.cif).group(1))
        except:
            pass

        self.r_factor = None
        patterns=('ls_R_factor_R_work', 'ls_R_factor_obs', 'ls_R_factor_all')
        for pat in patterns:
            m = re.search('_refine\.%s *([^ ]*)' % pat, self.cif)
            if m:
                try:
                    self.r_factor = float(m.group(1))
                except ValueError:
                    continue
                else:
                    break

        self.r_free   = None
        m = re.search('_refine\.ls_R_factor_R_free *([^ ]*)', self.cif)
        if m:
            try:
                self.r_free = float(m.group(1))
            except ValueError:
                pass
        self.aniso = False
        m = re.search('_atom_site_anisotrop', self.cif)
        if m:
            self.aniso = True

class PdbRepoObject(PdbCifFileObject):
    
    def __init__(self,code):
        self.code = code
        fn = os.path.join(config.cifs_path, '%s.cif' % code)
        super(PdbRepoObject, self).__init__(fn)

class PdbPdbFileObject(object):

    def __init__(self, fn):
        self.pdb = open(fn).read()

        self.r_factor = False
        self.r_free   = False

        m=re.search('  R VALUE.*WORKING SET.*: *([^ ]*)')
        if m:
            self.r_factor = float(m.group(1))
        m=re.search('  FREE R VALUE *: *([^ ]*)')
        if m:
            self.r_free = float(m.group(1))
        
data = pdbObjectFactory()
