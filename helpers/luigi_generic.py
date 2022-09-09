import os
import luigi
import luigi.contrib.postgres
import shlex
from luigi.contrib.external_program import ExternalProgramTask
from pathlib import Path


#TODO needs some error handling, for example what if we pass a filepath instead of a dir?
class b4t_directory_target(luigi.LocalTarget):
    def __init__(self, path=None, format=None, is_tmp=False):
        super().__init__(path, format, is_tmp)
        #isdir = os.path.isdir(path)
        #if not isdir:
        #    os.mkdir(path)

class b4t_execute_shell_command(ExternalProgramTask):
    date_ran = luigi.Parameter()
    command = luigi.Parameter(default="bash collect_data.sh srn devops rzBRWfBMh6NaccS 'o8bO#$nN0xbOssbER15/a6!ZXDWdkSAV7@PKCl^mb&rUpGReaxHLgiyie23$'")
    def program_args(self):
        commandArgs = shlex.split(self.command)
        return commandArgs
    
    def output(self):
        return luigi.LocalTarget("./sh/finalData/srn")


def check_dir(path):
    isdir = os.path.isdir(path)
    if not isdir:
        os.mkdir(path)