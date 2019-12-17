import os
import re
import subprocess
import csv
from psutil import Popen

from config import conf
from lib.helpers import display_cmd, display_cmd_output

NON_PRINTABLE_PAT = re.compile(r'[^\x00-\x7f]')
ANSI_ESCAPE_PAT = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
NO_DOUBLE_COLON = re.compile(r'::')
# =====================================================


class Oshelper:

    def __init__(self, logger):
        self.logger = logger
    # ___________________________________

    def cmdshell(self, cmd, timeout=None, inpt=None, log_cmd=True):

        if timeout is None:
            timeout = conf['SHELL_COMMAND_TIMEOUT']

        output = []
        errors = []

        execline = ' '.join(cmd)

        if log_cmd:
            display_cmd(self.logger, execline)

        p = Popen(
            execline,
            env=os.environ,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )

        try:
            if inpt:
                p.stdin = subprocess.PIPE
                out, err = p.communicate(input=bytes(inpt, 'utf-8'), timeout=timeout)
            else:
                out, err = p.communicate(timeout=timeout)

        except subprocess.TimeoutExpired:
            p.kill()
            out, err = p.communicate()

        if out:
            output = [l.strip() for l in out.decode('utf-8').split('\n') if len(l)]

        if log_cmd:
            display_cmd_output(self.logger, output)

        if err:
            errors = [l.strip() for l in err.decode().split('\n')]
            self.logger.debug("Errors: %r", errors)

        self.logger.debug("CMD return code: %r", p.returncode)

        return p.returncode, output, errors
    # __________________________________________

    def create_ssh_directory(self, node_hostname):
        """
        Not in use currently.
        """
        pxe_passwd = self.get_pxe_password()
        cmd = 'mkdir ~/.ssh'
        self.runssh_pswd_auth(
            dsthost=node_hostname,
            user=conf['NODE_USER'],
            pswd=pxe_passwd,
            cmd=cmd
        )
    # ________________________________________

    def download_file(self, url, dst):
        cmd = [
            'wget',
            url,
            "--no-proxy",
            "--no-cache",
            "--no-verbose",
            "--no-check-certificate",
            "-O %s" % dst,
        ]

        return self.cmdshell(cmd, log_cmd=True)
    # ________________________________________

    def fetch_ivtuser_infinilab_token(self, username):

        host = 'ivt-ubuntu.telad.il.infinidat.com'

        cmd = [
            'cat',
            '/home/%s/.infinidat/infinilab.tkn' % username
        ]
        rc, output, errors = self.sshcmd_shell(host, cmd)
        try:
            return str(output[0]).strip()
        except IndexError:
            # no token for the user
            return None
    # ______________________________________

    def get_idrac_password(self):
        pfile = os.path.expanduser('~/.infinidat/.ivtidrac')
        with open(pfile) as pf:
            pswd = pf.read().strip()

        return pswd
    # ____________________________

    def get_pxe_password(self):
        pfile = os.path.expanduser('~/.infinidat/.ivtpxe')
        with open(pfile) as pf:
            pswd = pf.read().strip()
        return pswd
    # ____________________________

    def copy_file_with_pswd(self, dsthost, user, pswd, srcpath, dstpath=None, port=22, override=False):
        """
        Copies a file to the dstserver.
        First checks if not already exists.
        Skips copying if it does.
        Overrides if *override* parameter is set to True. Default: False
        """

        if dstpath is None:
            dstpath = srcpath

        try:
            client = self._create_paramiko_ssh_client(dsthost, user, pswd)
            sftp = client.open_sftp()
            if override:
                sftp.put(srcpath, dstpath)
            else:
                try:
                    sftp.stat(dstpath)
                except IOError:
                    sftp.put(srcpath, dstpath)
        finally:
            if client:
                client.close()

    # ____________________________

    def put_public_ssh_key_to_node_in_dr(self, node_hostname):
        """
        DEPRECATED: Not in use currently.
        """
        self.create_ssh_directory(node_hostname)

        pxe_passwd = self.get_pxe_password()
        public_key_file = os.path.expanduser("~/.ssh/id_rsa.pub")
        public_key_string = open(public_key_file).read().strip()
        cmd = 'echo %s >> ~/.ssh/authorized_keys' % public_key_string
        self.runssh_pswd_auth(
            dsthost=node_hostname,
            user=conf['NODE_USER'],
            pswd=pxe_passwd,
            cmd=cmd
        )

    # ________________________________________

    def is_pingable(self, dst):

        cmd = ['ping', '-q', '-c', '1', '-W', '10', dst]

        rc, ouput, errors = self.cmdshell(cmd, timeout=5)
        self.logger.debug("Is pingable RC: %r", rc)
        return not rc
    # __________________________________________

    def is_accessible_on_ssh(self, nodename):

        cmd = ['date']
        rc, ouput, errors = self.sshcmd(nodename, cmd, log_cmd=False)
        return not rc
    # __________________________________________

    def is_accessible_on_ssh_dr(self, nodename):

        cmd = 'date'

        output = self.run_remote_command_with_pswd(nodename, cmd, connection_timeout=30.0, log_cmd=False)
        if len(output):
            return True

        return False
    # __________________________________________

    def sshcmd(self,
            dst,
            cmd,
            inpt=None,
            timeout=None,
            log_cmd=True,
            term=False,
            display_errors=False,
            parse_output=True):  # noqa

        if timeout is None:
            timeout = conf['SSH_CONNECT_TIMEOUT']

        output = []
        errors = []

        ex = [
            'ssh',
            '-q',
            '-o UserKnownHostsFile=/dev/null',
            '-o StrictHostKeyChecking=no',
            '-o ConnectTimeout=%s' % timeout,
            dst
        ]

        if term:
            ex.insert(1, '-t')

        ex.extend(cmd)

        self.logger.debug("Execution list: %r", ex)
        if log_cmd:
            display_cmd(self.logger, ' '.join(ex))

        sp = Popen(
            ex,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        if inpt:
            sp.stdin = subprocess.PIPE
            out, err = sp.communicate(input=bytes(inpt, 'utf-8'))
        else:
            out, err = sp.communicate()

        if parse_output and out:
            output = [
                self.replace_double_colon(
                    self.remove_ansi_esc_chars(
                        self.remove_non_printable_chars(l.strip())
                    )
                ) for l in out.decode('utf-8').split('\n') if len(l)]

        if log_cmd and len(output):
            display_cmd_output(self.logger, output)

        if err:
            errors = [l.strip() for l in err.decode('utf-8').split('\n') if l.strip()]
            if display_errors:
                display_cmd_output(self.logger, errors)
            else:
                self.logger.debug("Errors: %r", errors)

        self.logger.debug("SSH CMD return code: %r", sp.returncode)

        return sp.returncode, output, errors
    # ___________________________________

    def sshcmd_shell(self, dst, cmd):

        output = []
        errors = []

        ex = [
            'ssh',
            '-l root',
            '-q',
            '-o UserKnownHostsFile=/dev/null',
            '-o StrictHostKeyChecking=no',
            '-o ConnectTimeout=10',
            dst
        ]

        ex += cmd

        self.logger.debug("Execution list: %r", ex)
        execline = ' '.join(ex)
        self.logger.debug("Exec line: %r", execline)

        sp = Popen(
            execline,
            env=os.environ,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        out, err = sp.communicate()
        output = [l.strip() for l in out.decode('utf-8').split('\n') if len(l)]
        self.logger.debug("SSH CMD return code: %r", sp.returncode)

        return sp.returncode, output, errors
    # ___________________________________

    def scpcmd(self, src, dst, src_remote=False, log_cmd=True, timeout=None):

        if timeout is None:
            timeout = conf['SSH_CONNECT_TIMEOUT']

        output = []
        errors = []

        ex = [
            'scp',
            '-q',
            '-r',
            '-o UserKnownHostsFile=/dev/null',
            '-o StrictHostKeyChecking=no',
            '-o ConnectTimeout=%s' % timeout,
            src,
            dst
        ]

        if src_remote:
            ex.insert(1, '-3')

        if log_cmd:
            display_cmd(self.logger, ' '.join(ex))

        sp = Popen(
            ex,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE
        )

        out, err = sp.communicate()

        if out:
            output = [l.strip() for l in out.decode('utf-8').split('\n') if len(l)]

        if log_cmd:
            display_cmd_output(self.logger, output)

        if err:
            errors = [l.strip() for l in err.decode('utf-8').split('\n')]
            self.logger.info("Errors: %r", errors)

        self.logger.debug("SCP CMD return code: %r", sp.returncode)

        return sp.returncode, output, errors
    # ___________________________________

    def remove_ansi_esc_chars(self, s):
        return ANSI_ESCAPE_PAT.sub('', s)
    # ___________________________________

    def replace_double_colon(self, s):
        return NO_DOUBLE_COLON.sub('_', s)
    # ___________________________________

    def remove_non_printable_chars(self, s):
        return NON_PRINTABLE_PAT.sub(' ', s)
    # ___________________________________

    def run_remote_command_with_pswd(self, node_hostname, cmd, log_cmd=True, connection_timeout=None):

        if log_cmd:
            display_cmd(self.logger, cmd)

        pxe_passwd = self.get_pxe_password()
        output = self.runssh_pswd_auth(
            dsthost=node_hostname,
            user=conf['NODE_USER'],
            pswd=pxe_passwd,
            cmd=cmd,
            connection_timeout=connection_timeout
        )

        if log_cmd:
            display_cmd_output(self.logger, output)

        return '\n'.join(output)
    # ________________________________________

    def run_qaucli_command(self, node_hostname, param_string):
        cmd = '/usr/local/bin/qaucli %s' % param_string
        return self.run_remote_command_with_pswd(node_hostname, cmd)
    # __________________________________________

    def scp_file_with_pswd(self, node_hostname, srcfile, dstfile):

        pxe_passwd = self.get_pxe_password()
        self.copy_file_with_pswd(
            dsthost=node_hostname,
            user=conf['NODE_USER'],
            pswd=pxe_passwd,
            srcpath=srcfile,
            dstpath=dstfile
        )
    # ________________________________________

    def set_remote_loghandler(self, logger, logkey):
        from srv.lib.logclient import Logclient
        client = Logclient()
        return client.reset_remote_logging(lg=logger, logkey=logkey)
    # ________________________________________

    def write_csv_file(self, data, csv_file):
        field_names = data.keys()
        rows = data.values()

        with open(csv_file, 'wb') as fh:
            csv_writer = csv.DictWriter(
                fh, field_names, delimiter=',', quoting=csv.QUOTE_NONE, quotechar="'", escapechar="\\")
            csv_writer.writeheader()
            csv_writer.writerows(rows)
