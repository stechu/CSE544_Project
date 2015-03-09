import boto.ec2
import time
import paramiko
import select
import subprocess
import sys
import re
import socket
import errno

###############################################################################
# EC2 specific stuff

AMI_ID = 'ami-6bd3985b'
KEY_NAME = 'maaz_antoine'
SEC_GROUP = 'maaz_antoine'
PL_GROUP = 'maaz_antoine'
#PL_GROUP = None
KEY_PATH = '../code/maaz_antoine.pem'

conn = boto.ec2.connect_to_region('us-west-2',
        aws_access_key_id='AKIAIFGR4VQQKDP35S5Q',
        aws_secret_access_key='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')

# Start n new instances of the specified type
def start_instances(itype, n):
    res = conn.run_instances(AMI_ID, min_count=n, max_count=n,
            key_name=KEY_NAME, security_groups=[SEC_GROUP],
            instance_type=itype, placement_group=PL_GROUP)
    return res.instances

# Get list of our instances that are running
def get_instances(itype):
    ins = []
    ress = conn.get_all_reservations()
    for r in ress:
        for i in r.instances:
            if i.state == 'running' and i.key_name == KEY_NAME and \
                    i.instance_type == itype:
                ins.append(i)
    return ins

# Make sure we have exactly n instances of the specified type running, and
# return a list. If more are running the unneeded ones are shut down, if too few
# are running new instances will be started.
def get_n_instances(itype, n):
    ins = get_instances(itype)
    if len(ins) < n:
        new_ins = start_instances(itype, n - len(ins))
        ins = ins + new_ins
    elif len(ins) > n:
        to_terminate = ins[n:]
        ins = ins[:n]
        for i in to_terminate:
            i.terminate()
    return ins

# Terminate the specified instances
def terminate_all(ins):
    for i in ins:
        i.terminate()

# Wait until all of the specified instances are running
def wait_for_instances(ins):
    for i in ins:
        while i.state != 'running':
            time.sleep(5)
            i.update()


###############################################################################
# Handling remote execution of commands and deployment of files

def open_connections(ins):
    for i in ins:
        s = paramiko.SSHClient()
        s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        succeeded = False
        while not succeeded:
            try:
                succeeded = True
                s.connect(i.ip_address, username='ubuntu', key_filename=KEY_PATH,
                        timeout=120, banner_timeout=120)
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    succeeded = False
                else:
                    raise
        i.ssh_connection = s
        i.sftp_channel = s.open_sftp()

def close_connections(ins):
    for i in ins:
        i.sftp_channel.close()
        i.ssh_connection.close()
        i.ssh_connection = None
        
def run_cmd(i, cmd):
    (cin,cout,cerr) = i.ssh_connection.exec_command(cmd)
    cin.close()
    sout = ''
    serr = ''
    chans = [cout.channel,cerr.channel]
    while True:
        rl,wl,xl = select.select(chans[:], [],chans[:])
        any_ready = True
        while any_ready:
            any_ready = False
            for c in chans:
                if c.recv_stderr_ready():
                    serr += c.recv_stderr(1024)
                    any_ready = True
                elif c.recv_ready():
                    sout += c.recv(1024)
                    any_ready = True

        if c.exit_status_ready():
            cout.close()
            cerr.close()
            st = c.recv_exit_status()
            return (sout.split('\n'),serr.split('\n'),st)

def put_file(i, s, d):
    i.sftp_channel.put(s, d)

def compare_files(i, l, r):
    lh = subprocess.check_output(['md5sum', l]).split(' ')[0]
    o,e,st = run_cmd(i, 'md5sum ' + r)
    rh = o[0].split(' ')[0]
    return lh == rh

def remote_file(i, f, m):
    return i.sftp_channel.file(f, m)


###############################################################################
# Here begins the benchmark specific stuff




def run_benchmark(n, i_type, bench_args): 
    # Get the required number of instances
    instances = get_n_instances(i_type,n)
    wait_for_instances(instances)
    print 'Instance IPs:'
    for i in instances:
        print '   ', i.ip_address

    # Open ssh connections to all instances
    open_connections(instances)

    bench_time = -1.
    try:
        print 'Copy key to all machines'
        for i in instances:
            put_file(i, KEY_PATH, '.ssh/id_rsa')
            run_cmd(i, 'chmod 600 .ssh/id_rsa')

        # Make sure all hosts are in eachothers known_hosts
        print 'Populating known hosts'
        for i in instances:
            for j in instances:
                run_cmd(i, 'ssh-keyscan -H ' + j.private_dns_name +
                        ' >> ~/.ssh/known_hosts')


        # Deploy what we need on the master
        master = instances[0]
        # Copy code
        if not compare_files(master, '../code/main.cpp', 'graphlab/apps/550/main.cpp'):
            print 'Updating app'
            put_file(master, '../code/main.cpp', 'graphlab/apps/550/main.cpp')
            # Compile app
            (so,se,st) = run_cmd(master, 'make -C graphlab/release/apps/550')
            if st != 0:
                print 'Compilation failed:'
                for l in se:
                    print '   ', l
                raise

        print 'Creating /tmp/machines on master'
        f = remote_file(master, '/tmp/machines', 'w')
        for i in instances:
            f.write(i.private_dns_name + '\n')
        f.close()

        print 'Copying app to all instances'
        for i in instances:
            run_cmd(master, 'scp graphlab/release/apps/550/550_app ' +
                    i.private_dns_name + ':')

        print 'Running app...'
        so,se,st = run_cmd(master, 'time mpiexec -n ' + str(n) +
                ' --hostfile /tmp/machines /home/ubuntu/550_app ' + bench_args)
        if st == 0:
            print 'Completed successfully'

            # parse execution time
            for l in se:
                m = re.match(r"^real\s+([0-9]+)m([0-9\.]+)s", l)
                if m:
                    bench_time = float(m.group(1)) * 60 + float(m.group(2))
                    break
        else:
            print 'Execution failed'
        for l in so:
            print '   OUT: ' + l
        for l in se:
            print '   ERR: ' + l


    except:
        print sys.exc_info()
    finally:
        close_connections(instances)

    return bench_time

#it = 'c3.8xlarge'
instance_types = ['c3.8xlarge']
for it in instance_types:
    secs = run_benchmark(1, it, '500')
    print '######################',it,',',secs
    terminate_all(get_instances(it))

