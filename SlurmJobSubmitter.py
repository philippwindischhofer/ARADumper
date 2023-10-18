import subprocess as sp
import textwrap, uuid, os

class SlurmJobSubmitter:

    @staticmethod
    def submit(cmds, submitdir,
               job_name = None, partition = "avieregg", account_to_charge = "pi-avieregg",
               walltime_limit = "06:00:00", number_nodes = 1, cpus_per_node = 1, mem_per_cpu = 2000,
               mail_type = "NONE", mail_addr = None, stdout_path = None, stderr_path = None,
               dryrun = False):
        
        if job_name is None:
            job_name = str(uuid.uuid4())

        if mail_type != "NONE" and mail_addr is None:
            raise RuntimeError("Error: if you want mail, you need to provide your address!")

        if stdout_path is None:
            stdout_path = os.path.join(submitdir, f"{job_name}.out")

        if stderr_path is None:
            stderr_path = os.path.join(submitdir, f"{job_name}.err")
            
        if not os.path.exists(submitdir):
            os.makedirs(submitdir)
    
        submitpath = os.path.join(submitdir, f"{job_name}.sbatch")

        sceleton = """\
            #!/bin/bash
            #SBATCH --job-name={job_name}
            #SBATCH --output={out_path}
            #SBATCH --error={error_path}
            #SBATCH --account={account_to_charge}
            #SBATCH --time={walltime_limit}
            #SBATCH --partition={partition}
            #SBATCH --nodes={number_nodes}
            #SBATCH --ntasks-per-node={cpus_per_node}
            #SBATCH --mem-per-cpu={mem_per_cpu}
            #SBATCH --mail-type={mail_type}
            #SBATCH --mail-user={mail_addr}

            {cmds}
            """
        
        with open(submitpath, 'w') as submitfile:
            submitfile.write(textwrap.dedent(sceleton.format(
                job_name = job_name, out_path = stdout_path, error_path = stderr_path,
                account_to_charge = account_to_charge, walltime_limit = walltime_limit,
                partition = partition, number_nodes = number_nodes, cpus_per_node = cpus_per_node,
                mem_per_cpu = mem_per_cpu, mail_type = mail_type, mail_addr = mail_addr,
                cmds = "\n".join(cmds)
            )))

        if not dryrun:
            sp.check_output(["sbatch", submitpath])

if __name__ == "__main__":

    # run some simple demo jobs
    for cur in range(10):
        SlurmJobSubmitter.submit(cmds = ["sleep 10"], dryrun = False, submitdir = "./test_jobs/")
