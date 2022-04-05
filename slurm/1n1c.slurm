#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=1n1c.out

module load foss/2019b
module load python/3.7.4
module load mpi4py

time mpirun python3 new.py

my-job-stats -a -n -s
